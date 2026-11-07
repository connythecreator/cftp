//! Client-side pull: validate the server manifest, then fetch and write it.

use super::*;

/// Pulls `opts.source` from the server root into `opts.destination`.
///
/// Pull requires `cftp/4`. The server is authoritative over both the requested
/// path and which modes are enabled: a server with pull disabled rejects the
/// request with a message instead of sending data. The client validates the
/// manifest with the same path and size rules the server applies to pushes,
/// creates the tree, and only then signals that it is ready for data.
///
/// ```no_run
/// use std::net::SocketAddr;
/// use std::path::Path;
///
/// use cftp_core::prelude::*;
///
/// # async fn run(addr: SocketAddr, auth: ClientAuth) -> anyhow::Result<()> {
/// let opts = PullOptions {
///     source: "releases/".into(),
///     destination: Path::new("./downloads").to_path_buf(),
///     ..PullOptions::default()
/// };
/// let stats = pull(addr, &auth, opts).await?;
/// println!("received {} files, {} bytes", stats.files, stats.bytes);
/// # Ok(())
/// # }
/// ```
pub async fn pull(addr: SocketAddr, auth: &ClientAuth, opts: PullOptions) -> Result<PullStats> {
    validate_tuning(&opts.tuning)?;
    if opts.source.is_empty() {
        bail!("pull source is empty");
    }
    if opts.destination.as_os_str().is_empty() {
        bail!("pull destination is empty");
    }
    match fs::metadata(&opts.destination) {
        Ok(meta) if !meta.is_dir() => {
            bail!(
                "pull destination is not a directory: {}",
                opts.destination.display()
            )
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&opts.destination).with_context(|| {
                format!(
                    "failed to create pull destination {}",
                    opts.destination.display()
                )
            })?;
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "failed to inspect pull destination {}",
                    opts.destination.display()
                )
            });
        }
    }

    let (_endpoint, connection) =
        connect_client(addr, auth, &opts.tuning, false, &opts.server_name).await?;
    let wire: WireVersion = negotiated_version(&connection);
    if wire != WireVersion::V4 {
        bail!(
            "server does not support pull (cftp/4 required; negotiated cftp/{})",
            wire.number()
        );
    }
    notify(&opts.progress, format!("connected to {addr}"));

    let (mut control_send, mut control_recv): (SendStream, RecvStream) =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.open_bi()).await {
            Err(_) => bail!(
                "timed out after {}s opening the control stream",
                HANDSHAKE_TIMEOUT.as_secs()
            ),
            Ok(result) => result?,
        };
    notify(&opts.progress, format!("requesting {}", opts.source));
    tokio::time::timeout(
        HELLO_TIMEOUT,
        write_client_hello(
            &mut control_send,
            wire,
            &HelloInfo {
                version: wire.number(),
                mode: TransferMode::Pull,
                destination: opts.source.clone(),
                entries: Vec::new(),
                profile: opts.profile,
            },
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "timed out after {}s sending the pull request",
            HELLO_TIMEOUT.as_secs()
        )
    })??;
    notify(&opts.progress, "waiting for the server to accept the pull");

    let server_hello: ServerHelloInfo =
        match tokio::time::timeout(HELLO_TIMEOUT, read_server_hello(&mut control_recv, wire)).await
        {
            Err(_) => bail!(
                "timed out after {}s waiting for the server hello",
                HELLO_TIMEOUT.as_secs()
            ),
            Ok(result) => result?,
        };
    if !server_hello.accepted {
        bail!("server rejected pull: {}", server_hello.message);
    }
    if !server_hello.allow_pull {
        bail!("server does not allow pull");
    }
    let effective_profile: HardwareProfile = server_hello.effective_profile;
    let mut tuning: Tuning = if effective_profile == opts.profile {
        opts.tuning.clone()
    } else {
        Tuning::for_profile(effective_profile, None)
    };
    tuning.profile = effective_profile;
    apply_connection_tuning(&connection, &tuning);
    notify(
        &opts.progress,
        format!("pulling with the {effective_profile} profile"),
    );

    let manifest: ServerManifest =
        match tokio::time::timeout(HELLO_TIMEOUT, read_server_message(&mut control_recv, wire))
            .await
        {
            Err(_) => bail!(
                "timed out after {}s waiting for the pull manifest",
                HELLO_TIMEOUT.as_secs()
            ),
            Ok(Err(err)) => return Err(err).context("control stream failed"),
            Ok(Ok(IncomingServer::Manifest(manifest))) => manifest,
            Ok(Ok(IncomingServer::Error(err))) => bail!("server error: {}", err.message),
            Ok(Ok(_)) => bail!("server sent an unexpected message instead of a manifest"),
        };

    let (entries, total_files, total_bytes): (Vec<ManifestEntry>, u64, u64) =
        validate_manifest(&manifest.entries)?;
    if manifest.total_files != total_files || manifest.total_bytes != total_bytes {
        bail!("pull manifest counters do not match its entries");
    }
    let rel_paths: Vec<PathBuf> = entries
        .iter()
        .map(|entry| path::validate_relative(&entry.path))
        .collect::<Result<_>>()?;
    let root: PathBuf = opts.destination.clone();
    let entries: Arc<Vec<ManifestEntry>> = Arc::new(entries);
    let rel_paths: Arc<Vec<PathBuf>> = Arc::new(rel_paths);
    {
        let root = root.clone();
        let entries = Arc::clone(&entries);
        let rel_paths = Arc::clone(&rel_paths);
        let created = tokio::task::spawn_blocking(move || {
            create_manifest_entries(&root, Path::new(""), &entries, &rel_paths)
        })
        .await
        .context("manifest creation task panicked")?;
        if let Err(err) = created {
            let _ = tokio::time::timeout(
                CONTROL_WRITE_TIMEOUT,
                write_client_abort(
                    &mut control_send,
                    wire,
                    Abort {
                        message: err.to_string(),
                    },
                ),
            )
            .await;
            return Err(err);
        }
    }

    tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_client_ready(&mut control_send, wire),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "timed out after {}s signalling that the pull is ready",
            CONTROL_WRITE_TIMEOUT.as_secs()
        )
    })??;

    let activity: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let file_slots: usize = entries.len();
    let state: Arc<tokio::sync::Mutex<ReceiverState>> =
        Arc::new(tokio::sync::Mutex::new(ReceiverState {
            root,
            entries,
            rel_paths,
            ranges: (0..file_slots).map(|_| ReceivedRanges::default()).collect(),
            completed: vec![false; file_slots],
            in_progress: vec![false; file_slots],
            in_flight: vec![0; file_slots],
            pending: (0..file_slots).map(|_| None).collect(),
            pending_count: 0,
            total_files,
            total_bytes,
            files_done: 0,
            bytes_done: 0,
            wire_received: 0,
            written_bytes: 0,
            started: Instant::now(),
            progress: opts.progress.clone(),
        }));

    let (control_tx, mut control_rx): (ServerMessageSender, ServerMessageReceiver) =
        mpsc::channel(CONTROL_QUEUE);
    let control_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        loop {
            let message = read_server_message(&mut control_recv, wire).await;
            let failed = message.is_err();
            if control_tx.send(message).await.is_err() {
                break;
            }
            if failed {
                break;
            }
        }
    });

    let heartbeat_task: tokio::task::JoinHandle<()> = {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PROGRESS_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                state.lock().await.publish_progress();
            }
        })
    };

    let mut streams: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
    let mut watchdog: tokio::time::Interval = tokio::time::interval(WATCHDOG_INTERVAL);
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    watchdog.tick().await;
    let mut last_activity_seen: u64 = 0;
    let mut last_activity_at: Instant = Instant::now();
    let mut done_at: Option<Instant> = None;

    let result: Result<()> = loop {
        if let Some(started) = done_at
            && streams.is_empty()
        {
            if state.lock().await.all_complete() {
                break Ok(());
            }
            let remaining: Duration = COMPLETE_DRAIN_TIMEOUT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break Err(anyhow::anyhow!(
                    "timed out after {}s waiting for missing file data",
                    COMPLETE_DRAIN_TIMEOUT.as_secs()
                ));
            }
            match tokio::time::timeout(remaining, connection.accept_uni()).await {
                Ok(Ok(stream)) => {
                    activity.fetch_add(1, Ordering::Relaxed);
                    spawn_receive_task(&mut streams, state.clone(), stream, activity.clone());
                }
                Ok(Err(err)) => break Err(err).context("failed to accept data stream"),
                Err(_) => {
                    break Err(anyhow::anyhow!(
                        "timed out after {}s waiting for missing file data",
                        COMPLETE_DRAIN_TIMEOUT.as_secs()
                    ));
                }
            }
            continue;
        }

        tokio::select! {
            message = control_rx.recv() => {
                activity.fetch_add(1, Ordering::Relaxed);
                match message {
                    Some(Ok(IncomingServer::Done(_))) => {
                        done_at.get_or_insert_with(Instant::now);
                    }
                    Some(Ok(IncomingServer::Error(err))) => {
                        break Err(anyhow::anyhow!("server error: {}", err.message));
                    }
                    Some(Ok(IncomingServer::Progress(_))) => {}
                    Some(Ok(IncomingServer::ProfileChanged { .. })) => {}
                    Some(Ok(IncomingServer::Manifest(_))) => {
                        break Err(anyhow::anyhow!("server sent a second manifest"));
                    }
                    Some(Ok(IncomingServer::Hello(_))) => {
                        break Err(anyhow::anyhow!("server sent a second hello"));
                    }
                    Some(Err(err)) => break Err(err).context("control stream failed"),
                    None => {
                        break Err(anyhow::anyhow!(
                            "server closed the control stream before completion"
                        ));
                    }
                }
            }
            result = streams.join_next(), if !streams.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {
                        activity.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(Ok(Err(err))) => break Err(err),
                    Some(Err(err)) => break Err(err).context("receive task panicked"),
                    None => {}
                }
            }
            // Gate accepting new streams on the number still being decoded:
            // each one may buffer a full payload, so an unbounded accept loop
            // lets a peer pin memory with headers alone.
            stream = connection.accept_uni(), if streams.len() < opts.tuning.max_inflight_streams => {
                match stream {
                    Ok(stream) => {
                        activity.fetch_add(1, Ordering::Relaxed);
                        spawn_receive_task(&mut streams, state.clone(), stream, activity.clone());
                    }
                    Err(err) => break Err(err).context("failed to accept data stream"),
                }
            }
            _ = watchdog.tick() => {
                let seen: u64 = activity.load(Ordering::Relaxed);
                if seen != last_activity_seen {
                    last_activity_seen = seen;
                    last_activity_at = Instant::now();
                }
                if done_at.is_none() && last_activity_at.elapsed() >= SESSION_STALL_TIMEOUT {
                    break Err(anyhow::anyhow!(
                        "pull stalled: no data or control messages for {}s",
                        SESSION_STALL_TIMEOUT.as_secs()
                    ));
                }
            }
        }
    };
    control_task.abort();
    heartbeat_task.abort();

    if let Err(err) = result {
        streams.abort_all();
        let _ = tokio::time::timeout(
            CONTROL_WRITE_TIMEOUT,
            write_client_abort(
                &mut control_send,
                wire,
                Abort {
                    message: err.to_string(),
                },
            ),
        )
        .await;
        return Err(err);
    }

    let (files_done, bytes_done, wire_bytes): (u64, u64, u64) = {
        let state = state.lock().await;
        (state.files_done, state.bytes_done, state.wire_received)
    };
    if files_done != total_files {
        bail!("pull finished with {files_done} of {total_files} files; the transfer is incomplete");
    }
    tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_complete(
            &mut control_send,
            wire,
            Complete {
                files: files_done,
                bytes: bytes_done,
            },
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "timed out after {}s acknowledging the pull",
            CONTROL_WRITE_TIMEOUT.as_secs()
        )
    })??;
    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Complete {
            files: files_done,
            bytes: bytes_done,
        });
    }
    control_send.finish()?;

    Ok(PullStats {
        files: files_done,
        bytes: bytes_done,
        wire_bytes,
        effective_profile,
        transport: TransportStats::sample(&connection),
    })
}
