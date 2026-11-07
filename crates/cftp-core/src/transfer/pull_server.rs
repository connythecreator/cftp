//! Server side of a pull: resolve, scan, manifest, and stream.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn serve_pull(
    connection: Connection,
    mut control_send: SendStream,
    mut control_recv: RecvStream,
    root: PathBuf,
    hello: HelloInfo,
    policy: ServePolicy,
    progress: Option<ProgressSender>,
    peer: SocketAddr,
    wire: WireVersion,
    max_profile: HardwareProfile,
    initial_profile: HardwareProfile,
) -> Result<ServeStats> {
    let mut current_profile: HardwareProfile = initial_profile;
    let mut effective_profile: HardwareProfile = HardwareProfile::Default;
    let mut profile_accepted: bool = true;
    let mut profile_message: String = String::new();
    if wire.has_profiles() {
        if hello.profile <= max_profile {
            current_profile = hello.profile;
            effective_profile = hello.profile;
        } else {
            effective_profile = current_profile;
            profile_accepted = false;
            profile_message = format!(
                "requested profile {} exceeds server cap {}",
                hello.profile, max_profile
            );
        }
    }
    let server_tuning: Tuning = Tuning::for_profile(effective_profile, None);
    apply_connection_tuning(&connection, &server_tuning);
    notify(
        &progress,
        format!(
            "pull from {peer}: requested {}, server cap {}, effective {}",
            hello.profile, max_profile, effective_profile
        ),
    );
    if !profile_message.is_empty() {
        tracing::debug!("{profile_message}");
    }

    let relative: PathBuf = match path::validate_relative(&hello.destination) {
        Ok(relative) => relative,
        Err(err) => {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };
    let trailing_slash: bool =
        hello.destination.ends_with('/') || hello.destination.ends_with('\\');
    let resolved: PathBuf = match path::resolve_read_path(&root, &relative) {
        Ok(resolved) => resolved,
        Err(err) => {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };
    let meta: fs::Metadata = match fs::metadata(&resolved) {
        Ok(meta) => meta,
        Err(err) => {
            let err: anyhow::Error = Err::<(), _>(err)
                .with_context(|| format!("failed to inspect pull source {}", resolved.display()))
                .unwrap_err();
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };
    if trailing_slash && !meta.is_dir() {
        let err: anyhow::Error =
            anyhow::anyhow!("trailing slash requires a directory: {}", hello.destination);
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }
    let scan_path: PathBuf = if trailing_slash {
        let mut path = resolved.clone().into_os_string();
        path.push("/");
        PathBuf::from(path)
    } else {
        resolved
    };

    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_hello(
            &mut control_send,
            wire,
            &ServerHelloInfo {
                version: wire.number(),
                accepted: true,
                message: "ready".to_string(),
                effective_profile,
                server_cap: max_profile,
                profile_accepted,
                allow_push: policy.allow_push,
                allow_pull: policy.allow_pull,
            },
        ),
    )
    .await
    {
        Err(_) => bail!(
            "timed out after {}s sending the server hello",
            CONTROL_WRITE_TIMEOUT.as_secs()
        ),
        Ok(result) => result?,
    }

    // Scanning happens after the hello so the client learns the policy before
    // the server spends time walking the tree.
    let scan: scan::ScanResult = match scan::scan_source(&scan_path) {
        Ok(scan) => scan,
        Err(err) => {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };
    if let Err(err) = validate_manifest_limits(&scan.entries) {
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }
    notify(
        &progress,
        format!(
            "serving {} files, {} bytes to {peer}",
            scan.total_files, scan.total_bytes
        ),
    );
    let manifest: ServerManifest = ServerManifest {
        entries: scan.entries.clone(),
        total_files: scan.total_files,
        total_bytes: scan.total_bytes,
    };
    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_manifest(&mut control_send, wire, manifest),
    )
    .await
    {
        Err(_) => bail!(
            "timed out after {}s sending the pull manifest",
            CONTROL_WRITE_TIMEOUT.as_secs()
        ),
        Ok(result) => result?,
    }

    // Wait for the client to create the tree before streaming any data.
    let controller: Arc<TuningController> = TuningController::new(server_tuning);
    let mut ready: bool = false;
    while !ready {
        let message: IncomingClient =
            match tokio::time::timeout(HELLO_TIMEOUT, read_client_message(&mut control_recv, wire))
                .await
            {
                Err(_) => {
                    let err: anyhow::Error = anyhow::anyhow!(
                        "client did not signal readiness within {}s",
                        HELLO_TIMEOUT.as_secs()
                    );
                    send_server_error(&mut control_send, wire, &err).await;
                    return Err(err);
                }
                Ok(Err(err)) => {
                    send_server_error(&mut control_send, wire, &err).await;
                    return Err(err);
                }
                Ok(Ok(message)) => message,
            };
        match message {
            IncomingClient::Ready => ready = true,
            IncomingClient::Abort(abort) => {
                let err: anyhow::Error = anyhow::anyhow!("client aborted: {}", abort.message);
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
            IncomingClient::SetProfile {
                request_id,
                profile,
            } => {
                let (accepted, response) = match handle_profile_request(
                    &connection,
                    &mut current_profile,
                    &mut effective_profile,
                    &controller,
                    max_profile,
                    profile,
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        send_server_error(&mut control_send, wire, &err).await;
                        return Err(err);
                    }
                };
                match tokio::time::timeout(
                    CONTROL_WRITE_TIMEOUT,
                    write_profile_changed(
                        &mut control_send,
                        wire,
                        request_id,
                        accepted,
                        effective_profile,
                        max_profile,
                        response,
                    ),
                )
                .await
                {
                    Err(_) => bail!(
                        "timed out after {}s sending a profile response",
                        CONTROL_WRITE_TIMEOUT.as_secs()
                    ),
                    Ok(result) => result?,
                }
            }
            IncomingClient::Hello(_) => {
                let err: anyhow::Error = anyhow::anyhow!("client sent a second hello");
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
            IncomingClient::Complete(_) => {
                let err: anyhow::Error = anyhow::anyhow!("client completed before receiving data");
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
        }
    }

    let (control_tx, mut control_rx): (ClientMessageSender, ClientMessageReceiver) =
        mpsc::channel(CONTROL_QUEUE);
    let control_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        loop {
            let message = read_client_message(&mut control_recv, wire).await;
            let failed = message.is_err();
            if control_tx.send(message).await.is_err() {
                break;
            }
            if failed {
                break;
            }
        }
    });

    let totals: Totals = Totals {
        files: scan.total_files,
        bytes: scan.total_bytes,
    };
    let started: Instant = Instant::now();
    let mut stats: ServeStats = ServeStats {
        peer: Some(peer),
        effective_profile,
        ..Default::default()
    };
    let mut state: SendState = SendState::default();
    let mut heartbeat: tokio::time::Interval = tokio::time::interval(PROGRESS_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;

    let (tx, mut rx): (
        mpsc::Sender<Result<EncodedItem>>,
        mpsc::Receiver<Result<EncodedItem>>,
    ) = mpsc::channel::<Result<EncodedItem>>(ENCODED_QUEUE_CAPACITY);
    let producer: tokio::task::JoinHandle<()> =
        tokio::spawn(produce_items(scan, controller.clone(), tx));
    let mut sends: tokio::task::JoinSet<Result<SentItem>> = tokio::task::JoinSet::new();
    let mut producer_done: bool = false;
    let mut watchdog: tokio::time::Interval = tokio::time::interval(WATCHDOG_INTERVAL);
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    watchdog.tick().await;
    let mut last_activity_at: Instant = Instant::now();

    let result: Result<()> = loop {
        if producer_done && sends.is_empty() {
            break Ok(());
        }

        tokio::select! {
            message = control_rx.recv() => {
                match message {
                    Some(Ok(IncomingClient::Abort(abort))) => {
                        break Err(anyhow::anyhow!("client aborted: {}", abort.message));
                    }
                    Some(Ok(IncomingClient::SetProfile { request_id, profile })) => {
                        match handle_profile_request(
                            &connection,
                            &mut current_profile,
                            &mut effective_profile,
                            &controller,
                            max_profile,
                            profile,
                        ) {
                            Ok((accepted, response)) => {
                                match tokio::time::timeout(
                                    CONTROL_WRITE_TIMEOUT,
                                    write_profile_changed(
                                        &mut control_send,
                                        wire,
                                        request_id,
                                        accepted,
                                        effective_profile,
                                        max_profile,
                                        response,
                                    ),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(err)) => {
                                        break Err(err).context("failed to send profile response");
                                    }
                                    Err(_) => {
                                        break Err(anyhow::anyhow!(
                                            "timed out after {}s sending a profile response",
                                            CONTROL_WRITE_TIMEOUT.as_secs()
                                        ));
                                    }
                                }
                                stats.effective_profile = effective_profile;
                            }
                            Err(err) => {
                                break Err(err);
                            }
                        }
                    }
                    Some(Ok(IncomingClient::Complete(_))) => {
                        // A pull client acknowledges only after `Done`; an
                        // early acknowledgement is harmless and ignored.
                    }
                    Some(Ok(IncomingClient::Ready)) => {
                        break Err(anyhow::anyhow!("client signalled readiness twice"));
                    }
                    Some(Ok(IncomingClient::Hello(_))) => {
                        break Err(anyhow::anyhow!("client sent a second hello"));
                    }
                    Some(Err(err)) => {
                        break Err(err).context("control stream failed");
                    }
                    None => {
                        break Err(anyhow::anyhow!("control stream closed during the pull"));
                    }
                }
            }
            _ = heartbeat.tick() => {
                let progress: ServerProgress = ServerProgress {
                    files_done: stats.files,
                    files_total: totals.files,
                    bytes_total: totals.bytes,
                    received_bytes: state.sent_bytes,
                    written_bytes: 0,
                    elapsed_millis: started.elapsed().as_millis() as u64,
                };
                match tokio::time::timeout(
                    CONTROL_WRITE_TIMEOUT,
                    write_server_progress(&mut control_send, wire, progress),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => break Err(err).context("failed to send progress"),
                    Err(_) => {
                        break Err(anyhow::anyhow!(
                            "timed out after {}s sending progress (the client stopped reading)",
                            CONTROL_WRITE_TIMEOUT.as_secs()
                        ));
                    }
                }
            }
            item = rx.recv(), if !producer_done => {
                match item {
                    Some(Ok(item)) => {
                        last_activity_at = Instant::now();
                        let connection: Connection = connection.clone();
                        sends.spawn(async move { send_item(&connection, item).await });
                    }
                    Some(Err(err)) => break Err(err),
                    None => producer_done = true,
                }
            }
            result = sends.join_next(), if !sends.is_empty() => {
                match result {
                    Some(Ok(Ok(item))) => {
                        apply_pull_sent_item(item, &mut stats, &mut state, totals, &progress);
                        last_activity_at = Instant::now();
                    }
                    Some(Ok(Err(err))) => break Err(err),
                    Some(Err(err)) => break Err(err).context("send task panicked"),
                    None => {}
                }
            }
            _ = watchdog.tick() => {
                if last_activity_at.elapsed() >= SESSION_STALL_TIMEOUT {
                    break Err(anyhow::anyhow!(
                        "transfer stalled: no data or control messages for {}s",
                        SESSION_STALL_TIMEOUT.as_secs()
                    ));
                }
            }
        }
    };

    if let Err(err) = result {
        control_task.abort();
        sends.abort_all();
        producer.abort();
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }
    producer.await.context("producer task panicked")?;

    let done: Done = Done {
        files: stats.files,
        bytes: stats.bytes,
    };
    let final_progress: ServerProgress = ServerProgress {
        files_done: stats.files,
        files_total: totals.files,
        bytes_total: totals.bytes,
        received_bytes: state.sent_bytes,
        written_bytes: 0,
        elapsed_millis: started.elapsed().as_millis() as u64,
    };
    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_progress(&mut control_send, wire, final_progress),
    )
    .await
    {
        Err(_) => bail!("timed out sending the final progress message"),
        Ok(result) => result?,
    }
    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_done(&mut control_send, wire, done.clone()),
    )
    .await
    {
        Err(_) => bail!("timed out sending the completion message"),
        Ok(result) => result?,
    }

    // Wait for the client to acknowledge, so it can write its `Complete` frame
    // before the connection closes. A missing acknowledgement is not fatal.
    let deadline: Instant = Instant::now() + PULL_ACK_TIMEOUT;
    loop {
        let remaining: Duration = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, control_rx.recv()).await {
            Ok(Some(Ok(IncomingClient::Complete(_)))) => break,
            Ok(Some(Ok(IncomingClient::Abort(_)))) | Ok(None) => break,
            Ok(Some(Err(_))) | Err(_) => break,
            Ok(Some(Ok(_))) => {}
        }
    }
    control_task.abort();
    control_send.finish()?;
    if let Some(progress) = &progress {
        let _ = progress.send(ProgressEvent::Complete {
            files: done.files,
            bytes: done.bytes,
        });
    }

    Ok(ServeStats {
        files: done.files,
        bytes: done.bytes,
        peer: Some(peer),
        effective_profile,
    })
}

pub(super) fn handle_profile_request(
    connection: &Connection,
    current_profile: &mut HardwareProfile,
    effective_profile: &mut HardwareProfile,
    controller: &Arc<TuningController>,
    max_profile: HardwareProfile,
    profile: HardwareProfile,
) -> Result<(bool, String)> {
    if profile <= max_profile {
        *current_profile = profile;
        *effective_profile = profile;
        controller.set_profile(profile, None);
        apply_connection_tuning(connection, &controller.snapshot());
        Ok((true, String::new()))
    } else {
        *effective_profile = *current_profile;
        Ok((
            false,
            format!("requested profile {profile} exceeds server cap {max_profile}"),
        ))
    }
}

pub(super) fn apply_pull_sent_item(
    item: SentItem,
    stats: &mut ServeStats,
    state: &mut SendState,
    totals: Totals,
    progress: &Option<ProgressSender>,
) {
    stats.bytes = stats.bytes.saturating_add(item.logical_bytes);
    state.read_bytes = state.read_bytes.saturating_add(item.logical_bytes);
    state.sent_bytes = state.sent_bytes.saturating_add(item.wire_bytes);
    state.bytes_done = state.bytes_done.saturating_add(item.logical_bytes);
    if item.pack {
        stats.files = stats.files.saturating_add(item.files);
        state.files_done = state.files_done.saturating_add(item.files);
    } else if item.file_done {
        stats.files = stats.files.saturating_add(1);
        state.files_done = state.files_done.saturating_add(1);
    }
    if let Some(progress) = progress {
        let _ = progress.send(ProgressEvent::Sending(TransferProgress {
            at: Instant::now(),
            bytes_done: state.bytes_done,
            bytes_total: totals.bytes,
            files_done: state.files_done,
            files_total: totals.files,
            read_bytes: state.read_bytes,
            sent_bytes: state.sent_bytes,
            received_bytes: 0,
            written_bytes: 0,
            remote_elapsed_millis: 0,
        }));
    }
}

pub(super) fn create_manifest_entries(
    root: &Path,
    destination: &Path,
    entries: &[ManifestEntry],
    rel_paths: &[PathBuf],
) -> Result<()> {
    if !destination.as_os_str().is_empty() {
        path::create_directory(root, destination, 0o755, 0)?;
    }

    for (index, entry) in entries.iter().enumerate() {
        let Some(relative) = rel_paths.get(index) else {
            continue;
        };
        match entry.kind {
            EntryKind::Directory => {
                path::create_directory(root, relative, entry.mode, entry.mtime)?;
            }
            EntryKind::Symlink => {
                let target = entry
                    .link_target
                    .as_deref()
                    .with_context(|| format!("symlink {} has no target", entry.path))?;
                path::create_symlink(root, relative, target)?;
            }
            EntryKind::File => {}
        }
    }
    Ok(())
}
