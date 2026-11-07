//! Receiving side of a push: control loop and manifest validation.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn receive_connection(
    connection: Connection,
    mut control_send: SendStream,
    mut control_recv: RecvStream,
    root: PathBuf,
    progress: Option<ProgressSender>,
    peer: SocketAddr,
    wire: WireVersion,
    max_profile: HardwareProfile,
    initial_profile: HardwareProfile,
    policy: ServePolicy,
) -> Result<ServeStats> {
    let hello: HelloInfo = match tokio::time::timeout(
        CLIENT_HELLO_TIMEOUT,
        read_client_hello(&mut control_recv, wire),
    )
    .await
    {
        Ok(Ok(hello)) => hello,
        Ok(Err(err)) => {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
        Err(_) => {
            let err: anyhow::Error = anyhow::anyhow!(
                "client did not send a hello within {}s",
                CLIENT_HELLO_TIMEOUT.as_secs()
            );
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };

    match hello.mode {
        TransferMode::Pull => {
            let err: Option<anyhow::Error> = if wire != WireVersion::V4 {
                Some(anyhow::anyhow!(
                    "pull requires cftp/4; this connection negotiated cftp/{}",
                    wire.number()
                ))
            } else if !policy.allow_pull {
                Some(anyhow::anyhow!("pull is disabled on this server"))
            } else {
                None
            };
            if let Some(err) = err {
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
            return serve_pull(
                connection,
                control_send,
                control_recv,
                root,
                hello,
                policy,
                progress,
                peer,
                wire,
                max_profile,
                initial_profile,
            )
            .await;
        }
        TransferMode::Push => {
            if !policy.allow_push {
                let err: anyhow::Error = anyhow::anyhow!("push is disabled on this server");
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
        }
    }

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
    let mut server_tuning: Tuning = Tuning::for_profile(effective_profile, None);
    apply_connection_tuning(&connection, &server_tuning);
    let profile_notice: String = format!(
        "profile: requested {}, server cap {}, effective {}",
        hello.profile, max_profile, effective_profile
    );
    if let Some(progress) = &progress {
        let _ = progress.send(ProgressEvent::Notice(profile_notice));
    } else {
        tracing::info!("{profile_notice}");
    }

    let destination: PathBuf = match path::validate_destination(&hello.destination) {
        Ok(destination) => destination,
        Err(err) => {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };
    // Every entry stores `destination.join(relative)`, so a long destination is
    // multiplied by the manifest size. Bound that product before allocating it.
    if hello.destination.len().saturating_mul(hello.entries.len()) > cftp_proto::MAX_CONTROL_FRAME {
        let err: anyhow::Error = anyhow::anyhow!(
            "destination is too long for a manifest with {} entries",
            hello.entries.len()
        );
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }

    let (entries, total_files, total_bytes): (Vec<ManifestEntry>, u64, u64) =
        match validate_manifest(&hello.entries) {
            Ok(validated) => validated,
            Err(err) => {
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
        };
    // Entries are ordered by id, so `rel_paths[id - 1]` is the destination path.
    let mut rel_paths: Vec<PathBuf> = Vec::with_capacity(entries.len());
    for entry in &entries {
        let relative: PathBuf = match path::validate_relative(&entry.path) {
            Ok(relative) => relative,
            Err(err) => {
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
        };
        rel_paths.push(destination.join(relative));
    }

    // Create the directory/symlink tree before accepting data, but off the async
    // runtime. The manifest is already validated and bounded, so this cannot be
    // used to create an unbounded number of filesystem objects.
    let entries: Arc<Vec<ManifestEntry>> = Arc::new(entries);
    let rel_paths: Arc<Vec<PathBuf>> = Arc::new(rel_paths);
    {
        let root = root.clone();
        let destination = destination.clone();
        let entries = Arc::clone(&entries);
        let rel_paths = Arc::clone(&rel_paths);
        let created = tokio::task::spawn_blocking(move || {
            create_manifest_entries(&root, &destination, &entries, &rel_paths)
        })
        .await
        .context("manifest creation task panicked")?;
        if let Err(err) = created {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    }

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
                server_cap: if wire.has_profiles() {
                    max_profile
                } else {
                    HardwareProfile::Default
                },
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
    if !profile_message.is_empty() {
        tracing::debug!("{profile_message}");
    }

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
            progress,
        }));

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

    // Timer-driven telemetry: stream completions alone are too coarse, and a
    // silent UI cannot distinguish a slow transfer from a stalled one.
    let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel::<ServerProgress>(4);
    let heartbeat_task: tokio::task::JoinHandle<()> = {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PROGRESS_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                let progress = {
                    let state = state.lock().await;
                    state.publish_progress();
                    state.server_progress()
                };
                if heartbeat_tx.send(progress).await.is_err() {
                    break;
                }
            }
        })
    };

    let mut complete: bool = false;
    let mut complete_at: Option<Instant> = None;
    let mut streams: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
    let mut watchdog: tokio::time::Interval = tokio::time::interval(WATCHDOG_INTERVAL);
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    watchdog.tick().await;
    let mut last_activity_seen: u64 = 0;
    let mut last_activity_at: Instant = Instant::now();

    let result: Result<()> = loop {
        if complete && streams.is_empty() {
            let all_complete: bool = { state.lock().await.all_complete() };
            if all_complete {
                break Ok(());
            }
            // Absolute deadline: an empty pack must not reset it.
            let remaining: Duration = COMPLETE_DRAIN_TIMEOUT
                .saturating_sub(complete_at.unwrap_or_else(Instant::now).elapsed());
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
                Ok(Err(err)) => {
                    break Err(err).context("failed to accept data stream");
                }
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
                    Some(Ok(IncomingClient::Complete(_))) => {
                        complete = true;
                        complete_at.get_or_insert_with(Instant::now);
                    }
                    Some(Ok(IncomingClient::Abort(abort))) => {
                        break Err(anyhow::anyhow!("client aborted: {}", abort.message));
                    }
                    Some(Ok(IncomingClient::SetProfile { request_id, profile })) => {
                        if wire.has_profiles() {
                            let (accepted, message) = if profile <= max_profile {
                                current_profile = profile;
                                effective_profile = profile;
                                server_tuning = Tuning::for_profile(effective_profile, None);
                                apply_connection_tuning(&connection, &server_tuning);
                                (true, String::new())
                            } else {
                                effective_profile = current_profile;
                                (
                                    false,
                                    format!(
                                        "requested profile {profile} exceeds server cap {max_profile}"
                                    ),
                                )
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
                                    message,
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
                        }
                    }
                    Some(Ok(IncomingClient::Hello(_))) => {
                        break Err(anyhow::anyhow!("client sent a second hello"));
                    }
                    Some(Ok(IncomingClient::Ready)) => {
                        break Err(anyhow::anyhow!("client signalled readiness for a push"));
                    }
                    Some(Err(err)) => {
                        break Err(err).context("control stream failed");
                    }
                    None => {
                        break Err(anyhow::anyhow!("control stream closed before completion"));
                    }
                }
            }
            Some(progress) = heartbeat_rx.recv() => {
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
            result = streams.join_next(), if !streams.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {
                        activity.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(Ok(Err(err))) => {
                        break Err(err);
                    }
                    Some(Err(err)) => {
                        break Err(err).context("receive task panicked");
                    }
                    None => {}
                }
            }
            stream = connection.accept_uni(), if streams.len() < server_tuning.max_inflight_streams => {
                match stream {
                    Ok(stream) => {
                        activity.fetch_add(1, Ordering::Relaxed);
                        spawn_receive_task(&mut streams, state.clone(), stream, activity.clone());
                    }
                    Err(err) => {
                        break Err(err).context("failed to accept data stream");
                    }
                }
            }
            _ = watchdog.tick() => {
                let seen: u64 = activity.load(Ordering::Relaxed);
                if seen != last_activity_seen {
                    last_activity_seen = seen;
                    last_activity_at = Instant::now();
                }
                if let Some(started) = complete_at {
                    if started.elapsed() >= COMPLETE_DRAIN_TIMEOUT
                        && !state.lock().await.all_complete()
                    {
                        break Err(anyhow::anyhow!(
                            "timed out after {}s waiting for missing file data",
                            COMPLETE_DRAIN_TIMEOUT.as_secs()
                        ));
                    }
                } else if last_activity_at.elapsed() >= SESSION_STALL_TIMEOUT {
                    break Err(anyhow::anyhow!(
                        "transfer stalled: no data or control messages for {}s",
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
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }

    let (done, server_progress): (Done, ServerProgress) = {
        let state = state.lock().await;
        (
            Done {
                files: state.files_done,
                bytes: state.bytes_done,
            },
            state.server_progress(),
        )
    };
    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_progress(&mut control_send, wire, server_progress),
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
    control_send.finish()?;
    // Keep the connection alive long enough for the client to read the final
    // message. Dropping the last local handle closes the connection.
    let _ = tokio::time::timeout(Duration::from_secs(1), connection.closed()).await;
    {
        let state = state.lock().await;
        if let Some(progress) = &state.progress {
            let _ = progress.send(ProgressEvent::Complete {
                files: done.files,
                bytes: done.bytes,
            });
        }
    }

    Ok(ServeStats {
        files: done.files,
        bytes: done.bytes,
        peer: Some(peer),
        effective_profile,
    })
}

pub(super) fn spawn_receive_task(
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    state: Arc<tokio::sync::Mutex<ReceiverState>>,
    stream: RecvStream,
    activity: Arc<AtomicU64>,
) {
    tasks.spawn(async move {
        let decoded = decode_stream(stream, activity).await?;
        match decoded {
            DecodedData::Pack {
                header,
                raw,
                entries,
            } => {
                // Reserve the files under the lock, then do the blocking writes
                // off it so one slow filesystem cannot stall other receivers or
                // the control loop.
                let (root, jobs) = {
                    let mut state = state.lock().await;
                    (
                        state.root.clone(),
                        state.prepare_pack(&header, &entries, raw.len())?,
                    )
                };
                let done = tokio::task::spawn_blocking(move || -> Result<Vec<(u64, u64)>> {
                    let mut done = Vec::with_capacity(jobs.len());
                    for job in &jobs {
                        let contents = raw.get(job.range.clone()).with_context(|| {
                            format!("pack entry {} exceeds the payload", job.file_id)
                        })?;
                        path::write_atomic(&root, &job.relative, contents, job.mode, job.mtime)?;
                        done.push((job.file_id, job.size));
                    }
                    Ok(done)
                })
                .await
                .context("pack writer task panicked")??;
                let mut state = state.lock().await;
                state.finish_pack(done)?;
            }
            DecodedData::Chunk { header, raw } => {
                let (file, offset, len, file_id) = {
                    let mut state = state.lock().await;
                    state.prepare_chunk(&header, raw.len())?
                };
                tokio::task::spawn_blocking(move || write_at(&file, offset, &raw))
                    .await
                    .context("chunk writer task panicked")??;
                let mut state = state.lock().await;
                state.finish_chunk(file_id, len)?;
            }
        }
        Ok(())
    });
}

/// Rejects manifests that would exhaust memory, inodes, or disk bookkeeping
/// before anything is created on the filesystem.
pub(super) fn validate_manifest_limits(entries: &[ManifestEntry]) -> Result<(u64, u64)> {
    if entries.len() > cftp_proto::MAX_MANIFEST_ENTRIES {
        bail!(
            "manifest has {} entries; the maximum is {}",
            entries.len(),
            cftp_proto::MAX_MANIFEST_ENTRIES
        );
    }
    let mut files: u64 = 0u64;
    let mut bytes: u64 = 0u64;
    let mut encoded_bytes: usize = 0usize;
    for entry in entries {
        if entry.path.len() > cftp_proto::MAX_MANIFEST_PATH_LEN {
            bail!(
                "manifest path is longer than {} bytes: {}",
                cftp_proto::MAX_MANIFEST_PATH_LEN,
                entry.path
            );
        }
        if let Some(target) = &entry.link_target
            && target.len() > cftp_proto::MAX_MANIFEST_PATH_LEN
        {
            bail!(
                "symlink target is longer than {} bytes: {}",
                cftp_proto::MAX_MANIFEST_PATH_LEN,
                entry.path
            );
        }
        encoded_bytes = encoded_bytes
            .saturating_add(entry.path.len())
            .saturating_add(entry.link_target.as_deref().map_or(0, str::len))
            .saturating_add(64);
        if entry.kind == EntryKind::File {
            if entry.size > cftp_proto::MAX_FILE_SIZE {
                bail!(
                    "{} declares {} bytes; the per-file maximum is {}",
                    entry.path,
                    entry.size,
                    cftp_proto::MAX_FILE_SIZE
                );
            }
            files = files
                .checked_add(1)
                .context("manifest file count overflow")?;
            bytes = bytes
                .checked_add(entry.size)
                .context("manifest byte count overflow")?;
        }
    }
    if encoded_bytes > cftp_proto::MAX_CONTROL_FRAME {
        bail!(
            "manifest is too large to send in one message; split the source into smaller transfers"
        );
    }
    if bytes > cftp_proto::MAX_TRANSFER_BYTES {
        bail!(
            "transfer declares {} bytes; the maximum is {}",
            bytes,
            cftp_proto::MAX_TRANSFER_BYTES
        );
    }
    Ok((files, bytes))
}

/// Validates a manifest and returns its entries ordered by id.
///
/// Ids must be dense and start at 1. The sender (`scan_source`) always assigns
/// them that way, and the density lets the receiver index every per-file table
/// directly by `id - 1` instead of hashing integers in the hot path.
pub(super) fn validate_manifest(
    entries: &[ManifestEntry],
) -> Result<(Vec<ManifestEntry>, u64, u64)> {
    let (total_files, total_bytes): (u64, u64) = validate_manifest_limits(entries)?;
    let mut ordered: Vec<Option<ManifestEntry>> = Vec::new();
    ordered.resize_with(entries.len(), || None);
    let mut paths = HashSet::new();
    for entry in entries {
        let index: usize = entry
            .id
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .with_context(|| format!("manifest entry has an invalid id ({})", entry.id))?;
        if index >= entries.len() {
            bail!("manifest id {} is out of range", entry.id);
        }
        let relative = path::validate_relative(&entry.path)?;
        let normalized = relative.to_string_lossy().replace('\\', "/");
        if !paths.insert(normalized) {
            bail!("duplicate manifest path {}", entry.path);
        }
        if entry.kind == EntryKind::Symlink && entry.link_target.is_none() {
            bail!("symlink {} has no target", entry.path);
        }
        if ordered[index].replace(entry.clone()).is_some() {
            bail!("duplicate manifest id {}", entry.id);
        }
    }
    let ordered: Vec<ManifestEntry> = ordered
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            entry.with_context(|| format!("manifest is missing id {}", index + 1))
        })
        .collect::<Result<_>>()?;
    Ok((ordered, total_files, total_bytes))
}
