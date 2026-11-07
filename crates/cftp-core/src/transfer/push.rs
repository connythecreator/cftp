//! Client-side push: scan, encode, send, and report progress.

use super::*;

/// Scans `src` and pushes it to `addr` over QUIC.
///
/// This is the whole client half of a transfer and does not depend on the CLI:
/// the caller supplies the authentication backend, the destination path, and a
/// progress channel. Progress events are delivered through
/// [`PushOptions::progress`]; drop the receiver only after this future
/// completes, or sends become no-ops.
///
/// ```no_run
/// use std::net::SocketAddr;
/// use std::path::Path;
///
/// use cftp_core::prelude::*;
/// use cftp_core::auth::load_client_auth;
/// use cftp_core::trust::KnownHostsStore;
///
/// # async fn run(addr: SocketAddr) -> anyhow::Result<()> {
/// let auth = load_client_auth(
///     Path::new("/home/me/.ssh/id_ed25519"),
///     KnownHostsStore::load("/home/me/.ssh/known_hosts")?,
///     "receiver",
///     4433,
///     HostKeyPolicy::Ask,
///     |_| Ok(String::new()),
/// )?;
/// let opts = PushOptions {
///     destination: "project".into(),
///     ..PushOptions::default()
/// };
/// let stats = push(Path::new("./project"), addr, &auth, opts).await?;
/// println!("sent {} files, {} bytes", stats.files, stats.bytes);
/// # Ok(())
/// # }
/// ```
pub async fn push(
    src: &Path,
    addr: SocketAddr,
    auth: &ClientAuth,
    opts: PushOptions,
) -> Result<PushStats> {
    validate_tuning(&opts.tuning)?;

    let scan: scan::ScanResult = scan::scan_source(src)?;
    validate_manifest_limits(&scan.entries)?;
    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Scan {
            files: scan.total_files,
            bytes: scan.total_bytes,
        });
    }
    notice(
        &opts,
        format!(
            "scanned {} files, {} bytes; connecting to {addr}",
            scan.total_files, scan.total_bytes
        ),
    );

    let bind: SocketAddr = if addr.is_ipv6() {
        "[::]:0".parse().expect("valid ipv6 bind address")
    } else {
        "0.0.0.0:0".parse().expect("valid ipv4 bind address")
    };
    let mtu = Mtu::for_peer(addr);
    let mut endpoint: Endpoint = bind_endpoint(bind, None, &opts.tuning, mtu)?;
    let client_config = auth.client_config(&opts.tuning, opts.force_v2, mtu)?;
    endpoint.set_default_client_config(client_config);
    let connecting = endpoint
        .connect(addr, &opts.server_name)
        .with_context(|| format!("failed to start connection to {addr}"))?;
    let connection: Connection = match tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting).await {
        Err(_) => bail!(
            "timed out after {}s establishing a connection to {addr}",
            HANDSHAKE_TIMEOUT.as_secs()
        ),
        Ok(Ok(connection)) => connection,
        Ok(Err(err)) => {
            if let Some(unknown) = auth.take_unknown_host_key() {
                return Err(anyhow::Error::new(unknown));
            }
            return Err(err).with_context(|| format!("failed to connect to {addr}"));
        }
    };
    notice(&opts, format!("connected to {addr}"));

    let wire: WireVersion = if opts.force_v2 {
        WireVersion::V2
    } else {
        negotiated_version(&connection)
    };

    let (mut control_send, mut control_recv): (SendStream, RecvStream) =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.open_bi()).await {
            Err(_) => bail!(
                "timed out after {}s opening the control stream",
                HANDSHAKE_TIMEOUT.as_secs()
            ),
            Ok(result) => result?,
        };
    notice(
        &opts,
        format!("sending manifest ({} entries)", scan.entries.len()),
    );
    tokio::time::timeout(
        HELLO_TIMEOUT,
        write_client_hello(
            &mut control_send,
            wire,
            &HelloInfo {
                version: wire.number(),
                mode: TransferMode::Push,
                destination: opts.destination.clone(),
                entries: scan.entries.clone(),
                profile: opts.profile,
            },
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "timed out after {}s sending the manifest",
            HELLO_TIMEOUT.as_secs()
        )
    })??;
    notice(
        &opts,
        "waiting for the server to accept the transfer".to_string(),
    );

    let server_hello: ServerHelloInfo = match tokio::time::timeout(
        HELLO_TIMEOUT,
        read_server_hello(&mut control_recv, wire),
    )
    .await
    {
        Err(_) => bail!(
            "timed out after {}s waiting for the server hello (the server accepted the connection but is not responding)",
            HELLO_TIMEOUT.as_secs()
        ),
        Ok(result) => result?,
    };
    if !server_hello.accepted {
        bail!("server rejected transfer: {}", server_hello.message);
    }

    let mut effective_profile: HardwareProfile = if wire.has_profiles() {
        server_hello.effective_profile
    } else {
        HardwareProfile::Default
    };
    let server_cap: HardwareProfile = if wire.has_profiles() {
        server_hello.server_cap
    } else {
        HardwareProfile::Default
    };
    let mut link_speed: Option<u64> = if effective_profile == HardwareProfile::Aggressive {
        detect_link_speed(addr.ip())
    } else {
        None
    };

    let mut initial_tuning: Tuning = if wire == WireVersion::V2 {
        if opts.profile_flag_used {
            Tuning::default()
        } else {
            opts.tuning.clone()
        }
    } else if effective_profile == opts.profile {
        opts.tuning.clone()
    } else {
        Tuning::for_profile(effective_profile, link_speed)
    };
    initial_tuning.profile = effective_profile;
    if effective_profile == HardwareProfile::Aggressive {
        let (chunk, pack): (i32, i32) = compression_levels(effective_profile, link_speed);
        initial_tuning.chunk_zstd_level = chunk;
        initial_tuning.pack_zstd_level = pack;
    }
    let controller: Arc<TuningController> = TuningController::new(initial_tuning);
    apply_connection_tuning(&connection, &controller.snapshot());

    if wire.has_profiles() && (!server_hello.profile_accepted || effective_profile != opts.profile)
    {
        notice(
            &opts,
            format!(
                "profile: requested {}, server cap {}, effective {}{}",
                opts.profile,
                server_cap,
                effective_profile,
                if server_hello.profile_accepted {
                    ""
                } else {
                    " (request rejected)"
                }
            ),
        );
    } else if wire == WireVersion::V2 && opts.profile_flag_used {
        notice(
            &opts,
            "profile flags are ignored because the peer only supports cftp/2".to_string(),
        );
    }
    notice(
        &opts,
        format!("server accepted; transferring with the {effective_profile} profile"),
    );

    let (server_tx, mut server_rx): (ServerMessageSender, ServerMessageReceiver) =
        mpsc::channel::<ServerMessageResult>(CONTROL_QUEUE);
    let control_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        loop {
            let message = read_server_message(&mut control_recv, wire).await;
            let failed = message.is_err();
            if server_tx.send(message).await.is_err() {
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
    let mut stats: PushStats = PushStats {
        effective_profile,
        ..Default::default()
    };
    let mut state: SendState = SendState::default();

    let (tx, mut rx): (
        mpsc::Sender<Result<EncodedItem>>,
        mpsc::Receiver<Result<EncodedItem>>,
    ) = mpsc::channel::<Result<EncodedItem>>(ENCODED_QUEUE_CAPACITY);
    let producer: tokio::task::JoinHandle<()> =
        tokio::spawn(produce_items(scan.clone(), controller.clone(), tx));
    let mut sends: tokio::task::JoinSet<Result<SentItem>> = tokio::task::JoinSet::new();
    let mut profile_rx: Option<watch::Receiver<HardwareProfile>> = opts.profile_requests.clone();
    let mut has_hotkeys: bool = profile_rx.is_some();
    let mut pending_requests: HashSet<u64> = HashSet::new();
    let mut next_request_id: u64 = 1u64;
    let mut last_server_message: Instant = Instant::now();
    let mut stall_watchdog: tokio::time::Interval = tokio::time::interval(WATCHDOG_INTERVAL);
    stall_watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    stall_watchdog.tick().await;

    let mut optimizer: Option<NetworkOptimizer> =
        (wire.has_profiles() && effective_profile == HardwareProfile::Aggressive).then(|| {
            let snapshot = controller.snapshot();
            NetworkOptimizer::new(
                snapshot.chunk_zstd_level,
                snapshot.pack_zstd_level,
                Instant::now(),
            )
        });
    let mut adaptive: bool = optimizer.is_some();
    let mut optimizer_tick: tokio::time::Interval = tokio::time::interval(Duration::from_secs(3));
    optimizer_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    optimizer_tick.tick().await;
    let mut last_sample: Option<SampleState> = None;
    let mut last_link_check: Instant = Instant::now() - Duration::from_secs(10);

    loop {
        tokio::select! {
            biased;
            message = server_rx.recv() => {
                match message {
                    Some(Ok(message)) => {
                        last_server_message = Instant::now();
                        if let IncomingServer::ProfileChanged {
                            request_id,
                            accepted,
                            effective_profile: new_profile,
                            server_cap: new_cap,
                            message,
                        } = message
                        {
                            if pending_requests.remove(&request_id) {
                                effective_profile = new_profile;
                                let link = if effective_profile == HardwareProfile::Aggressive {
                                    link_speed = detect_link_speed(addr.ip());
                                    link_speed
                                } else {
                                    None
                                };
                                controller.set_profile(effective_profile, link);
                                apply_connection_tuning(&connection, &controller.snapshot());
                                let snapshot = controller.snapshot();
                                optimizer = (wire.has_profiles()
                                    && effective_profile == HardwareProfile::Aggressive)
                                    .then(|| NetworkOptimizer::new(
                                        snapshot.chunk_zstd_level,
                                        snapshot.pack_zstd_level,
                                        Instant::now(),
                                    ));
                                last_sample = None;
                                notice(
                                    &opts,
                                    format!(
                                        "profile request {}: {}, effective {} (server cap {}){}",
                                        request_id,
                                        if accepted { "accepted" } else { "rejected" },
                                        effective_profile,
                                        new_cap,
                                        if message.is_empty() {
                                            String::new()
                                        } else {
                                            format!(": {message}")
                                        }
                                    ),
                                );
                            }
                        } else if apply_server_message(message, &mut state, &opts, totals)?.is_some() {
                            bail!("server completed before receiving data");
                        }
                    }
                    Some(Err(err)) => return Err(err).context("control stream failed"),
                    None => bail!("server closed control stream before completion"),
                }
            }
            profile = next_profile(&mut profile_rx), if has_hotkeys => {
                match profile {
                    Some(profile) => {
                        if wire == WireVersion::V2 {
                            notice(&opts, "profile hotkeys are unavailable on cftp/2".to_string());
                        } else {
                            let request_id: u64 = next_request_id;
                            next_request_id += 1;
                            pending_requests.insert(request_id);
                            match tokio::time::timeout(
                                CONTROL_WRITE_TIMEOUT,
                                write_set_profile(&mut control_send, wire, request_id, profile),
                            )
                            .await
                            {
                                Err(_) => bail!(
                                    "timed out after {}s sending a profile request",
                                    CONTROL_WRITE_TIMEOUT.as_secs()
                                ),
                                Ok(result) => result?,
                            }
                            notice(&opts, format!("requesting {} profile", profile));
                        }
                    }
                    None => has_hotkeys = false,
                }
            }
            _ = optimizer_tick.tick(), if adaptive => {
                if wire.has_profiles() && effective_profile == HardwareProfile::Aggressive {
                    let now: Instant = Instant::now();
                    if now.duration_since(last_link_check) >= Duration::from_secs(10) {
                        last_link_check = now;
                        let detected: Option<u64> = detect_link_speed(addr.ip());
                        if detected != link_speed {
                            link_speed = detected;
                            let (chunk, pack): (i32, i32) =
                                compression_levels(HardwareProfile::Aggressive, link_speed);
                            controller.set_compression(chunk, pack);
                            if let Some(optimizer) = &mut optimizer {
                                optimizer.reset(chunk, pack, now);
                            }
                        }
                    }
                    if let Some(previous) = last_sample {
                        let previous: SampleState = previous;
                        let elapsed: f64 = now.duration_since(previous.at).as_secs_f64();
                        if elapsed >= 2.0 {
                            let path: quinn::PathStats = connection.stats().path;
                            let sample: NetworkSample = NetworkSample {
                                link_speed,
                                wire_bps: (state.sent_bytes.saturating_sub(previous.sent_bytes)) as f64 * 8.0 / elapsed,
                                rtt: path.rtt,
                                min_rtt: path.min_rtt,
                                lost_bytes: path.lost_bytes.saturating_sub(previous.lost_bytes),
                                sent_bytes: state.sent_bytes.saturating_sub(previous.sent_bytes),
                            };
                            if let Some(optimizer) = &mut optimizer
                                && let Some((chunk, pack)) = optimizer.observe(&sample, now)
                            {
                                controller.set_compression(chunk, pack);
                                notice(
                                    &opts,
                                    format!(
                                        "adaptive zstd levels: chunk {chunk}, pack {pack} (link {}, wire {:.1} Mbps)",
                                        link_speed
                                            .map(|speed| format!("{:.0} Mbps", speed as f64 / 1_000_000.0))
                                            .unwrap_or_else(|| "unknown".to_string()),
                                        sample.wire_bps / 1_000_000.0,
                                    ),
                                );
                            }
                            last_sample = Some(SampleState {
                                at: now,
                                sent_bytes: state.sent_bytes,
                                lost_bytes: path.lost_bytes,
                            });
                        }
                    } else {
                        last_sample = Some(SampleState {
                            at: now,
                            sent_bytes: state.sent_bytes,
                            lost_bytes: connection.stats().path.lost_bytes,
                        });
                    }
                }
            }
            _ = stall_watchdog.tick() => {
                if last_server_message.elapsed() >= SERVER_STALL_TIMEOUT {
                    bail!(
                        "no progress from the server for {}s; the transfer is stalled",
                        last_server_message.elapsed().as_secs()
                    );
                }
            }
            result = sends.join_next(), if !sends.is_empty() => {
                let result: Result<Result<SentItem>, tokio::task::JoinError> =
                    result.expect("send set is not empty");
                apply_sent_item(
                    result.context("send task panicked")??,
                    &mut stats,
                    &mut state,
                    &opts,
                    totals,
                );
            }
            item = rx.recv() => {
                let Some(item): Option<Result<EncodedItem>> = item else {
                    break;
                };
                let item: EncodedItem = item?;
                loop {
                    let limit: usize = controller.snapshot().max_inflight_sends;
                    if sends.len() < limit {
                        break;
                    }
                    let result: Result<Result<SentItem>, tokio::task::JoinError> =
                        sends.join_next().await.expect("send set is not empty");
                    apply_sent_item(
                        result.context("send task panicked")??,
                        &mut stats,
                        &mut state,
                        &opts,
                        totals,
                    );
                }
                let connection: Connection = connection.clone();
                sends.spawn(async move { send_item(&connection, item).await });
            }
        }
        adaptive = wire.has_profiles() && effective_profile == HardwareProfile::Aggressive;
    }
    while let Some(result) = sends.join_next().await {
        apply_sent_item(
            result.context("send task panicked")??,
            &mut stats,
            &mut state,
            &opts,
            totals,
        );
    }
    producer.await.context("producer task panicked")?;

    notice(
        &opts,
        "all data sent; waiting for the server to finalize and confirm".to_string(),
    );
    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_complete(
            &mut control_send,
            wire,
            Complete {
                files: stats.files,
                bytes: stats.bytes,
            },
        ),
    )
    .await
    {
        Err(_) => bail!(
            "timed out after {}s sending the completion message",
            CONTROL_WRITE_TIMEOUT.as_secs()
        ),
        Ok(result) => result?,
    }

    let done_deadline: Instant = Instant::now() + DONE_TIMEOUT;
    let done: Done = loop {
        let remaining: Duration = done_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!(
                "timed out after {}s waiting for the server to confirm completion",
                DONE_TIMEOUT.as_secs()
            );
        }
        match tokio::time::timeout(remaining, server_rx.recv()).await {
            Err(_) => bail!(
                "timed out after {}s waiting for the server to confirm completion",
                DONE_TIMEOUT.as_secs()
            ),
            Ok(Some(Ok(message))) => {
                if let Some(done) = apply_server_message(message, &mut state, &opts, totals)? {
                    break done;
                }
            }
            Ok(Some(Err(err))) => return Err(err).context("control stream failed"),
            Ok(None) => bail!("server closed control stream before completion"),
        }
    };
    control_task.abort();

    stats.files = done.files;
    stats.bytes = done.bytes;
    stats.effective_profile = effective_profile;
    stats.transport = TransportStats::sample(&connection);

    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Complete {
            files: stats.files,
            bytes: stats.bytes,
        });
    }

    control_send.finish()?;
    Ok(stats)
}

pub(super) fn notify(progress: &Option<ProgressSender>, message: impl Into<String>) {
    match progress {
        Some(progress) => {
            let _ = progress.send(ProgressEvent::Notice(message.into()));
        }
        None => tracing::info!("{}", message.into()),
    }
}

#[derive(Clone, Copy)]
struct SampleState {
    at: Instant,
    sent_bytes: u64,
    lost_bytes: u64,
}

async fn next_profile(
    rx: &mut Option<watch::Receiver<HardwareProfile>>,
) -> Option<HardwareProfile> {
    match rx {
        Some(rx) => match rx.changed().await {
            Ok(()) => Some(*rx.borrow_and_update()),
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

fn notice(opts: &PushOptions, message: String) {
    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Notice(message));
    } else {
        tracing::info!("{message}");
    }
}

#[derive(Debug, Default)]
pub(super) struct SendState {
    pub(super) read_bytes: u64,
    pub(super) sent_bytes: u64,
    pub(super) bytes_done: u64,
    pub(super) files_done: u64,
    pub(super) remote_received: u64,
    pub(super) remote_written: u64,
    pub(super) remote_elapsed_millis: u64,
}

fn apply_server_message(
    message: IncomingServer,
    state: &mut SendState,
    opts: &PushOptions,
    totals: Totals,
) -> Result<Option<Done>> {
    match message {
        IncomingServer::Progress(progress) => {
            state.remote_received = progress.received_bytes;
            state.remote_written = progress.written_bytes;
            state.remote_elapsed_millis = progress.elapsed_millis;
            publish_push_progress(opts, state, totals);
            Ok(None)
        }
        IncomingServer::Done(done) => Ok(Some(done)),
        IncomingServer::Error(err) => bail!("server error: {}", err.message),
        IncomingServer::Hello(_) => bail!("unexpected server hello"),
        IncomingServer::Manifest(_) => bail!("unexpected pull manifest in a push"),
        IncomingServer::ProfileChanged { .. } => Ok(None),
    }
}

#[derive(Clone, Copy)]
pub(super) struct Totals {
    pub(super) files: u64,
    pub(super) bytes: u64,
}

pub(super) fn publish_push_progress(opts: &PushOptions, state: &SendState, totals: Totals) {
    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Sending(TransferProgress {
            at: Instant::now(),
            bytes_done: state.bytes_done,
            bytes_total: totals.bytes,
            files_done: state.files_done,
            files_total: totals.files,
            read_bytes: state.read_bytes,
            sent_bytes: state.sent_bytes,
            received_bytes: state.remote_received,
            written_bytes: state.remote_written,
            remote_elapsed_millis: state.remote_elapsed_millis,
        }));
    }
}
