use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cftp_proto::{
    Abort, ChunkHeader, Codec, Complete, DataHeader, Done, EntryKind, HardwareProfile,
    ManifestEntry, PackEntry, PackHeader, ServerManifest, ServerProgress, TransferMode,
};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tokio::io::AsyncReadExt;
use tokio::sync::{Semaphore, mpsc, watch};

use crate::auth::{ClientAuth, ServerAuth};
use crate::codec;
use crate::identity::DEFAULT_SERVER_NAME;
use crate::link::{NetworkOptimizer, NetworkSample, compression_levels, detect_link_speed};
use crate::path;
use crate::progress::{ProgressEvent, ProgressSender, TransferProgress};
use crate::scan;
use crate::tuning::{MAX_INFLIGHT_SENDS, Tuning, TuningController, apply_connection_tuning};

/// How long to wait for the QUIC handshake and the control stream to appear.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long either peer will wait for the other's hello/manifest.
const HELLO_TIMEOUT: Duration = Duration::from_secs(120);
/// Idle timeout for writing a single data stream.
const STREAM_WRITE_IDLE: Duration = Duration::from_secs(60);
/// Idle timeout for reading a single data stream.
const STREAM_READ_IDLE: Duration = Duration::from_secs(60);
/// Maximum time a control write may block before the peer is considered wedged.
const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(15);
/// A session with no data or control activity for this long is considered dead.
const SESSION_STALL_TIMEOUT: Duration = Duration::from_secs(120);
/// Time allowed for the client to finish after it has sent `Complete`.
const COMPLETE_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);
/// Time allowed between server messages on the client side. The server sends a
/// progress heartbeat every second, so silence this long means it is wedged.
const SERVER_STALL_TIMEOUT: Duration = Duration::from_secs(300);
/// Absolute time allowed for the server to confirm `Done` after `Complete`.
const DONE_TIMEOUT: Duration = Duration::from_secs(120);
/// Time a pulling server waits for the client's acknowledgement after `Done`.
/// The data is already delivered at that point, so a missing acknowledgement is
/// not an error.
const PULL_ACK_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the server sends progress telemetry.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
/// How often the server checks for stalls.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);
/// Maximum number of partially received files held open at once.
const MAX_PENDING_FILES: usize = 4096;
type ServerMessageResult = Result<IncomingServer, anyhow::Error>;
type ClientMessageResult = Result<IncomingClient, anyhow::Error>;
type ServerMessageSender = mpsc::Sender<ServerMessageResult>;
type ServerMessageReceiver = mpsc::Receiver<ServerMessageResult>;
type ClientMessageSender = mpsc::Sender<ClientMessageResult>;
type ClientMessageReceiver = mpsc::Receiver<ClientMessageResult>;

/// Bound on queued control messages. The control stream is low-volume; a bound
/// stops a peer from growing our memory by flooding it.
const CONTROL_QUEUE: usize = 64;

use crate::wire::{
    HelloInfo, IncomingClient, IncomingServer, ServerHelloInfo, WireVersion, negotiated_version,
    read_client_hello, read_client_message, read_server_hello, read_server_message,
    write_client_abort, write_client_hello, write_client_ready, write_complete,
    write_profile_changed, write_server_done, write_server_error, write_server_hello,
    write_server_manifest, write_server_progress, write_set_profile,
};

/// Client-side transfer settings.
///
/// `destination` is the path relative to the server's root. Build one with
/// [`PushOptions::default`] and change only the fields you need.
#[derive(Debug, Clone)]
pub struct PushOptions {
    pub destination: String,
    pub server_name: String,
    pub progress: Option<ProgressSender>,
    pub profile: HardwareProfile,
    pub tuning: Tuning,
    pub profile_flag_used: bool,
    pub force_v2: bool,
    pub profile_requests: Option<watch::Receiver<HardwareProfile>>,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            destination: String::new(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            progress: None,
            profile: HardwareProfile::Default,
            tuning: Tuning::default(),
            profile_flag_used: false,
            force_v2: false,
            profile_requests: None,
        }
    }
}

/// Counters from one completed client-side transfer.
#[derive(Debug, Clone, Default)]
pub struct PushStats {
    pub files: u64,
    pub bytes: u64,
    pub packs: u64,
    pub chunks: u64,
    pub effective_profile: HardwareProfile,
}

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
    let mut endpoint: Endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(auth.client_config(&opts.tuning, opts.force_v2)?);
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
    ) = mpsc::channel::<Result<EncodedItem>>(MAX_INFLIGHT_SENDS);
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

    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Complete {
            files: stats.files,
            bytes: stats.bytes,
        });
    }

    control_send.finish()?;
    Ok(stats)
}

/// Client-side pull settings.
///
/// `source` is the path relative to the server root, and `destination` is the
/// local directory that received entries are written under. A trailing slash on
/// `source` pulls a directory's contents instead of the directory itself,
/// mirroring [`PushOptions::destination`] on the push side.
#[derive(Debug, Clone)]
pub struct PullOptions {
    pub source: String,
    pub destination: PathBuf,
    pub server_name: String,
    pub progress: Option<ProgressSender>,
    pub profile: HardwareProfile,
    pub tuning: Tuning,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            source: String::new(),
            destination: PathBuf::new(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            progress: None,
            profile: HardwareProfile::Default,
            tuning: Tuning::default(),
        }
    }
}

/// Counters from one completed client-side pull.
#[derive(Debug, Clone, Default)]
pub struct PullStats {
    pub files: u64,
    pub bytes: u64,
    /// Payload bytes received over the wire, before decompression.
    pub wire_bytes: u64,
    pub effective_profile: HardwareProfile,
}

fn notify(progress: &Option<ProgressSender>, message: impl Into<String>) {
    match progress {
        Some(progress) => {
            let _ = progress.send(ProgressEvent::Notice(message.into()));
        }
        None => tracing::info!("{}", message.into()),
    }
}

async fn connect_client(
    addr: SocketAddr,
    auth: &ClientAuth,
    tuning: &Tuning,
    force_v2: bool,
    server_name: &str,
) -> Result<(Endpoint, Connection)> {
    let bind: SocketAddr = if addr.is_ipv6() {
        "[::]:0".parse().expect("valid ipv6 bind address")
    } else {
        "0.0.0.0:0".parse().expect("valid ipv4 bind address")
    };
    let mut endpoint: Endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(auth.client_config(tuning, force_v2)?);
    let connecting = endpoint
        .connect(addr, server_name)
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
    Ok((endpoint, connection))
}

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
            stream = connection.accept_uni() => {
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
    })
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

fn validate_tuning(tuning: &Tuning) -> Result<()> {
    if tuning.chunk_size == 0 || tuning.chunk_size > cftp_proto::MAX_DATA_PAYLOAD {
        bail!(
            "chunk size must be between 1 and {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if tuning.pack_target == 0 || tuning.pack_target > cftp_proto::MAX_DATA_PAYLOAD {
        bail!(
            "pack target must be between 1 and {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if tuning.small_file_max > cftp_proto::MAX_DATA_PAYLOAD as u64 {
        bail!(
            "small-file threshold cannot exceed {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if tuning.pack_max_files == 0 {
        bail!("pack max files must be greater than zero");
    }
    if tuning.max_inflight_sends == 0 || tuning.max_inflight_streams == 0 {
        bail!("in-flight limits must be greater than zero");
    }
    Ok(())
}

#[derive(Debug, Default)]
struct SendState {
    read_bytes: u64,
    sent_bytes: u64,
    bytes_done: u64,
    files_done: u64,
    remote_received: u64,
    remote_written: u64,
    remote_elapsed_millis: u64,
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

struct BuiltPack {
    header: PackHeader,
    payload: Vec<u8>,
    logical_bytes: u64,
    files: u64,
}

enum EncodedItem {
    Pack {
        header: PackHeader,
        payload: Vec<u8>,
        logical_bytes: u64,
        files: u64,
    },
    Chunk {
        header: ChunkHeader,
        payload: Vec<u8>,
        logical_bytes: u64,
        file_done: bool,
    },
}

async fn produce_items(
    scan: scan::ScanResult,
    controller: Arc<TuningController>,
    tx: mpsc::Sender<Result<EncodedItem>>,
) {
    if let Err(err) = produce_items_inner(scan, controller, &tx).await {
        let _ = tx.send(Err(err)).await;
    }
}

async fn produce_items_inner(
    scan: scan::ScanResult,
    controller: Arc<TuningController>,
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    let permits = controller.permits().clone();
    let mut tasks = tokio::task::JoinSet::new();
    let mut pack_entries = Vec::new();
    let mut pack_bytes = 0u64;
    let mut pack_id = 0u64;

    for entry in scan
        .entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
    {
        let tuning = controller.snapshot();
        if entry.size <= tuning.small_file_max {
            pack_bytes = pack_bytes.saturating_add(entry.size);
            pack_entries.push(entry.clone());
            if pack_bytes >= tuning.pack_target as u64
                || pack_entries.len() >= tuning.pack_max_files
            {
                enqueue_pack(
                    &mut tasks,
                    &permits,
                    &scan.base,
                    &mut pack_entries,
                    &mut pack_id,
                    tuning.pack_zstd_level,
                    tx,
                )
                .await?;
                pack_bytes = 0;
            }
        } else {
            if !pack_entries.is_empty() {
                enqueue_pack(
                    &mut tasks,
                    &permits,
                    &scan.base,
                    &mut pack_entries,
                    &mut pack_id,
                    tuning.pack_zstd_level,
                    tx,
                )
                .await?;
                pack_bytes = 0;
            }
            enqueue_large(&mut tasks, &permits, &scan.base, entry, &controller, tx).await?;
        }
        drain_encoding_tasks(&mut tasks)?;
    }

    if !pack_entries.is_empty() {
        let tuning = controller.snapshot();
        enqueue_pack(
            &mut tasks,
            &permits,
            &scan.base,
            &mut pack_entries,
            &mut pack_id,
            tuning.pack_zstd_level,
            tx,
        )
        .await?;
    }

    while let Some(result) = tasks.join_next().await {
        result.context("encoding task panicked")??;
    }
    Ok(())
}

fn drain_encoding_tasks(tasks: &mut tokio::task::JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = tasks.try_join_next() {
        result.context("encoding task panicked")??;
    }
    Ok(())
}

async fn enqueue_pack(
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    permits: &Arc<Semaphore>,
    base: &Path,
    entries: &mut Vec<ManifestEntry>,
    pack_id: &mut u64,
    pack_level: i32,
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let entries: Vec<ManifestEntry> = std::mem::take(entries);
    let base: PathBuf = base.to_path_buf();
    let id: u64 = *pack_id;
    *pack_id += 1;
    let permit: tokio::sync::OwnedSemaphorePermit = permits
        .clone()
        .acquire_owned()
        .await
        .context("encoding semaphore closed")?;
    let tx: mpsc::Sender<Result<EncodedItem>> = tx.clone();
    tasks.spawn(async move {
        let _permit: tokio::sync::OwnedSemaphorePermit = permit;
        let built: BuiltPack =
            tokio::task::spawn_blocking(move || build_pack(base, entries, id, pack_level))
                .await??;
        tx.send(Ok(EncodedItem::Pack {
            header: built.header,
            payload: built.payload,
            logical_bytes: built.logical_bytes,
            files: built.files,
        }))
        .await
        .map_err(|_| anyhow::anyhow!("send pipeline closed"))?;
        Ok(())
    });
    Ok(())
}

fn build_pack(
    base: PathBuf,
    entries: Vec<ManifestEntry>,
    pack_id: u64,
    pack_level: i32,
) -> Result<BuiltPack> {
    let mut pack_entries: Vec<PackEntry> = Vec::with_capacity(entries.len());
    let mut payload: Vec<u8> = Vec::new();
    let mut compressible: bool = false;
    let mut logical_bytes: u64 = 0u64;

    for entry in &entries {
        let full_path: PathBuf = base.join(Path::new(&entry.path));
        let data: Vec<u8> = fs::read(&full_path)
            .with_context(|| format!("failed to read {}", full_path.display()))?;
        if data.len() as u64 != entry.size {
            bail!("{} changed while scanning", full_path.display());
        }
        logical_bytes += entry.size;
        compressible |= !codec::extension_is_incompressible(&entry.path);
        pack_entries.push(PackEntry {
            file_id: entry.id,
            size: data.len() as u64,
            hash: codec::hash(&data),
        });
        payload.extend_from_slice(&data);
    }

    let (chunk_codec, encoded): (Codec, Vec<u8>) = if compressible {
        codec::compress(&payload, pack_level, true)?
    } else {
        (Codec::Raw, payload)
    };
    let header: PackHeader = PackHeader {
        pack_id,
        codec: chunk_codec,
        uncompressed_len: logical_bytes,
        compressed_len: encoded.len() as u64,
        entries: pack_entries,
    };

    Ok(BuiltPack {
        header,
        payload: encoded,
        logical_bytes,
        files: entries.len() as u64,
    })
}

async fn enqueue_large(
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    permits: &Arc<Semaphore>,
    base: &Path,
    entry: &ManifestEntry,
    controller: &Arc<TuningController>,
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    let full_path = base.join(Path::new(&entry.path));
    let mut file: tokio::fs::File = tokio::fs::File::open(&full_path)
        .await
        .with_context(|| format!("failed to open {}", full_path.display()))?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut offset: u64 = 0u64;
    let allow_skip: bool = !codec::extension_is_incompressible(&entry.path);

    loop {
        let tuning: Tuning = controller.snapshot();
        let chunk_size: usize = tuning.chunk_size.max(1);
        if buffer.len() != chunk_size {
            buffer.resize(chunk_size, 0);
        }
        let chunk_level: i32 = tuning.chunk_zstd_level;
        let permit: tokio::sync::OwnedSemaphorePermit = permits
            .clone()
            .acquire_owned()
            .await
            .context("encoding semaphore closed")?;
        let read: usize = file
            .read(&mut buffer[..chunk_size])
            .await
            .with_context(|| format!("failed to read {}", full_path.display()))?;
        if read == 0 {
            break;
        }
        let data: Vec<u8> = buffer[..read].to_vec();
        let done: bool = offset + read as u64 == entry.size;
        let file_id: u64 = entry.id;
        let tx: mpsc::Sender<Result<EncodedItem>> = tx.clone();
        let chunk_offset: u64 = offset;
        let chunk_len: usize = read;
        tasks.spawn(async move {
            let _permit = permit;
            let (hash, chunk_codec, encoded): ([u8; 32], Codec, Vec<u8>) =
                tokio::task::spawn_blocking(move || {
                    let hash = codec::hash(&data);
                    let (chunk_codec, encoded) = if allow_skip {
                        codec::compress(&data, chunk_level, true)?
                    } else {
                        (Codec::Raw, data)
                    };
                    Ok::<_, anyhow::Error>((hash, chunk_codec, encoded))
                })
                .await??;
            let header: ChunkHeader = ChunkHeader {
                file_id,
                offset: chunk_offset,
                uncompressed_len: chunk_len as u32,
                compressed_len: encoded.len() as u32,
                codec: chunk_codec,
                hash,
            };
            tx.send(Ok(EncodedItem::Chunk {
                header,
                payload: encoded,
                logical_bytes: chunk_len as u64,
                file_done: done,
            }))
            .await
            .map_err(|_| anyhow::anyhow!("send pipeline closed"))?;
            Ok(())
        });
        offset += read as u64;
    }

    if offset != entry.size {
        bail!("{} changed while scanning", full_path.display());
    }
    Ok(())
}

struct SentItem {
    wire_bytes: u64,
    logical_bytes: u64,
    files: u64,
    file_done: bool,
    pack: bool,
}

async fn send_item(connection: &Connection, item: EncodedItem) -> Result<SentItem> {
    match item {
        EncodedItem::Pack {
            header,
            payload,
            logical_bytes,
            files,
        } => {
            let wire_bytes = payload.len() as u64;
            let mut stream = open_uni(connection).await?;
            write_data_header(&mut stream, &DataHeader::Pack(header)).await?;
            write_all_idle(&mut stream, &payload).await?;
            stream.finish()?;
            Ok(SentItem {
                wire_bytes,
                logical_bytes,
                files,
                file_done: false,
                pack: true,
            })
        }
        EncodedItem::Chunk {
            header,
            payload,
            logical_bytes,
            file_done,
        } => {
            let wire_bytes = payload.len() as u64;
            let mut stream = open_uni(connection).await?;
            write_data_header(&mut stream, &DataHeader::Chunk(header)).await?;
            write_all_idle(&mut stream, &payload).await?;
            stream.finish()?;
            Ok(SentItem {
                wire_bytes,
                logical_bytes,
                files: 0,
                file_done,
                pack: false,
            })
        }
    }
}

async fn open_uni(connection: &Connection) -> Result<SendStream> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.open_uni()).await {
        Err(_) => bail!(
            "timed out after {}s opening a data stream (the peer is not accepting streams)",
            HANDSHAKE_TIMEOUT.as_secs()
        ),
        Ok(result) => result.context("failed to open data stream"),
    }
}

async fn write_data_header(stream: &mut SendStream, header: &DataHeader) -> Result<()> {
    match tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        cftp_proto::write_frame(stream, header),
    )
    .await
    {
        Err(_) => bail!(
            "timed out after {}s writing a data header",
            CONTROL_WRITE_TIMEOUT.as_secs()
        ),
        Ok(result) => result.context("failed to write data header")?,
    }
    Ok(())
}

async fn write_all_idle(stream: &mut SendStream, mut data: &[u8]) -> Result<()> {
    while !data.is_empty() {
        let written = match tokio::time::timeout(STREAM_WRITE_IDLE, stream.write(data)).await {
            Err(_) => bail!(
                "timed out after {}s writing data (the peer stopped reading)",
                STREAM_WRITE_IDLE.as_secs()
            ),
            Ok(result) => result.context("failed to write data")?,
        };
        if written == 0 {
            bail!("data stream closed while writing");
        }
        data = &data[written..];
    }
    Ok(())
}

fn apply_sent_item(
    item: SentItem,
    stats: &mut PushStats,
    state: &mut SendState,
    opts: &PushOptions,
    totals: Totals,
) {
    stats.bytes += item.logical_bytes;
    state.read_bytes = state.read_bytes.saturating_add(item.logical_bytes);
    state.sent_bytes = state.sent_bytes.saturating_add(item.wire_bytes);
    state.bytes_done = state.bytes_done.saturating_add(item.logical_bytes);

    if item.pack {
        stats.files += item.files;
        stats.packs += 1;
        state.files_done = state.files_done.saturating_add(item.files);
    } else {
        stats.chunks += 1;
        if item.file_done {
            stats.files += 1;
            state.files_done = state.files_done.saturating_add(1);
        }
    }

    publish_push_progress(opts, state, totals);
}
#[derive(Clone, Copy)]
struct Totals {
    files: u64,
    bytes: u64,
}

fn publish_push_progress(opts: &PushOptions, state: &SendState, totals: Totals) {
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

/// Maximum number of sessions a persistent server runs at once. Further
/// connections wait for a slot instead of being accepted, which bounds the
/// memory and file descriptors a burst of clients can consume.
pub const MAX_CONCURRENT_SESSIONS: usize = 4;

/// Receives one [`ServeStats`] value per completed session.
pub type SessionReporter = mpsc::UnboundedSender<ServeStats>;

/// Server-side mode policy.
///
/// The server is authoritative: a disabled mode is rejected with a message and
/// the choice is reported to clients in the `cftp/4` server hello.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServePolicy {
    pub allow_push: bool,
    pub allow_pull: bool,
}

impl Default for ServePolicy {
    fn default() -> Self {
        Self {
            allow_push: true,
            allow_pull: true,
        }
    }
}

impl ServePolicy {
    pub fn push_only() -> Self {
        Self {
            allow_push: true,
            allow_pull: false,
        }
    }

    pub fn pull_only() -> Self {
        Self {
            allow_push: false,
            allow_pull: true,
        }
    }
}

#[derive(Debug)]
pub struct ServeOptions {
    pub root: PathBuf,
    pub listen: SocketAddr,
    pub progress: Option<ProgressSender>,
    pub on_ready: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
    pub max_profile: HardwareProfile,
    pub force_v2: bool,
    /// Which transfer modes this server accepts.
    pub policy: ServePolicy,
    /// Serve exactly one successful session and then close the endpoint.
    ///
    /// Defaults to `true` so existing library callers keep the old one-shot
    /// behaviour. The CLI opts into a persistent server.
    pub one_shot: bool,
    /// Upper bound on concurrent sessions when `one_shot` is false.
    pub max_sessions: usize,
    /// Per-session results for a persistent server.
    pub on_session: Option<SessionReporter>,
}

impl ServeOptions {
    pub fn new(root: impl Into<PathBuf>, listen: SocketAddr) -> Self {
        Self {
            root: root.into(),
            listen,
            progress: None,
            on_ready: None,
            max_profile: HardwareProfile::Default,
            force_v2: false,
            policy: ServePolicy::default(),
            one_shot: true,
            max_sessions: MAX_CONCURRENT_SESSIONS,
            on_session: None,
        }
    }
}

/// Counters from one completed server-side session.
#[derive(Debug, Clone, Default)]
pub struct ServeStats {
    pub files: u64,
    pub bytes: u64,
    pub peer: Option<SocketAddr>,
    pub effective_profile: HardwareProfile,
}

/// Serves transfers until the endpoint closes.
///
/// With `one_shot`, this returns after the first successful session, which is
/// the historical behaviour. Otherwise it keeps accepting sessions, running at
/// most `max_sessions` of them concurrently; further clients wait for a slot.
pub async fn serve(auth: &ServerAuth, mut opts: ServeOptions) -> Result<ServeStats> {
    let root: PathBuf = opts.root.clone();
    let meta: fs::Metadata = fs::metadata(&root)
        .with_context(|| format!("failed to inspect root {}", root.display()))?;
    if !meta.is_dir() {
        bail!("root is not a directory: {}", root.display());
    }
    if !opts.policy.allow_push && !opts.policy.allow_pull {
        bail!("server policy disables both push and pull");
    }

    let current_profile: HardwareProfile = opts.max_profile.min(HardwareProfile::Default);
    let initial_tuning: Tuning = Tuning::for_profile(current_profile, None);
    let endpoint: Endpoint = Endpoint::server(
        auth.server_config(&initial_tuning, opts.force_v2)?,
        opts.listen,
    )?;
    let local_addr: SocketAddr = endpoint.local_addr()?;
    if let Some(on_ready) = opts.on_ready.take() {
        let _ = on_ready.send(local_addr);
    }

    if opts.one_shot {
        let connection: Connection = accept_connection(&endpoint).await?;
        let result: Result<ServeStats> = serve_connection(
            &connection,
            &root,
            opts.progress,
            opts.max_profile,
            current_profile,
            opts.force_v2,
            opts.policy,
        )
        .await;
        let _ = tokio::time::timeout(Duration::from_secs(1), endpoint.wait_idle()).await;
        endpoint.close(0u32.into(), b"done");
        return result;
    }

    tracing::info!(
        "serving on {local_addr} (root {}, up to {} sessions)",
        root.display(),
        opts.max_sessions.max(1)
    );
    let slots: Arc<Semaphore> = Arc::new(Semaphore::new(opts.max_sessions.max(1)));
    let mut sessions: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        let incoming: Option<quinn::Incoming> = tokio::select! {
            incoming = endpoint.accept() => incoming,
            Some(finished) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(err) = finished {
                    tracing::warn!("session task failed: {err}");
                }
                continue;
            }
        };
        let Some(incoming) = incoming else {
            break;
        };
        // Skip failed handshakes instead of exiting: an unauthenticated client
        // (or a QUIC scanner) must not take the server down.
        let connection: Connection = match incoming.await {
            Ok(connection) => connection,
            Err(err) => {
                tracing::warn!("ignoring a failed client handshake: {err}");
                continue;
            }
        };
        // Backpressure: stop accepting while every slot is busy. Permits are
        // released when a session task ends, even if it panics.
        let Ok(permit) = slots.clone().acquire_owned().await else {
            break;
        };
        let root: PathBuf = root.clone();
        let progress: Option<ProgressSender> = opts.progress.clone();
        let reporter: Option<SessionReporter> = opts.on_session.clone();
        let max_profile: HardwareProfile = opts.max_profile;
        let force_v2: bool = opts.force_v2;
        let policy: ServePolicy = opts.policy;
        sessions.spawn(async move {
            let _permit: tokio::sync::OwnedSemaphorePermit = permit;
            match serve_connection(
                &connection,
                &root,
                progress,
                max_profile,
                current_profile,
                force_v2,
                policy,
            )
            .await
            {
                Ok(stats) => {
                    if let Some(reporter) = reporter {
                        let _ = reporter.send(stats);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "session from {} failed: {err:#}",
                        connection.remote_address()
                    );
                }
            }
        });
    }

    while let Some(finished) = sessions.join_next().await {
        if let Err(err) = finished {
            tracing::warn!("session task failed: {err}");
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), endpoint.wait_idle()).await;
    endpoint.close(0u32.into(), b"done");
    Ok(ServeStats::default())
}

/// Serves exactly one successful session and then closes the endpoint.
pub async fn serve_once(auth: &ServerAuth, mut opts: ServeOptions) -> Result<ServeStats> {
    opts.one_shot = true;
    serve(auth, opts).await
}

async fn accept_connection(endpoint: &Endpoint) -> Result<Connection> {
    loop {
        let incoming: quinn::Incoming = endpoint
            .accept()
            .await
            .context("listener closed before a client connected")?;
        match incoming.await {
            Ok(connection) => return Ok(connection),
            Err(err) => {
                tracing::warn!("ignoring a failed client handshake: {err}");
            }
        }
    }
}

async fn serve_connection(
    connection: &Connection,
    root: &Path,
    progress: Option<ProgressSender>,
    max_profile: HardwareProfile,
    initial_profile: HardwareProfile,
    force_v2: bool,
    policy: ServePolicy,
) -> Result<ServeStats> {
    let peer: SocketAddr = connection.remote_address();
    let wire: WireVersion = if force_v2 {
        WireVersion::V2
    } else {
        negotiated_version(connection)
    };

    let (control_send, control_recv): (SendStream, RecvStream) =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.accept_bi()).await {
            Err(_) => bail!(
                "client did not open a control stream within {}s",
                HANDSHAKE_TIMEOUT.as_secs()
            ),
            Ok(result) => result.context("client did not open a control stream")?,
        };

    let result: Result<ServeStats> = receive_connection(
        connection.clone(),
        control_send,
        control_recv,
        root.to_path_buf(),
        progress,
        peer,
        wire,
        max_profile,
        initial_profile,
        policy,
    )
    .await;

    // Give the peer a moment to collect the final control frames before the
    // connection is torn down; an unresponsive peer costs at most a second.
    let _ = tokio::time::timeout(Duration::from_secs(1), connection.closed()).await;
    connection.close(0u32.into(), b"done");
    result
}

#[allow(clippy::too_many_arguments)]
async fn receive_connection(
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
    let hello: HelloInfo =
        match tokio::time::timeout(HELLO_TIMEOUT, read_client_hello(&mut control_recv, wire)).await
        {
            Ok(Ok(hello)) => hello,
            Ok(Err(err)) => {
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
            Err(_) => {
                let err: anyhow::Error = anyhow::anyhow!(
                    "client did not send a manifest within {}s",
                    HELLO_TIMEOUT.as_secs()
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

#[allow(clippy::too_many_arguments)]
async fn serve_pull(
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
    ) = mpsc::channel::<Result<EncodedItem>>(MAX_INFLIGHT_SENDS);
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

fn handle_profile_request(
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

fn apply_pull_sent_item(
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

async fn send_server_error(control_send: &mut SendStream, wire: WireVersion, err: &anyhow::Error) {
    let _ = tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_error(
            control_send,
            wire,
            Abort {
                message: err.to_string(),
            },
        ),
    )
    .await;
    let _ = control_send.finish();
}

fn create_manifest_entries(
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

fn spawn_receive_task(
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

#[cfg(unix)]
fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(data, offset)
        .context("failed to write chunk payload")
}

#[cfg(windows)]
fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0usize;
    while written < data.len() {
        let count = file
            .seek_write(&data[written..], offset + written as u64)
            .context("failed to write chunk payload")?;
        if count == 0 {
            bail!("failed to write chunk payload");
        }
        written += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = file.try_clone().context("failed to clone chunk file")?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)
        .context("failed to write chunk payload")
}

enum DecodedData {
    Pack {
        header: PackHeader,
        raw: Vec<u8>,
        entries: Vec<(u64, Range<usize>)>,
    },
    Chunk {
        header: ChunkHeader,
        raw: Vec<u8>,
    },
}

async fn decode_stream(mut stream: RecvStream, activity: Arc<AtomicU64>) -> Result<DecodedData> {
    let header: DataHeader = match tokio::time::timeout(
        STREAM_READ_IDLE,
        cftp_proto::read_frame_with_limit::<_, DataHeader>(
            &mut stream,
            cftp_proto::MAX_DATA_HEADER,
        ),
    )
    .await
    {
        Err(_) => bail!(
            "timed out after {}s waiting for a data header",
            STREAM_READ_IDLE.as_secs()
        ),
        Ok(result) => result.context("failed to read data header")?,
    };
    activity.fetch_add(1, Ordering::Relaxed);

    match header {
        DataHeader::Pack(pack) => {
            if pack.entries.is_empty() {
                bail!("pack has no entries");
            }
            if pack.entries.len() > cftp_proto::MAX_PACK_ENTRIES {
                bail!("pack has too many entries");
            }
            if pack.compressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("pack payload too large");
            }
            if pack.uncompressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("pack expands beyond the maximum payload size");
            }
            let mut encoded = vec![0u8; pack.compressed_len as usize];
            read_exact_idle(&mut stream, &mut encoded, &activity).await?;
            let (pack, raw, entries) = tokio::task::spawn_blocking(move || {
                let raw = codec::decompress(pack.codec, &encoded, pack.uncompressed_len as usize)?;
                let mut offset = 0usize;
                let mut entries = Vec::with_capacity(pack.entries.len());
                for entry in &pack.entries {
                    let size = usize::try_from(entry.size).context("pack entry is too large")?;
                    let end = offset.checked_add(size).context("pack size overflow")?;
                    if end > raw.len() {
                        bail!("pack entry {} exceeds the payload", entry.file_id);
                    }
                    if codec::hash(&raw[offset..end]) != entry.hash {
                        bail!("hash mismatch for pack entry {}", entry.file_id);
                    }
                    entries.push((entry.file_id, offset..end));
                    offset = end;
                }
                if offset != raw.len() {
                    bail!("pack payload contains trailing bytes");
                }
                Ok::<_, anyhow::Error>((pack, raw, entries))
            })
            .await??;
            Ok(DecodedData::Pack {
                header: pack,
                raw,
                entries,
            })
        }
        DataHeader::Chunk(chunk) => {
            if chunk.uncompressed_len == 0 {
                bail!("chunk has an empty payload");
            }
            if chunk.compressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("chunk payload too large");
            }
            if chunk.uncompressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("chunk expands beyond the maximum payload size");
            }
            let mut encoded = vec![0u8; chunk.compressed_len as usize];
            read_exact_idle(&mut stream, &mut encoded, &activity).await?;
            let (chunk, raw) = tokio::task::spawn_blocking(move || {
                let raw =
                    codec::decompress(chunk.codec, &encoded, chunk.uncompressed_len as usize)?;
                if codec::hash(&raw) != chunk.hash {
                    bail!("hash mismatch for chunk");
                }
                Ok::<_, anyhow::Error>((chunk, raw))
            })
            .await??;
            Ok(DecodedData::Chunk { header: chunk, raw })
        }
    }
}

/// Reads exactly `buf.len()` bytes, failing only when the peer makes no progress
/// for `STREAM_READ_IDLE`. A total-duration timeout would break legitimately
/// slow links.
async fn read_exact_idle(
    stream: &mut RecvStream,
    buf: &mut [u8],
    activity: &AtomicU64,
) -> Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let read =
            match tokio::time::timeout(STREAM_READ_IDLE, stream.read(&mut buf[filled..])).await {
                Err(_) => bail!(
                    "timed out after {}s waiting for a data payload",
                    STREAM_READ_IDLE.as_secs()
                ),
                Ok(Err(err)) => return Err(err).context("failed to read data payload"),
                Ok(Ok(None)) => {
                    bail!(
                        "data stream closed with {} bytes missing",
                        buf.len() - filled
                    )
                }
                Ok(Ok(Some(read))) => read,
            };
        filled += read;
        activity.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// Rejects manifests that would exhaust memory, inodes, or disk bookkeeping
/// before anything is created on the filesystem.
fn validate_manifest_limits(entries: &[ManifestEntry]) -> Result<(u64, u64)> {
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
fn validate_manifest(entries: &[ManifestEntry]) -> Result<(Vec<ManifestEntry>, u64, u64)> {
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

struct PendingFile {
    file: File,
    temp_path: PathBuf,
    final_path: PathBuf,
    entry: ManifestEntry,
}

impl Drop for PendingFile {
    fn drop(&mut self) {
        // A transfer that aborts must not leave partial temp files behind. After
        // a successful rename this is a no-op (the temp path no longer exists).
        let _ = fs::remove_file(&self.temp_path);
    }
}

/// A file reserved for writing from a pack.
struct PackWrite {
    file_id: u64,
    relative: PathBuf,
    range: Range<usize>,
    size: u64,
    mode: u32,
    mtime: i64,
}

#[derive(Debug, Default)]
struct ReceivedRanges {
    ranges: BTreeMap<u64, u64>,
}

impl ReceivedRanges {
    fn insert(&mut self, start: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let end = start.checked_add(len).context("chunk range overflow")?;

        let (key, mut covered_until) = match self.ranges.range(..=start).next_back() {
            Some((&existing_start, &existing_end)) => {
                if existing_end > start {
                    bail!("overlapping chunk at offset {start}");
                }
                if existing_end == start {
                    (existing_start, end)
                } else {
                    (start, end)
                }
            }
            None => (start, end),
        };

        if let Some((&next_start, &next_end)) = self.ranges.range(start..).next() {
            if next_start < end {
                bail!("overlapping chunk at offset {start}");
            }
            if next_start == end {
                covered_until = next_end;
                self.ranges.remove(&next_start);
            }
        }
        if key != start {
            self.ranges.remove(&key);
        }
        self.ranges.insert(key, covered_until);
        Ok(())
    }

    fn covers(&self, size: u64) -> bool {
        size == 0 || self.ranges.len() == 1 && self.ranges.first_key_value() == Some((&0, &size))
    }
}

struct ReceiverState {
    root: PathBuf,
    /// Manifest entries ordered by id. `entries[id - 1]` is the entry.
    entries: Arc<Vec<ManifestEntry>>,
    /// Destination paths ordered by id.
    rel_paths: Arc<Vec<PathBuf>>,
    /// Per-file chunk coverage, indexed by `id - 1`.
    ranges: Vec<ReceivedRanges>,
    /// Per-file completion flag, indexed by `id - 1`.
    completed: Vec<bool>,
    /// Files reserved by a pack but not yet written.
    in_progress: Vec<bool>,
    /// Per-file count of chunk writes that have been reserved but not finished.
    in_flight: Vec<usize>,
    /// Per-file open temp handle, indexed by `id - 1`.
    pending: Vec<Option<PendingFile>>,
    /// Number of `Some` values in `pending`, so the cap check stays O(1).
    pending_count: usize,
    total_files: u64,
    total_bytes: u64,
    files_done: u64,
    bytes_done: u64,
    wire_received: u64,
    written_bytes: u64,
    started: Instant,
    progress: Option<ProgressSender>,
}

impl ReceiverState {
    /// Maps an id on the wire to an index, rejecting unknown ids.
    fn slot(&self, file_id: u64) -> Option<usize> {
        file_id
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .filter(|&index| index < self.entries.len())
    }

    fn all_complete(&self) -> bool {
        self.files_done == self.total_files
    }

    /// Validates a pack under the lock and reserves every file it contains.
    /// The actual writes happen outside the lock.
    fn prepare_pack(
        &mut self,
        pack: &PackHeader,
        entries: &[(u64, Range<usize>)],
        raw_len: usize,
    ) -> Result<Vec<PackWrite>> {
        self.wire_received = self.wire_received.saturating_add(pack.compressed_len);
        if entries.is_empty() {
            bail!("pack has no entries");
        }
        if entries.len() > cftp_proto::MAX_PACK_ENTRIES {
            bail!("pack has too many entries");
        }
        if pack.uncompressed_len as usize != raw_len {
            bail!("pack payload has the wrong size");
        }

        let mut jobs: Vec<PackWrite> = Vec::with_capacity(entries.len());
        for (file_id, range) in entries {
            let index: usize = self
                .slot(*file_id)
                .with_context(|| format!("unknown file id {file_id}"))?;
            if self.entries[index].kind != EntryKind::File {
                bail!("pack entry {} is not a file", self.entries[index].path);
            }
            if self.completed[index] || self.in_progress[index] {
                bail!("received duplicate data for {}", self.entries[index].path);
            }
            if range.len() as u64 != self.entries[index].size {
                bail!("pack entry {} has the wrong size", self.entries[index].path);
            }
            self.in_progress[index] = true;
            jobs.push(PackWrite {
                file_id: *file_id,
                relative: self.rel_paths[index].clone(),
                range: range.clone(),
                size: self.entries[index].size,
                mode: self.entries[index].mode,
                mtime: self.entries[index].mtime,
            });
        }
        Ok(jobs)
    }

    fn finish_pack(&mut self, done: Vec<(u64, u64)>) -> Result<()> {
        for (file_id, size) in done {
            if let Some(index) = self.slot(file_id) {
                self.in_progress[index] = false;
            }
            self.written_bytes = self.written_bytes.saturating_add(size);
            self.complete_file(file_id, size)?;
        }
        Ok(())
    }

    /// Validates a chunk, reserves its range, and returns a handle positioned
    /// for a write that happens outside the lock.
    fn prepare_chunk(
        &mut self,
        chunk: &ChunkHeader,
        raw_len: usize,
    ) -> Result<(File, u64, u64, u64)> {
        self.wire_received = self
            .wire_received
            .saturating_add(chunk.compressed_len as u64);

        if chunk.uncompressed_len == 0 {
            bail!("chunk has an empty payload");
        }
        let index: usize = self
            .slot(chunk.file_id)
            .with_context(|| format!("unknown file id {}", chunk.file_id))?;
        if self.entries[index].kind != EntryKind::File {
            bail!("chunk entry {} is not a file", self.entries[index].path);
        }
        if self.completed[index] {
            bail!("received duplicate chunk for {}", self.entries[index].path);
        }
        if chunk.offset.saturating_add(chunk.uncompressed_len as u64) > self.entries[index].size {
            bail!("chunk for {} exceeds file size", self.entries[index].path);
        }
        if raw_len != chunk.uncompressed_len as usize {
            bail!("chunk for {} has the wrong size", self.entries[index].path);
        }

        if self.pending[index].is_none() {
            if self.pending_count >= MAX_PENDING_FILES {
                bail!("too many partially received files (maximum {MAX_PENDING_FILES})");
            }
            let (file, temp_path, final_path) =
                path::create_temp_file(&self.root, &self.rel_paths[index])?;
            file.set_len(self.entries[index].size)?;
            self.pending[index] = Some(PendingFile {
                file,
                temp_path,
                final_path,
                entry: self.entries[index].clone(),
            });
            self.pending_count += 1;
        }

        // Reserve before writing so overlapping or duplicate chunks are rejected
        // without racing another writer.
        self.ranges[index].insert(chunk.offset, chunk.uncompressed_len as u64)?;
        self.in_flight[index] = self.in_flight[index].saturating_add(1);

        let file = self.pending[index]
            .as_ref()
            .context("pending file disappeared")?
            .file
            .try_clone()
            .context("failed to clone pending file handle")?;
        Ok((
            file,
            chunk.offset,
            chunk.uncompressed_len as u64,
            chunk.file_id,
        ))
    }

    fn finish_chunk(&mut self, file_id: u64, len: u64) -> Result<()> {
        self.written_bytes = self.written_bytes.saturating_add(len);

        let index: usize = self
            .slot(file_id)
            .context("chunk write was reserved for an unknown file")?;
        let remaining: usize = {
            let count: &mut usize = &mut self.in_flight[index];
            *count = count.saturating_sub(1);
            *count
        };
        if remaining > 0 {
            self.publish_progress();
            return Ok(());
        }

        if self.ranges[index].covers(self.entries[index].size) {
            self.finish_pending(file_id)?;
        } else {
            self.publish_progress();
        }
        Ok(())
    }

    fn finish_pending(&mut self, file_id: u64) -> Result<()> {
        let index: usize = self.slot(file_id).context("completed an unknown file")?;
        self.ranges[index] = ReceivedRanges::default();
        let Some(mut pending) = self.pending[index].take() else {
            return Ok(());
        };
        self.pending_count = self.pending_count.saturating_sub(1);
        path::finish_temp(
            &mut pending.file,
            &pending.temp_path,
            &pending.final_path,
            pending.entry.mode,
            pending.entry.mtime,
        )?;
        self.complete_file(file_id, pending.entry.size)
    }

    fn complete_file(&mut self, file_id: u64, size: u64) -> Result<()> {
        let index: usize = self.slot(file_id).context("completed an unknown file")?;
        if self.completed[index] {
            return Ok(());
        }
        self.completed[index] = true;
        self.files_done += 1;
        self.bytes_done += size;
        self.publish_progress();
        Ok(())
    }

    fn transfer_progress(&self) -> TransferProgress {
        TransferProgress {
            at: Instant::now(),
            bytes_done: self.bytes_done,
            bytes_total: self.total_bytes,
            files_done: self.files_done,
            files_total: self.total_files,
            read_bytes: 0,
            sent_bytes: 0,
            received_bytes: self.wire_received,
            written_bytes: self.written_bytes,
            remote_elapsed_millis: 0,
        }
    }

    fn server_progress(&self) -> ServerProgress {
        ServerProgress {
            files_done: self.files_done,
            files_total: self.total_files,
            bytes_total: self.total_bytes,
            received_bytes: self.wire_received,
            written_bytes: self.written_bytes,
            elapsed_millis: self.started.elapsed().as_millis() as u64,
        }
    }

    fn publish_progress(&self) {
        if let Some(progress) = &self.progress {
            let _ = progress.send(ProgressEvent::Receiving(self.transfer_progress()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_accept_out_of_order_and_adjacent_chunks() {
        let mut ranges = ReceivedRanges::default();
        ranges.insert(10, 5).unwrap();
        ranges.insert(0, 10).unwrap();
        assert!(ranges.covers(15));
    }

    #[test]
    fn ranges_reject_overlapping_chunks() {
        let mut ranges = ReceivedRanges::default();
        ranges.insert(0, 5).unwrap();
        assert!(ranges.insert(3, 5).is_err());
    }

    #[test]
    fn ranges_do_not_cover_gaps() {
        let mut ranges = ReceivedRanges::default();
        ranges.insert(0, 5).unwrap();
        ranges.insert(10, 5).unwrap();
        assert!(!ranges.covers(15));
        ranges.insert(5, 5).unwrap();
        assert!(ranges.covers(15));
    }

    #[test]
    fn ranges_ignore_zero_length_chunks() {
        let mut ranges = ReceivedRanges::default();
        ranges.insert(5, 0).unwrap();
        assert!(!ranges.covers(10));
        ranges.insert(0, 10).unwrap();
        assert!(ranges.covers(10));
    }

    #[test]
    fn ranges_reject_offset_overflow() {
        let mut ranges = ReceivedRanges::default();
        assert!(ranges.insert(u64::MAX, 2).is_err());
    }

    fn manifest_entry(id: u64) -> ManifestEntry {
        ManifestEntry {
            id,
            path: format!("file-{id}"),
            kind: EntryKind::File,
            size: 1,
            mode: 0o644,
            mtime: 0,
            link_target: None,
        }
    }

    #[test]
    fn manifest_requires_dense_ids_starting_at_one() {
        assert!(validate_manifest(&[manifest_entry(1)]).is_ok());
        assert!(validate_manifest(&[manifest_entry(1), manifest_entry(2)]).is_ok());
        assert!(validate_manifest(&[manifest_entry(0)]).is_err());
        assert!(validate_manifest(&[manifest_entry(2)]).is_err());
        assert!(validate_manifest(&[manifest_entry(1), manifest_entry(3)]).is_err());
        assert!(validate_manifest(&[manifest_entry(1), manifest_entry(1)]).is_err());
    }

    #[test]
    fn manifest_rejects_oversized_declarations() {
        let mut entry: ManifestEntry = manifest_entry(1);
        entry.size = cftp_proto::MAX_FILE_SIZE + 1;
        assert!(validate_manifest(&[entry]).is_err());

        let mut entry: ManifestEntry = manifest_entry(1);
        entry.path = "a".repeat(cftp_proto::MAX_MANIFEST_PATH_LEN + 1);
        assert!(validate_manifest(&[entry]).is_err());
    }

    #[test]
    fn manifest_rejects_duplicate_paths() {
        let mut second: ManifestEntry = manifest_entry(2);
        second.path = manifest_entry(1).path;
        assert!(validate_manifest(&[manifest_entry(1), second]).is_err());
    }
}
