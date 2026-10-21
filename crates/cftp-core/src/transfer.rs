use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cftp_proto::{
    Abort, ChunkHeader, Codec, Complete, DataHeader, Done, EntryKind, HardwareProfile,
    ManifestEntry, PackEntry, PackHeader, ServerProgress, TransferMode,
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
type ServerMessageResult = Result<IncomingServer, anyhow::Error>;
type ClientMessageResult = Result<IncomingClient, anyhow::Error>;
type ServerMessageSender = mpsc::UnboundedSender<ServerMessageResult>;
type ServerMessageReceiver = mpsc::UnboundedReceiver<ServerMessageResult>;
type ClientMessageSender = mpsc::UnboundedSender<ClientMessageResult>;
type ClientMessageReceiver = mpsc::UnboundedReceiver<ClientMessageResult>;

use crate::wire::{
    HelloInfo, IncomingClient, IncomingServer, ServerHelloInfo, WireVersion, negotiated_version,
    read_client_hello, read_client_message, read_server_hello, read_server_message,
    write_client_hello, write_complete, write_profile_changed, write_server_done,
    write_server_error, write_server_hello, write_server_progress, write_set_profile,
};

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

#[derive(Debug, Clone, Default)]
pub struct PushStats {
    pub files: u64,
    pub bytes: u64,
    pub packs: u64,
    pub chunks: u64,
    pub effective_profile: HardwareProfile,
}

pub async fn push(
    src: &Path,
    addr: SocketAddr,
    auth: &ClientAuth,
    opts: PushOptions,
) -> Result<PushStats> {
    validate_tuning(&opts.tuning)?;

    let scan: scan::ScanResult = scan::scan_source(src)?;
    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Scan {
            files: scan.total_files,
            bytes: scan.total_bytes,
        });
    }

    let bind: SocketAddr = if addr.is_ipv6() {
        "[::]:0".parse().expect("valid ipv6 bind address")
    } else {
        "0.0.0.0:0".parse().expect("valid ipv4 bind address")
    };
    let mut endpoint: Endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(auth.client_config(&opts.tuning, opts.force_v2)?);
    let connection: Connection = match endpoint
        .connect(addr, &opts.server_name)
        .with_context(|| format!("failed to start connection to {addr}"))?
        .await
    {
        Ok(connection) => connection,
        Err(err) => {
            if let Some(unknown) = auth.take_unknown_host_key() {
                return Err(anyhow::Error::new(unknown));
            }
            return Err(err).with_context(|| format!("failed to connect to {addr}"));
        }
    };

    let wire: WireVersion = if opts.force_v2 {
        WireVersion::V2
    } else {
        negotiated_version(&connection)
    };

    let (mut control_send, mut control_recv): (SendStream, RecvStream) =
        connection.open_bi().await?;
    write_client_hello(
        &mut control_send,
        wire,
        &HelloInfo {
            version: if wire == WireVersion::V3 {
                cftp_proto::PROTOCOL_VERSION_V3
            } else {
                cftp_proto::PROTOCOL_VERSION_V2
            },
            mode: TransferMode::Push,
            destination: opts.destination.clone(),
            entries: scan.entries.clone(),
            profile: opts.profile,
        },
    )
    .await?;

    let server_hello: ServerHelloInfo = read_server_hello(&mut control_recv, wire).await?;
    if !server_hello.accepted {
        bail!("server rejected transfer: {}", server_hello.message);
    }

    let mut effective_profile: HardwareProfile = if wire == WireVersion::V3 {
        server_hello.effective_profile
    } else {
        HardwareProfile::Default
    };
    let server_cap: HardwareProfile = if wire == WireVersion::V3 {
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

    if wire == WireVersion::V3
        && (!server_hello.profile_accepted || effective_profile != opts.profile)
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

    let (server_tx, mut server_rx): (ServerMessageSender, ServerMessageReceiver) =
        mpsc::unbounded_channel::<ServerMessageResult>();
    let control_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        loop {
            match read_server_message(&mut control_recv, wire).await {
                Ok(message) => {
                    if server_tx.send(Ok(message)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let _ = server_tx.send(Err(err));
                    break;
                }
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
    let mut pending_request: Option<u64> = None;
    let mut next_request_id: u64 = 1u64;

    let mut optimizer: Option<NetworkOptimizer> =
        (wire == WireVersion::V3 && effective_profile == HardwareProfile::Aggressive).then(|| {
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
                        if let IncomingServer::ProfileChanged {
                            request_id,
                            accepted,
                            effective_profile: new_profile,
                            server_cap: new_cap,
                            message,
                        } = message
                        {
                            if pending_request.take() == Some(request_id) {
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
                                optimizer = (wire == WireVersion::V3
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
                            write_set_profile(&mut control_send, request_id, profile).await?;
                            pending_request = Some(request_id);
                            notice(&opts, format!("requesting {} profile", profile));
                        }
                    }
                    None => has_hotkeys = false,
                }
            }
            _ = optimizer_tick.tick(), if adaptive => {
                if wire == WireVersion::V3 && effective_profile == HardwareProfile::Aggressive {
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
        adaptive = wire == WireVersion::V3 && effective_profile == HardwareProfile::Aggressive;
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

    write_complete(
        &mut control_send,
        wire,
        Complete {
            files: stats.files,
            bytes: stats.bytes,
        },
    )
    .await?;

    let done: Done = loop {
        match server_rx.recv().await {
            Some(Ok(message)) => {
                if let Some(done) = apply_server_message(message, &mut state, &opts, totals)? {
                    break done;
                }
            }
            Some(Err(err)) => return Err(err).context("control stream failed"),
            None => bail!("server closed control stream before completion"),
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
            let mut stream = connection.open_uni().await?;
            cftp_proto::write_frame(&mut stream, &DataHeader::Pack(header)).await?;
            stream.write_all(&payload).await?;
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
            let mut stream = connection.open_uni().await?;
            cftp_proto::write_frame(&mut stream, &DataHeader::Chunk(header)).await?;
            stream.write_all(&payload).await?;
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

#[derive(Debug)]
pub struct ServeOptions {
    pub root: PathBuf,
    pub listen: SocketAddr,
    pub progress: Option<ProgressSender>,
    pub on_ready: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
    pub max_profile: HardwareProfile,
    pub force_v2: bool,
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
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ServeStats {
    pub files: u64,
    pub bytes: u64,
    pub peer: Option<SocketAddr>,
    pub effective_profile: HardwareProfile,
}

pub async fn serve_once(auth: &ServerAuth, opts: ServeOptions) -> Result<ServeStats> {
    let root: PathBuf = opts.root.clone();
    let meta: fs::Metadata = fs::metadata(&root)
        .with_context(|| format!("failed to inspect root {}", root.display()))?;
    if !meta.is_dir() {
        bail!("root is not a directory: {}", root.display());
    }

    let current_profile: HardwareProfile = opts.max_profile.min(HardwareProfile::Default);
    let initial_tuning: Tuning = Tuning::for_profile(current_profile, None);
    let endpoint: Endpoint = Endpoint::server(
        auth.server_config(&initial_tuning, opts.force_v2)?,
        opts.listen,
    )?;
    let local_addr: SocketAddr = endpoint.local_addr()?;
    if let Some(on_ready) = opts.on_ready {
        let _ = on_ready.send(local_addr);
    }
    let incoming: quinn::Incoming = endpoint
        .accept()
        .await
        .context("listener closed before a client connected")?;
    let connection: Connection = incoming.await.context("failed to establish connection")?;
    let peer: SocketAddr = connection.remote_address();
    let wire: WireVersion = if opts.force_v2 {
        WireVersion::V2
    } else {
        negotiated_version(&connection)
    };

    let (control_send, control_recv): (SendStream, RecvStream) = connection
        .accept_bi()
        .await
        .context("client did not open a control stream")?;

    let connection_for_error: Connection = connection.clone();
    let result: Result<ServeStats> = receive_connection(
        connection,
        control_send,
        control_recv,
        root,
        opts.progress,
        peer,
        wire,
        opts.max_profile,
        current_profile,
    )
    .await;

    if result.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(1), connection_for_error.closed()).await;
    }

    endpoint.wait_idle().await;
    endpoint.close(0u32.into(), b"done");
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
) -> Result<ServeStats> {
    let hello: HelloInfo = match read_client_hello(&mut control_recv, wire).await {
        Ok(hello) => hello,
        Err(err) => {
            let _ = write_server_error(
                &mut control_send,
                wire,
                Abort {
                    message: err.to_string(),
                },
            )
            .await;
            let _ = control_send.finish();
            return Err(err);
        }
    };

    if hello.mode != TransferMode::Push {
        let err: anyhow::Error = anyhow::anyhow!("unsupported transfer mode");
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }

    let mut current_profile: HardwareProfile = initial_profile;
    let mut effective_profile: HardwareProfile = HardwareProfile::Default;
    let mut profile_accepted: bool = true;
    let mut profile_message: String = String::new();
    if wire == WireVersion::V3 {
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

    let entries: HashMap<u64, ManifestEntry> = match validate_manifest(&hello.entries) {
        Ok(entries) => entries,
        Err(err) => {
            send_server_error(&mut control_send, wire, &err).await;
            return Err(err);
        }
    };
    let mut rel_paths: HashMap<u64, PathBuf> = HashMap::new();
    for entry in entries.values() {
        let relative: PathBuf = match path::validate_relative(&entry.path) {
            Ok(relative) => relative,
            Err(err) => {
                send_server_error(&mut control_send, wire, &err).await;
                return Err(err);
            }
        };
        rel_paths.insert(entry.id, destination.join(relative));
    }

    let total_files: u64 = entries
        .values()
        .filter(|entry| entry.kind == EntryKind::File)
        .count() as u64;
    let total_bytes: u64 = entries
        .values()
        .filter(|entry| entry.kind == EntryKind::File)
        .map(|entry| entry.size)
        .sum();

    if let Err(err) = create_manifest_entries(&root, &destination, &entries, &rel_paths) {
        send_server_error(&mut control_send, wire, &err).await;
        return Err(err);
    }

    write_server_hello(
        &mut control_send,
        wire,
        &ServerHelloInfo {
            version: if wire == WireVersion::V3 {
                cftp_proto::PROTOCOL_VERSION_V3
            } else {
                cftp_proto::PROTOCOL_VERSION_V2
            },
            accepted: true,
            message: "ready".to_string(),
            effective_profile,
            server_cap: if wire == WireVersion::V3 {
                max_profile
            } else {
                HardwareProfile::Default
            },
            profile_accepted,
        },
    )
    .await?;
    if !profile_message.is_empty() {
        tracing::debug!("{profile_message}");
    }

    let state: Arc<tokio::sync::Mutex<ReceiverState>> =
        Arc::new(tokio::sync::Mutex::new(ReceiverState {
            root,
            entries,
            rel_paths,
            ranges: HashMap::new(),
            completed: HashSet::new(),
            pending: HashMap::new(),
            total_files,
            total_bytes,
            files_done: 0,
            bytes_done: 0,
            wire_received: 0,
            written_bytes: 0,
            started: Instant::now(),
            progress,
        }));
    let mut last_progress: Instant = Instant::now();

    let (control_tx, mut control_rx): (ClientMessageSender, ClientMessageReceiver) =
        mpsc::unbounded_channel();
    let control_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        loop {
            match read_client_message(&mut control_recv, wire).await {
                Ok(message) => {
                    if control_tx.send(Ok(message)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let _ = control_tx.send(Err(err));
                    break;
                }
            }
        }
    });

    let mut complete: bool = false;
    let mut streams: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
    let result: Result<()> = loop {
        if complete && streams.is_empty() {
            let all_complete: bool = { state.lock().await.all_complete() };
            if all_complete {
                break Ok(());
            }
            match tokio::time::timeout(Duration::from_secs(30), connection.accept_uni()).await {
                Ok(Ok(stream)) => {
                    spawn_receive_task(&mut streams, state.clone(), stream);
                }
                Ok(Err(err)) => {
                    break Err(err).context("failed to accept data stream");
                }
                Err(_) => {
                    break Err(anyhow::anyhow!("timed out waiting for missing file data"));
                }
            }
            continue;
        }

        tokio::select! {
            message = control_rx.recv() => {
                match message {
                    Some(Ok(IncomingClient::Complete(_))) => {
                        complete = true;
                    }
                    Some(Ok(IncomingClient::Abort(abort))) => {
                        break Err(anyhow::anyhow!("client aborted: {}", abort.message));
                    }
                    Some(Ok(IncomingClient::SetProfile { request_id, profile })) => {
                        if wire == WireVersion::V3 {
                            if profile <= max_profile {
                                current_profile = profile;
                                effective_profile = profile;
                                server_tuning = Tuning::for_profile(effective_profile, None);
                                apply_connection_tuning(&connection, &server_tuning);
                                write_profile_changed(
                                    &mut control_send,
                                    request_id,
                                    true,
                                    effective_profile,
                                    max_profile,
                                    String::new(),
                                )
                                .await?;
                            } else {
                                effective_profile = current_profile;
                                write_profile_changed(
                                    &mut control_send,
                                    request_id,
                                    false,
                                    effective_profile,
                                    max_profile,
                                    format!(
                                        "requested profile {} exceeds server cap {}",
                                        profile, max_profile
                                    ),
                                )
                                .await?;
                            }
                        }
                    }
                    Some(Ok(IncomingClient::Hello(_))) => {
                        break Err(anyhow::anyhow!("client sent a second hello"));
                    }
                    Some(Err(err)) => {
                        break Err(err).context("control stream failed");
                    }
                    None => {
                        break Err(anyhow::anyhow!("control stream closed before completion"));
                    }
                }
            }
            result = streams.join_next(), if !streams.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {
                        if let Err(err) =
                            maybe_send_progress(&mut control_send, wire, &state, &mut last_progress).await
                        {
                            break Err(err);
                        }
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
                        spawn_receive_task(&mut streams, state.clone(), stream);
                    }
                    Err(err) => {
                        break Err(err).context("failed to accept data stream");
                    }
                }
            }
        }
    };
    control_task.abort();

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
    write_server_progress(&mut control_send, wire, server_progress).await?;
    write_server_done(&mut control_send, wire, done.clone()).await?;
    control_send.finish()?;
    // Keep the connection alive long enough for the client to read the final
    // message. Dropping the last local handle closes the connection.
    let _ = tokio::time::timeout(Duration::from_secs(3), connection.closed()).await;
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

async fn send_server_error(control_send: &mut SendStream, wire: WireVersion, err: &anyhow::Error) {
    let _ = write_server_error(
        control_send,
        wire,
        Abort {
            message: err.to_string(),
        },
    )
    .await;
    let _ = control_send.finish();
}

fn create_manifest_entries(
    root: &Path,
    destination: &Path,
    entries: &HashMap<u64, ManifestEntry>,
    rel_paths: &HashMap<u64, PathBuf>,
) -> Result<()> {
    if !destination.as_os_str().is_empty() {
        path::create_directory(root, destination, 0o755, 0)?;
    }

    for entry in entries.values() {
        let Some(relative) = rel_paths.get(&entry.id) else {
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

async fn maybe_send_progress(
    control_send: &mut SendStream,
    wire: WireVersion,
    state: &Arc<tokio::sync::Mutex<ReceiverState>>,
    last: &mut Instant,
) -> Result<()> {
    if last.elapsed() < Duration::from_millis(250) {
        return Ok(());
    }
    *last = Instant::now();
    let progress = { state.lock().await.server_progress() };
    write_server_progress(control_send, wire, progress).await?;
    Ok(())
}

fn spawn_receive_task(
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    state: Arc<tokio::sync::Mutex<ReceiverState>>,
    stream: RecvStream,
) {
    tasks.spawn(async move {
        let decoded = decode_stream(stream).await?;
        let mut state = state.lock().await;
        state.apply(decoded)?;
        Ok(())
    });
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

async fn decode_stream(mut stream: RecvStream) -> Result<DecodedData> {
    let header: DataHeader = cftp_proto::read_frame(&mut stream)
        .await
        .context("failed to read data header")?;

    match header {
        DataHeader::Pack(pack) => {
            if pack.compressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("pack payload too large");
            }
            if pack.uncompressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("pack expands beyond the maximum payload size");
            }
            let mut encoded = vec![0u8; pack.compressed_len as usize];
            stream.read_exact(&mut encoded).await?;
            let (pack, raw, entries) = tokio::task::spawn_blocking(move || {
                let raw = codec::decompress(pack.codec, &encoded, pack.uncompressed_len as usize)?;
                let mut offset = 0usize;
                let mut entries = Vec::with_capacity(pack.entries.len());
                for entry in &pack.entries {
                    let end = offset
                        .checked_add(entry.size as usize)
                        .context("pack size overflow")?;
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
            if chunk.compressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("chunk payload too large");
            }
            if chunk.uncompressed_len as usize > cftp_proto::MAX_DATA_PAYLOAD {
                bail!("chunk expands beyond the maximum payload size");
            }
            let mut encoded = vec![0u8; chunk.compressed_len as usize];
            stream.read_exact(&mut encoded).await?;
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

fn validate_manifest(entries: &[ManifestEntry]) -> Result<HashMap<u64, ManifestEntry>> {
    let mut map = HashMap::new();
    let mut paths = HashSet::new();
    for entry in entries {
        if entry.id == 0 {
            bail!("manifest entry has an invalid id");
        }
        if map.insert(entry.id, entry.clone()).is_some() {
            bail!("duplicate manifest id {}", entry.id);
        }
        let relative = path::validate_relative(&entry.path)?;
        let normalized = relative.to_string_lossy().replace('\\', "/");
        if !paths.insert(normalized) {
            bail!("duplicate manifest path {}", entry.path);
        }
        if entry.kind == EntryKind::Symlink && entry.link_target.is_none() {
            bail!("symlink {} has no target", entry.path);
        }
    }
    Ok(map)
}

struct PendingFile {
    file: File,
    temp_path: PathBuf,
    final_path: PathBuf,
    entry: ManifestEntry,
}

#[derive(Debug, Default)]
struct ReceivedRanges {
    ranges: Vec<(u64, u64)>,
}

impl ReceivedRanges {
    fn insert(&mut self, start: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let end = start.checked_add(len).context("chunk range overflow")?;
        for &(existing_start, existing_end) in &self.ranges {
            if start < existing_end && end > existing_start {
                bail!("overlapping chunk at offset {start}");
            }
        }
        self.ranges.push((start, end));
        self.ranges.sort_unstable();

        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.ranges.len());
        for &(start, end) in &self.ranges {
            if let Some(last) = merged.last_mut()
                && start == last.1
            {
                last.1 = end;
                continue;
            }
            merged.push((start, end));
        }
        self.ranges = merged;
        Ok(())
    }

    fn covers(&self, size: u64) -> bool {
        size == 0 || self.ranges.as_slice() == [(0, size)]
    }
}

struct ReceiverState {
    root: PathBuf,
    entries: HashMap<u64, ManifestEntry>,
    rel_paths: HashMap<u64, PathBuf>,
    ranges: HashMap<u64, ReceivedRanges>,
    completed: HashSet<u64>,
    pending: HashMap<u64, PendingFile>,
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
    fn all_complete(&self) -> bool {
        self.completed.len() as u64 == self.total_files
    }

    fn apply(&mut self, data: DecodedData) -> Result<()> {
        match data {
            DecodedData::Pack {
                header,
                raw,
                entries,
            } => self.apply_pack(header, raw, entries),
            DecodedData::Chunk { header, raw } => self.apply_chunk(header, raw),
        }
    }

    fn apply_pack(
        &mut self,
        pack: PackHeader,
        raw: Vec<u8>,
        entries: Vec<(u64, Range<usize>)>,
    ) -> Result<()> {
        self.wire_received = self.wire_received.saturating_add(pack.compressed_len);

        for (file_id, range) in entries {
            let (file_size, file_mode, file_mtime, entry_path, relative) = {
                let entry = self
                    .entries
                    .get(&file_id)
                    .with_context(|| format!("unknown file id {file_id}"))?;
                if entry.kind != EntryKind::File {
                    bail!("pack entry {} is not a file", entry.path);
                }
                if self.completed.contains(&file_id) {
                    bail!("received duplicate data for {}", entry.path);
                }
                let relative = self
                    .rel_paths
                    .get(&file_id)
                    .with_context(|| format!("missing path for {}", entry.path))?
                    .clone();
                (
                    entry.size,
                    entry.mode,
                    entry.mtime,
                    entry.path.clone(),
                    relative,
                )
            };

            let contents = raw
                .get(range.clone())
                .with_context(|| format!("pack entry {entry_path} exceeds the payload"))?;
            if contents.len() as u64 != file_size {
                bail!("pack entry {entry_path} has the wrong size");
            }
            path::write_atomic(&self.root, &relative, contents, file_mode, file_mtime)?;
            self.written_bytes = self.written_bytes.saturating_add(file_size);
            self.complete_file(file_id, file_size)?;
        }
        Ok(())
    }

    fn apply_chunk(&mut self, chunk: ChunkHeader, raw: Vec<u8>) -> Result<()> {
        self.wire_received = self
            .wire_received
            .saturating_add(chunk.compressed_len as u64);

        let entry = self
            .entries
            .get(&chunk.file_id)
            .with_context(|| format!("unknown file id {}", chunk.file_id))?
            .clone();
        if entry.kind != EntryKind::File {
            bail!("chunk entry {} is not a file", entry.path);
        }
        if self.completed.contains(&entry.id) {
            bail!("received duplicate chunk for {}", entry.path);
        }
        if chunk.offset.saturating_add(chunk.uncompressed_len as u64) > entry.size {
            bail!("chunk for {} exceeds file size", entry.path);
        }
        if raw.len() != chunk.uncompressed_len as usize {
            bail!("chunk for {} has the wrong size", entry.path);
        }

        if !self.pending.contains_key(&entry.id) {
            let relative = self
                .rel_paths
                .get(&entry.id)
                .with_context(|| format!("missing path for {}", entry.path))?
                .clone();
            let (file, temp_path, final_path) = path::create_temp_file(&self.root, &relative)?;
            file.set_len(entry.size)?;
            self.pending.insert(
                entry.id,
                PendingFile {
                    file,
                    temp_path,
                    final_path,
                    entry: entry.clone(),
                },
            );
        }

        {
            let pending = self
                .pending
                .get_mut(&entry.id)
                .context("pending file disappeared")?;
            pending.file.seek(SeekFrom::Start(chunk.offset))?;
            pending.file.write_all(&raw)?;
        }
        self.written_bytes = self.written_bytes.saturating_add(raw.len() as u64);

        let complete = {
            let ranges = self.ranges.entry(entry.id).or_default();
            ranges.insert(chunk.offset, chunk.uncompressed_len as u64)?;
            ranges.covers(entry.size)
        };
        if complete {
            self.ranges.remove(&entry.id);
            self.finish_pending(entry.id)?;
        } else {
            self.publish_progress();
        }
        Ok(())
    }

    fn finish_pending(&mut self, file_id: u64) -> Result<()> {
        self.ranges.remove(&file_id);
        let Some(mut pending) = self.pending.remove(&file_id) else {
            return Ok(());
        };
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
        if !self.completed.insert(file_id) {
            return Ok(());
        }
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
}
