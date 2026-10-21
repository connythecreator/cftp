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
    Abort, ChunkHeader, ClientHello, ClientMessage, Codec, Complete, DataHeader, Done, EntryKind,
    ManifestEntry, PROTOCOL_VERSION, PackEntry, PackHeader, ServerHello, ServerMessage,
    ServerProgress, TransferMode,
};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tokio::io::AsyncReadExt;
use tokio::sync::{Semaphore, mpsc};

use crate::auth::{ClientAuth, ServerAuth};
use crate::codec::{self, CHUNK_LEVEL, PACK_LEVEL};
use crate::identity::DEFAULT_SERVER_NAME;
use crate::path;
use crate::progress::{ProgressEvent, ProgressSender, TransferProgress};
use crate::scan;

#[derive(Debug, Clone)]
pub struct PushOptions {
    pub destination: String,
    pub server_name: String,
    pub progress: Option<ProgressSender>,
    pub chunk_size: usize,
    pub pack_target: usize,
    pub pack_max_files: usize,
    pub small_file_max: u64,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            destination: String::new(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            progress: None,
            chunk_size: 1024 * 1024,
            pack_target: 4 * 1024 * 1024,
            pack_max_files: 128,
            small_file_max: 2 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PushStats {
    pub files: u64,
    pub bytes: u64,
    pub packs: u64,
    pub chunks: u64,
}

pub async fn push(
    src: &Path,
    addr: SocketAddr,
    auth: &ClientAuth,
    opts: PushOptions,
) -> Result<PushStats> {
    if opts.chunk_size == 0 || opts.chunk_size > cftp_proto::MAX_DATA_PAYLOAD {
        bail!(
            "chunk size must be between 1 and {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if opts.pack_target == 0 || opts.pack_target > cftp_proto::MAX_DATA_PAYLOAD {
        bail!(
            "pack target must be between 1 and {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if opts.small_file_max > cftp_proto::MAX_DATA_PAYLOAD as u64 {
        bail!(
            "small-file threshold cannot exceed {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if opts.pack_max_files == 0 {
        bail!("pack max files must be greater than zero");
    }

    let scan = scan::scan_source(src)?;
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
    let mut endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(auth.client_config()?);
    let connection = match endpoint
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

    let (mut control_send, mut control_recv) = connection.open_bi().await?;
    let hello = ClientMessage::Hello(ClientHello {
        version: PROTOCOL_VERSION,
        mode: TransferMode::Push,
        destination: opts.destination.clone(),
        entries: scan.entries.clone(),
    });
    cftp_proto::write_frame(&mut control_send, &hello).await?;

    let response: ServerMessage = cftp_proto::read_frame(&mut control_recv).await?;
    match response {
        ServerMessage::Hello(hello) => {
            if !hello.accepted {
                bail!("server rejected transfer: {}", hello.message);
            }
        }
        ServerMessage::Error(err) => bail!("server error: {}", err.message),
        ServerMessage::Progress(_) => bail!("server sent progress before accepting the transfer"),
        ServerMessage::Done(_) => bail!("server completed before receiving data"),
    }

    let (server_tx, mut server_rx) = mpsc::unbounded_channel::<Result<ServerMessage, _>>();
    let control_task = tokio::spawn(async move {
        loop {
            match cftp_proto::read_frame::<_, ServerMessage>(&mut control_recv).await {
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

    let totals = Totals {
        files: scan.total_files,
        bytes: scan.total_bytes,
    };
    let mut stats = PushStats::default();
    let mut state = SendState::default();

    let (tx, mut rx) = mpsc::channel::<Result<EncodedItem>>(MAX_INFLIGHT_SENDS);
    let producer = tokio::spawn(produce_items(scan.clone(), opts.clone(), tx));
    let mut sends = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            message = server_rx.recv() => {
                match message {
                    Some(Ok(message)) => {
                        if apply_server_message(message, &mut state, &opts, totals)?.is_some() {
                            bail!("server completed before receiving data");
                        }
                    }
                    Some(Err(err)) => return Err(err).context("control stream failed"),
                    None => bail!("server closed control stream before completion"),
                }
            }
            result = sends.join_next(), if !sends.is_empty() => {
                let result = result.expect("send set is not empty");
                apply_sent_item(
                    result.context("send task panicked")??,
                    &mut stats,
                    &mut state,
                    &opts,
                    totals,
                );
                drain_server_messages(&mut server_rx, &mut state, &opts, totals)?;
            }
            item = rx.recv() => {
                let Some(item) = item else {
                    break;
                };
                let item = item?;
                while sends.len() >= MAX_INFLIGHT_SENDS {
                    let result = sends.join_next().await.expect("send set is not empty");
                    apply_sent_item(
                        result.context("send task panicked")??,
                        &mut stats,
                        &mut state,
                        &opts,
                        totals,
                    );
                    drain_server_messages(&mut server_rx, &mut state, &opts, totals)?;
                }
                let connection = connection.clone();
                sends.spawn(async move { send_item(&connection, item).await });
            }
        }
    }
    while let Some(result) = sends.join_next().await {
        apply_sent_item(
            result.context("send task panicked")??,
            &mut stats,
            &mut state,
            &opts,
            totals,
        );
        drain_server_messages(&mut server_rx, &mut state, &opts, totals)?;
    }
    producer.await.context("producer task panicked")?;

    cftp_proto::write_frame(
        &mut control_send,
        &ClientMessage::Complete(Complete {
            files: stats.files,
            bytes: stats.bytes,
        }),
    )
    .await?;

    let done = loop {
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

    if let Some(progress) = &opts.progress {
        let _ = progress.send(ProgressEvent::Complete {
            files: stats.files,
            bytes: stats.bytes,
        });
    }

    control_send.finish()?;
    Ok(stats)
}

const MAX_INFLIGHT_SENDS: usize = 32;
const MAX_INFLIGHT_STREAMS: usize = 32;

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
    message: ServerMessage,
    state: &mut SendState,
    opts: &PushOptions,
    totals: Totals,
) -> Result<Option<Done>> {
    match message {
        ServerMessage::Progress(progress) => {
            state.remote_received = progress.received_bytes;
            state.remote_written = progress.written_bytes;
            state.remote_elapsed_millis = progress.elapsed_millis;
            publish_push_progress(opts, state, totals);
            Ok(None)
        }
        ServerMessage::Done(done) => Ok(Some(done)),
        ServerMessage::Error(err) => bail!("server error: {}", err.message),
        ServerMessage::Hello(_) => bail!("unexpected server hello"),
    }
}

fn drain_server_messages(
    rx: &mut mpsc::UnboundedReceiver<Result<ServerMessage, cftp_proto::ProtoError>>,
    state: &mut SendState,
    opts: &PushOptions,
    totals: Totals,
) -> Result<()> {
    while let Ok(message) = rx.try_recv() {
        match message {
            Ok(message) => {
                if apply_server_message(message, state, opts, totals)?.is_some() {
                    bail!("server completed before receiving data");
                }
            }
            Err(err) => return Err(err).context("control stream failed"),
        }
    }
    Ok(())
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
    opts: PushOptions,
    tx: mpsc::Sender<Result<EncodedItem>>,
) {
    if let Err(err) = produce_items_inner(scan, opts, &tx).await {
        let _ = tx.send(Err(err)).await;
    }
}

async fn produce_items_inner(
    scan: scan::ScanResult,
    opts: PushOptions,
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    let parallelism = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    let permits = Arc::new(Semaphore::new(parallelism));
    let mut tasks = tokio::task::JoinSet::new();
    let mut pack_entries = Vec::new();
    let mut pack_bytes = 0u64;
    let mut pack_id = 0u64;

    for entry in scan
        .entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
    {
        if entry.size <= opts.small_file_max {
            pack_bytes = pack_bytes.saturating_add(entry.size);
            pack_entries.push(entry.clone());
            if pack_bytes >= opts.pack_target as u64 || pack_entries.len() >= opts.pack_max_files {
                enqueue_pack(
                    &mut tasks,
                    &permits,
                    &scan.base,
                    &mut pack_entries,
                    &mut pack_id,
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
                    tx,
                )
                .await?;
                pack_bytes = 0;
            }
            enqueue_large(&mut tasks, &permits, &scan.base, entry, &opts, tx).await?;
        }
        drain_encoding_tasks(&mut tasks)?;
    }

    if !pack_entries.is_empty() {
        enqueue_pack(
            &mut tasks,
            &permits,
            &scan.base,
            &mut pack_entries,
            &mut pack_id,
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
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let entries = std::mem::take(entries);
    let base = base.to_path_buf();
    let id = *pack_id;
    *pack_id += 1;
    let permit = permits
        .clone()
        .acquire_owned()
        .await
        .context("encoding semaphore closed")?;
    let tx = tx.clone();
    tasks.spawn(async move {
        let _permit = permit;
        let built = tokio::task::spawn_blocking(move || build_pack(base, entries, id)).await??;
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

fn build_pack(base: PathBuf, entries: Vec<ManifestEntry>, pack_id: u64) -> Result<BuiltPack> {
    let mut pack_entries = Vec::with_capacity(entries.len());
    let mut payload = Vec::new();
    let mut compressible = false;
    let mut logical_bytes = 0u64;

    for entry in &entries {
        let full_path = base.join(Path::new(&entry.path));
        let data = fs::read(&full_path)
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

    let (chunk_codec, encoded) = if compressible {
        codec::compress(&payload, PACK_LEVEL, true)?
    } else {
        (Codec::Raw, payload)
    };
    let header = PackHeader {
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
    opts: &PushOptions,
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    let full_path = base.join(Path::new(&entry.path));
    let mut file = tokio::fs::File::open(&full_path)
        .await
        .with_context(|| format!("failed to open {}", full_path.display()))?;
    let mut buffer = vec![0u8; opts.chunk_size.max(1)];
    let mut offset = 0u64;
    let allow_skip = !codec::extension_is_incompressible(&entry.path);

    loop {
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .context("encoding semaphore closed")?;
        let read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("failed to read {}", full_path.display()))?;
        if read == 0 {
            break;
        }
        let data = buffer[..read].to_vec();
        let done = offset + read as u64 == entry.size;
        let file_id = entry.id;
        let tx = tx.clone();
        let chunk_offset = offset;
        let chunk_len = read;
        tasks.spawn(async move {
            let _permit = permit;
            let (hash, chunk_codec, encoded) = tokio::task::spawn_blocking(move || {
                let hash = codec::hash(&data);
                let (chunk_codec, encoded) = if allow_skip {
                    codec::compress(&data, CHUNK_LEVEL, true)?
                } else {
                    (Codec::Raw, data)
                };
                Ok::<_, anyhow::Error>((hash, chunk_codec, encoded))
            })
            .await??;
            let header = ChunkHeader {
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
}

impl ServeOptions {
    pub fn new(root: impl Into<PathBuf>, listen: SocketAddr) -> Self {
        Self {
            root: root.into(),
            listen,
            progress: None,
            on_ready: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ServeStats {
    pub files: u64,
    pub bytes: u64,
    pub peer: Option<SocketAddr>,
}

pub async fn serve_once(auth: &ServerAuth, opts: ServeOptions) -> Result<ServeStats> {
    let root = opts.root.clone();
    let meta = fs::metadata(&root)
        .with_context(|| format!("failed to inspect root {}", root.display()))?;
    if !meta.is_dir() {
        bail!("root is not a directory: {}", root.display());
    }

    let endpoint = Endpoint::server(auth.server_config()?, opts.listen)?;
    let local_addr = endpoint.local_addr()?;
    if let Some(on_ready) = opts.on_ready {
        let _ = on_ready.send(local_addr);
    }
    let incoming = endpoint
        .accept()
        .await
        .context("listener closed before a client connected")?;
    let connection = incoming.await.context("failed to establish connection")?;
    let peer = connection.remote_address();

    let (control_send, control_recv) = connection
        .accept_bi()
        .await
        .context("client did not open a control stream")?;

    let connection_for_error = connection.clone();
    let result = receive_connection(
        connection,
        control_send,
        control_recv,
        root,
        opts.progress,
        peer,
    )
    .await;

    if result.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(1), connection_for_error.closed()).await;
    }

    endpoint.wait_idle().await;
    endpoint.close(0u32.into(), b"done");
    result
}

async fn receive_connection(
    connection: Connection,
    mut control_send: SendStream,
    mut control_recv: RecvStream,
    root: PathBuf,
    progress: Option<ProgressSender>,
    peer: SocketAddr,
) -> Result<ServeStats> {
    let first: ClientMessage = cftp_proto::read_frame(&mut control_recv)
        .await
        .context("failed to read client hello")?;

    let hello = match first {
        ClientMessage::Hello(hello) => hello,
        ClientMessage::Abort(abort) => bail!("client aborted: {}", abort.message),
        ClientMessage::Complete(_) => bail!("client completed before sending a manifest"),
    };

    if hello.version != PROTOCOL_VERSION {
        let message = format!(
            "protocol version mismatch: client {}, server {}",
            hello.version, PROTOCOL_VERSION
        );
        let _ = cftp_proto::write_frame(
            &mut control_send,
            &ServerMessage::Hello(ServerHello {
                version: PROTOCOL_VERSION,
                accepted: false,
                message: message.clone(),
            }),
        )
        .await;
        let _ = control_send.finish();
        bail!("{message}");
    }

    if hello.mode != TransferMode::Push {
        let err = anyhow::anyhow!("unsupported transfer mode");
        send_server_error(&mut control_send, &err).await;
        return Err(err);
    }

    let destination = match path::validate_destination(&hello.destination) {
        Ok(destination) => destination,
        Err(err) => {
            send_server_error(&mut control_send, &err).await;
            return Err(err);
        }
    };

    let entries = match validate_manifest(&hello.entries) {
        Ok(entries) => entries,
        Err(err) => {
            send_server_error(&mut control_send, &err).await;
            return Err(err);
        }
    };
    let mut rel_paths = HashMap::new();
    for entry in entries.values() {
        let relative = match path::validate_relative(&entry.path) {
            Ok(relative) => relative,
            Err(err) => {
                send_server_error(&mut control_send, &err).await;
                return Err(err);
            }
        };
        rel_paths.insert(entry.id, destination.join(relative));
    }

    let total_files = entries
        .values()
        .filter(|entry| entry.kind == EntryKind::File)
        .count() as u64;
    let total_bytes: u64 = entries
        .values()
        .filter(|entry| entry.kind == EntryKind::File)
        .map(|entry| entry.size)
        .sum();

    if let Err(err) = create_manifest_entries(&root, &destination, &entries, &rel_paths) {
        send_server_error(&mut control_send, &err).await;
        return Err(err);
    }

    cftp_proto::write_frame(
        &mut control_send,
        &ServerMessage::Hello(ServerHello {
            version: PROTOCOL_VERSION,
            accepted: true,
            message: "ready".to_string(),
        }),
    )
    .await?;

    let state = Arc::new(tokio::sync::Mutex::new(ReceiverState {
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
    let mut last_progress = Instant::now();

    let (control_tx, mut control_rx) = mpsc::unbounded_channel();
    let control_task = tokio::spawn(async move {
        while let Ok(message) = cftp_proto::read_frame::<_, ClientMessage>(&mut control_recv).await
        {
            if control_tx.send(message).is_err() {
                break;
            }
        }
    });

    let mut complete = false;
    let mut streams = tokio::task::JoinSet::new();
    let result = loop {
        if complete && streams.is_empty() {
            let all_complete = { state.lock().await.all_complete() };
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
                    Some(ClientMessage::Complete(_)) => {
                        complete = true;
                    }
                    Some(ClientMessage::Abort(abort)) => {
                        break Err(anyhow::anyhow!("client aborted: {}", abort.message));
                    }
                    Some(ClientMessage::Hello(_)) => {
                        break Err(anyhow::anyhow!("client sent a second hello"));
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
                            maybe_send_progress(&mut control_send, &state, &mut last_progress).await
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
            stream = connection.accept_uni(), if streams.len() < MAX_INFLIGHT_STREAMS => {
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
        send_server_error(&mut control_send, &err).await;
        return Err(err);
    }

    let (done, server_progress) = {
        let state = state.lock().await;
        (
            Done {
                files: state.files_done,
                bytes: state.bytes_done,
            },
            state.server_progress(),
        )
    };
    cftp_proto::write_frame(&mut control_send, &ServerMessage::Progress(server_progress)).await?;
    cftp_proto::write_frame(&mut control_send, &ServerMessage::Done(done.clone())).await?;
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
    })
}

async fn send_server_error(control_send: &mut SendStream, err: &anyhow::Error) {
    let _ = cftp_proto::write_frame(
        control_send,
        &ServerMessage::Error(Abort {
            message: err.to_string(),
        }),
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
    state: &Arc<tokio::sync::Mutex<ReceiverState>>,
    last: &mut Instant,
) -> Result<()> {
    if last.elapsed() < Duration::from_millis(250) {
        return Ok(());
    }
    *last = Instant::now();
    let progress = { state.lock().await.server_progress() };
    cftp_proto::write_frame(control_send, &ServerMessage::Progress(progress)).await?;
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
