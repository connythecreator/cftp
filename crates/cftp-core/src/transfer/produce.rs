//! Local files to wire items: packing, chunking, parallel reads.

use super::*;

pub(super) struct BuiltPack {
    header: PackHeader,
    payload: Vec<u8>,
    logical_bytes: u64,
    files: u64,
}

pub(super) enum EncodedItem {
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

pub(super) async fn produce_items(
    scan: scan::ScanResult,
    controller: Arc<TuningController>,
    tx: mpsc::Sender<Result<EncodedItem>>,
) {
    if let Err(err) = produce_items_inner(scan, controller, &tx).await {
        let _ = tx.send(Err(err)).await;
    }
}

pub(super) async fn produce_items_inner(
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

pub(super) async fn enqueue_pack(
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

pub(super) fn build_pack(
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
        codec::compress(payload, pack_level, true)?
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

/// Claims the next range on `cursor`. The chunk size is snapshotted at claim
/// time, so a profile switch changes the size of subsequently claimed ranges
/// rather than only affecting the next file.
pub(super) fn reserve_chunk(
    cursor: &AtomicU64,
    expected_size: u64,
    controller: &TuningController,
) -> Option<(u64, usize)> {
    loop {
        let offset: u64 = cursor.load(Ordering::Relaxed);
        if offset >= expected_size {
            return None;
        }
        let chunk_size: usize = controller.snapshot().chunk_size.max(1);
        let len: u64 = (expected_size - offset).min(chunk_size as u64);
        if cursor
            .compare_exchange(offset, offset + len, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Some((offset, len as usize));
        }
    }
}

/// One of the concurrent blocking readers. Workers claim disjoint ranges from
/// the shared cursor and read them with positional I/O, so they run genuinely
/// in parallel without contending over a shared file cursor. Reading is
/// decoupled from the encoding permits, so the disk keeps feeding compression
/// workers while every permit is held.
///
/// Errors are delivered through the channel rather than returned: the consumer
/// must observe them. A worker only stops early when the consumer has dropped
/// the receiver, which means the transfer has already failed.
pub(super) fn read_worker(
    file: &File,
    full_path: &Path,
    expected_size: u64,
    cursor: &AtomicU64,
    controller: &TuningController,
    chunk_tx: &mpsc::Sender<Result<(u64, Vec<u8>, bool)>>,
) -> Result<()> {
    while let Some((offset, len)) = reserve_chunk(cursor, expected_size, controller) {
        let mut data: Vec<u8> = vec![0u8; len];
        let result: Result<(u64, Vec<u8>, bool)> = match read_at(file, offset, &mut data) {
            Ok(()) => Ok((offset, data, offset + len as u64 == expected_size)),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Err(
                anyhow::anyhow!("{} changed while scanning", full_path.display()),
            ),
            Err(error) => Err(anyhow::Error::new(error)
                .context(format!("failed to read {}", full_path.display()))),
        };
        let failed: bool = result.is_err();
        if chunk_tx.blocking_send(result).is_err() || failed {
            // The consumer is gone, or it already saw this worker's error.
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, data: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(data, offset)
}

#[cfg(windows)]
fn read_at(file: &File, offset: u64, data: &mut [u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut read = 0usize;
    while read < data.len() {
        let count = file.seek_read(&mut data[read..], offset + read as u64)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to read chunk payload",
            ));
        }
        read += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_at(file: &File, offset: u64, data: &mut [u8]) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(data)
}

pub(super) async fn enqueue_large(
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    permits: &Arc<Semaphore>,
    base: &Path,
    entry: &ManifestEntry,
    controller: &Arc<TuningController>,
    tx: &mpsc::Sender<Result<EncodedItem>>,
) -> Result<()> {
    let full_path = base.join(Path::new(&entry.path));
    let tuning: Tuning = controller.snapshot();
    let read_workers: usize = tuning.read_workers.max(1);
    // The channel is the read-ahead window: at most `readahead_chunks` fully
    // read chunks wait to be encoded, so peak read-ahead memory is
    // `readahead_chunks * chunk_size`, plus one in-flight buffer per worker
    // while it is blocked on a full channel.
    let (chunk_tx, mut chunk_rx) =
        mpsc::channel::<Result<(u64, Vec<u8>, bool)>>(tuning.readahead_chunks.max(1));
    let allow_skip: bool = !codec::extension_is_incompressible(&entry.path);
    let file_id: u64 = entry.id;
    let expected_size: u64 = entry.size;

    let file: Arc<File> = Arc::new(
        tokio::fs::File::open(&full_path)
            .await
            .with_context(|| format!("failed to open {}", full_path.display()))?
            .into_std()
            .await,
    );
    let cursor: Arc<AtomicU64> = Arc::new(AtomicU64::new(0u64));
    let controller: Arc<TuningController> = Arc::clone(controller);

    for _ in 0..read_workers {
        let file: Arc<File> = Arc::clone(&file);
        let cursor: Arc<AtomicU64> = Arc::clone(&cursor);
        let controller: Arc<TuningController> = Arc::clone(&controller);
        let chunk_tx: mpsc::Sender<Result<(u64, Vec<u8>, bool)>> = chunk_tx.clone();
        let full_path: PathBuf = full_path.clone();
        tasks.spawn_blocking(move || {
            read_worker(
                &file,
                &full_path,
                expected_size,
                &cursor,
                &controller,
                &chunk_tx,
            )
        });
    }
    // The consumer holds the other sender; dropping this one lets the channel
    // close once every worker has finished.
    drop(chunk_tx);

    while let Some(result) = chunk_rx.recv().await {
        let (chunk_offset, data, done): (u64, Vec<u8>, bool) = result?;
        let chunk_level: i32 = controller.snapshot().chunk_zstd_level;
        // Encoding concurrency stays bounded by `encode_workers`: reading is
        // decoupled from permits, but each chunk still needs one to hash and
        // compress.
        let permit: tokio::sync::OwnedSemaphorePermit = permits
            .clone()
            .acquire_owned()
            .await
            .context("encoding semaphore closed")?;
        let tx: mpsc::Sender<Result<EncodedItem>> = tx.clone();
        let chunk_len: usize = data.len();
        tasks.spawn(async move {
            let _permit = permit;
            let (hash, chunk_codec, encoded): ([u8; 32], Codec, Vec<u8>) =
                tokio::task::spawn_blocking(move || {
                    let hash = codec::hash(&data);
                    let (chunk_codec, encoded) = if allow_skip {
                        codec::compress(data, chunk_level, true)?
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
    }

    Ok(())
}
