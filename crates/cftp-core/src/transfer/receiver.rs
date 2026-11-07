//! Receiver state: stream decoding, payload reads, and file writes.

use super::*;

pub(super) fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(data, offset)
        .context("failed to write chunk payload")
}

#[cfg(windows)]
pub(super) fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
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
pub(super) fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = file.try_clone().context("failed to clone chunk file")?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)
        .context("failed to write chunk payload")
}

pub(super) enum DecodedData {
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

pub(super) async fn decode_stream(
    mut stream: RecvStream,
    activity: Arc<AtomicU64>,
) -> Result<DecodedData> {
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
            let encoded =
                read_payload_idle(&mut stream, pack.compressed_len as usize, &activity).await?;
            let (pack, raw, entries) = tokio::task::spawn_blocking(move || {
                let raw = codec::decompress(pack.codec, encoded, pack.uncompressed_len as usize)?;
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
            let encoded =
                read_payload_idle(&mut stream, chunk.compressed_len as usize, &activity).await?;
            let (chunk, raw) = tokio::task::spawn_blocking(move || {
                let raw = codec::decompress(chunk.codec, encoded, chunk.uncompressed_len as usize)?;
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

/// Reads exactly `len` payload bytes, failing only when the peer makes no
/// progress for `STREAM_READ_IDLE`. A total-duration timeout would break
/// legitimately slow links.
///
/// The buffer grows with the data actually received instead of being allocated
/// up front: a peer that sends only a header and then stalls must not be able to
/// pin a full payload's worth of memory per stream.
pub(super) async fn read_payload_idle(
    stream: &mut RecvStream,
    len: usize,
    activity: &AtomicU64,
) -> Result<Vec<u8>> {
    // Start with the full payload for the common chunk and pack sizes so the
    // data is read directly into its final buffer. Reading into a fixed 64 KiB
    // scratch buffer and growing a Vec copied every payload several times and
    // created one timer per 64 KiB. For larger payloads the allocation still
    // grows gradually, so a peer that sends only a header cannot pin a full
    // 32 MiB per stream across every concurrent stream.
    const DIRECT: usize = 4 * 1024 * 1024;
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut out: Vec<u8> = vec![0u8; len.min(DIRECT)];
    let mut filled: usize = 0;
    loop {
        if filled == out.len() && out.len() < len {
            let grown: usize = out.len().saturating_mul(2).min(len);
            out.resize(grown, 0);
        }
        let read =
            match tokio::time::timeout(STREAM_READ_IDLE, stream.read(&mut out[filled..])).await {
                Err(_) => bail!(
                    "timed out after {}s waiting for a data payload",
                    STREAM_READ_IDLE.as_secs()
                ),
                Ok(Err(err)) => return Err(err).context("failed to read data payload"),
                Ok(Ok(None)) => {
                    bail!("data stream closed with {} bytes missing", len - filled)
                }
                Ok(Ok(Some(read))) => read,
            };
        filled += read;
        activity.fetch_add(1, Ordering::Relaxed);
        if filled == len {
            return Ok(out);
        }
    }
}

pub(super) struct PendingFile {
    pub(super) file: File,
    pub(super) temp_path: PathBuf,
    pub(super) final_path: PathBuf,
    pub(super) entry: ManifestEntry,
}

impl Drop for PendingFile {
    fn drop(&mut self) {
        // A transfer that aborts must not leave partial temp files behind. After
        // a successful rename this is a no-op (the temp path no longer exists).
        let _ = fs::remove_file(&self.temp_path);
    }
}

/// A file reserved for writing from a pack.
pub(super) struct PackWrite {
    pub(super) file_id: u64,
    pub(super) relative: PathBuf,
    pub(super) range: Range<usize>,
    pub(super) size: u64,
    pub(super) mode: u32,
    pub(super) mtime: i64,
}

#[derive(Debug, Default)]
pub(super) struct ReceivedRanges {
    pub(super) ranges: BTreeMap<u64, u64>,
}

impl ReceivedRanges {
    pub(super) fn insert(&mut self, start: u64, len: u64) -> Result<()> {
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

    pub(super) fn covers(&self, size: u64) -> bool {
        size == 0 || self.ranges.len() == 1 && self.ranges.first_key_value() == Some((&0, &size))
    }
}

pub(super) struct ReceiverState {
    pub(super) root: PathBuf,
    /// Manifest entries ordered by id. `entries[id - 1]` is the entry.
    pub(super) entries: Arc<Vec<ManifestEntry>>,
    /// Destination paths ordered by id.
    pub(super) rel_paths: Arc<Vec<PathBuf>>,
    /// Per-file chunk coverage, indexed by `id - 1`.
    pub(super) ranges: Vec<ReceivedRanges>,
    /// Per-file completion flag, indexed by `id - 1`.
    pub(super) completed: Vec<bool>,
    /// Files reserved by a pack but not yet written.
    pub(super) in_progress: Vec<bool>,
    /// Per-file count of chunk writes that have been reserved but not finished.
    pub(super) in_flight: Vec<usize>,
    /// Per-file open temp handle, indexed by `id - 1`.
    pub(super) pending: Vec<Option<PendingFile>>,
    /// Number of `Some` values in `pending`, so the cap check stays O(1).
    pub(super) pending_count: usize,
    pub(super) total_files: u64,
    pub(super) total_bytes: u64,
    pub(super) files_done: u64,
    pub(super) bytes_done: u64,
    pub(super) wire_received: u64,
    pub(super) written_bytes: u64,
    pub(super) started: Instant,
    pub(super) progress: Option<ProgressSender>,
}

impl ReceiverState {
    /// Maps an id on the wire to an index, rejecting unknown ids.
    pub(super) fn slot(&self, file_id: u64) -> Option<usize> {
        file_id
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .filter(|&index| index < self.entries.len())
    }

    pub(super) fn all_complete(&self) -> bool {
        self.files_done == self.total_files
    }

    /// Validates a pack under the lock and reserves every file it contains.
    /// The actual writes happen outside the lock.
    pub(super) fn prepare_pack(
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

    pub(super) fn finish_pack(&mut self, done: Vec<(u64, u64)>) -> Result<()> {
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
    pub(super) fn prepare_chunk(
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

    pub(super) fn finish_chunk(&mut self, file_id: u64, len: u64) -> Result<()> {
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

    pub(super) fn finish_pending(&mut self, file_id: u64) -> Result<()> {
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

    pub(super) fn complete_file(&mut self, file_id: u64, size: u64) -> Result<()> {
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

    pub(super) fn transfer_progress(&self) -> TransferProgress {
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

    pub(super) fn server_progress(&self) -> ServerProgress {
        ServerProgress {
            files_done: self.files_done,
            files_total: self.total_files,
            bytes_total: self.total_bytes,
            received_bytes: self.wire_received,
            written_bytes: self.written_bytes,
            elapsed_millis: self.started.elapsed().as_millis() as u64,
        }
    }

    pub(super) fn publish_progress(&self) {
        if let Some(progress) = &self.progress {
            let _ = progress.send(ProgressEvent::Receiving(self.transfer_progress()));
        }
    }
}
