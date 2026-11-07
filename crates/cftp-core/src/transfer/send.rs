//! Writing encoded items to QUIC data streams.

use super::*;

pub(super) struct SentItem {
    pub(super) wire_bytes: u64,
    pub(super) logical_bytes: u64,
    pub(super) files: u64,
    pub(super) file_done: bool,
    pub(super) pack: bool,
}

pub(super) async fn send_item(connection: &Connection, item: EncodedItem) -> Result<SentItem> {
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

pub(super) async fn open_uni(connection: &Connection) -> Result<SendStream> {
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

pub(super) fn apply_sent_item(
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
