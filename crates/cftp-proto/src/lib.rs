#![forbid(unsafe_code)]

//! Wire protocol for cftp.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const ALPN: &[u8] = b"cftp/2";
pub const PROTOCOL_VERSION: u16 = 2;
pub const MAX_CONTROL_FRAME: usize = 256 * 1024 * 1024;
pub const MAX_DATA_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("frame length is zero")]
    EmptyFrame,
    #[error("codec error: {0}")]
    Codec(#[from] postcard::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferMode {
    Push,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub id: u64,
    /// Relative path with `/` separators. Never absolute and never contains `..`.
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
    pub mode: u32,
    pub mtime: i64,
    pub link_target: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHello {
    pub version: u16,
    pub mode: TransferMode,
    pub destination: String,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHello {
    pub version: u16,
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Codec {
    Raw,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackEntry {
    pub file_id: u64,
    pub size: u64,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackHeader {
    pub pack_id: u64,
    pub codec: Codec,
    pub uncompressed_len: u64,
    pub compressed_len: u64,
    pub entries: Vec<PackEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub file_id: u64,
    pub offset: u64,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub codec: Codec,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataHeader {
    Pack(PackHeader),
    Chunk(ChunkHeader),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Complete {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Done {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerProgress {
    pub files_done: u64,
    pub files_total: u64,
    pub bytes_total: u64,
    pub received_bytes: u64,
    pub written_bytes: u64,
    pub elapsed_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Abort {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    Hello(ClientHello),
    Complete(Complete),
    Abort(Abort),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Hello(ServerHello),
    Done(Done),
    Error(Abort),
    Progress(ServerProgress),
}

pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), ProtoError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = postcard::to_allocvec(value)?;
    let len = u32::try_from(body.len()).map_err(|_| ProtoError::FrameTooLarge(body.len()))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R, T>(reader: &mut R) -> Result<T, ProtoError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Err(ProtoError::EmptyFrame);
    }
    if len > MAX_CONTROL_FRAME {
        return Err(ProtoError::FrameTooLarge(len));
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    Ok(postcard::from_bytes(&body)?)
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtoError> {
    Ok(postcard::to_allocvec(value)?)
}

pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtoError> {
    Ok(postcard::from_bytes(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip() {
        let msg = ClientMessage::Hello(ClientHello {
            version: PROTOCOL_VERSION,
            mode: TransferMode::Push,
            destination: "dest".into(),
            entries: vec![ManifestEntry {
                id: 1,
                path: "src/main.js".into(),
                kind: EntryKind::File,
                size: 42,
                mode: 0o644,
                mtime: 123,
                link_target: None,
            }],
        });
        let bytes = encode(&msg).unwrap();
        let decoded: ClientMessage = decode(&bytes).unwrap();
        match decoded {
            ClientMessage::Hello(hello) => assert_eq!(hello.entries[0].path, "src/main.js"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_progress_roundtrip() {
        let message = ServerMessage::Progress(ServerProgress {
            files_done: 2,
            files_total: 3,
            bytes_total: 100,
            received_bytes: 50,
            written_bytes: 40,
            elapsed_millis: 250,
        });
        let bytes = encode(&message).unwrap();
        let decoded: ServerMessage = decode(&bytes).unwrap();
        match decoded {
            ServerMessage::Progress(progress) => {
                assert_eq!(progress.files_done, 2);
                assert_eq!(progress.received_bytes, 50);
                assert_eq!(progress.written_bytes, 40);
                assert_eq!(progress.elapsed_millis, 250);
            }
            _ => panic!("wrong variant"),
        }
    }
}
