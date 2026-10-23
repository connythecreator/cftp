#![forbid(unsafe_code)]

//! Wire protocol for cftp.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const ALPN_V2: &[u8] = b"cftp/2";
pub const ALPN_V3: &[u8] = b"cftp/3";
pub const ALPN_V4: &[u8] = b"cftp/4";
pub const PROTOCOL_VERSION_V2: u16 = 2;
pub const PROTOCOL_VERSION_V3: u16 = 3;
pub const PROTOCOL_VERSION_V4: u16 = 4;
pub const PROTOCOL_VERSION: u16 = PROTOCOL_VERSION_V4;
/// Upper bound for a control frame (hello manifests, progress, and so on).
///
/// This is deliberately far above any legitimate control message but low enough
/// that a peer cannot make us allocate an arbitrary amount of memory by
/// declaring a huge frame and then going silent.
pub const MAX_CONTROL_FRAME: usize = 64 * 1024 * 1024;
/// Upper bound for a single data header. Data headers only carry metadata, but a
/// pack header may list many entries (`MAX_PACK_ENTRIES`).
pub const MAX_DATA_HEADER: usize = 8 * 1024 * 1024;
pub const MAX_DATA_PAYLOAD: usize = 64 * 1024 * 1024;

/// Maximum number of manifest entries accepted in a single transfer.
pub const MAX_MANIFEST_ENTRIES: usize = 100_000;
/// Maximum length of a relative path or symlink target in a manifest.
pub const MAX_MANIFEST_PATH_LEN: usize = 4096;
/// Maximum declared size of a single file (1 TiB).
pub const MAX_FILE_SIZE: u64 = 1 << 40;
/// Maximum declared size of a whole transfer (16 TiB).
pub const MAX_TRANSFER_BYTES: u64 = 16 << 40;
/// Maximum number of entries in one pack header.
pub const MAX_PACK_ENTRIES: usize = 1 << 16;

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
    Pull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum HardwareProfile {
    Light,
    #[default]
    Default,
    Aggressive,
}

impl HardwareProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Default => "default",
            Self::Aggressive => "aggressive",
        }
    }
}

impl std::fmt::Display for HardwareProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHelloV3 {
    pub version: u16,
    pub mode: TransferMode,
    pub destination: String,
    pub entries: Vec<ManifestEntry>,
    pub profile: HardwareProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHelloV3 {
    pub version: u16,
    pub accepted: bool,
    pub message: String,
    pub effective_profile: HardwareProfile,
    pub server_cap: HardwareProfile,
    pub profile_accepted: bool,
}

/// `cftp/4` hello. For a push, `destination` is the path under the server root;
/// for a pull, it is the source path the client wants to read and `entries` is
/// empty.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHelloV4 {
    pub version: u16,
    pub mode: TransferMode,
    pub destination: String,
    pub entries: Vec<ManifestEntry>,
    pub profile: HardwareProfile,
}

/// `cftp/4` server hello. `allow_push` and `allow_pull` are the server policy
/// so a client can report a clear reason when a mode is disabled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHelloV4 {
    pub version: u16,
    pub accepted: bool,
    pub message: String,
    pub effective_profile: HardwareProfile,
    pub server_cap: HardwareProfile,
    pub profile_accepted: bool,
    pub allow_push: bool,
    pub allow_pull: bool,
}

/// A pull manifest. Entries are ordered by id and carry paths relative to the
/// requested source, exactly like a push manifest carries paths relative to
/// the destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerManifest {
    pub entries: Vec<ManifestEntry>,
    pub total_files: u64,
    pub total_bytes: u64,
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
pub enum ClientMessageV3 {
    Hello(ClientHelloV3),
    Complete(Complete),
    Abort(Abort),
    SetProfile {
        request_id: u64,
        profile: HardwareProfile,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Hello(ServerHello),
    Done(Done),
    Error(Abort),
    Progress(ServerProgress),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessageV3 {
    Hello(ServerHelloV3),
    Done(Done),
    Error(Abort),
    Progress(ServerProgress),
    ProfileChanged {
        request_id: u64,
        accepted: bool,
        effective_profile: HardwareProfile,
        server_cap: HardwareProfile,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessageV4 {
    Hello(ClientHelloV4),
    Complete(Complete),
    Abort(Abort),
    SetProfile {
        request_id: u64,
        profile: HardwareProfile,
    },
    /// Sent by a pull client after it created the directory tree and is ready
    /// for data streams.
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessageV4 {
    Hello(ServerHelloV4),
    Manifest(ServerManifest),
    Done(Done),
    Error(Abort),
    Progress(ServerProgress),
    ProfileChanged {
        request_id: u64,
        accepted: bool,
        effective_profile: HardwareProfile,
        server_cap: HardwareProfile,
        message: String,
    },
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
    read_frame_with_limit(reader, MAX_CONTROL_FRAME).await
}

/// Reads one length-prefixed frame, refusing bodies larger than `max`.
///
/// Callers that know a frame only carries metadata should pass a small `max` so
/// that a hostile peer cannot force a large allocation with a fake length.
pub async fn read_frame_with_limit<R, T>(reader: &mut R, max: usize) -> Result<T, ProtoError>
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
    if len > max {
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
            version: PROTOCOL_VERSION_V2,
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

    #[tokio::test]
    async fn read_frame_rejects_an_empty_frame() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(16);
        client.write_all(&[0, 0, 0, 0]).await.unwrap();
        let result: Result<ClientMessage, ProtoError> =
            read_frame_with_limit(&mut server, MAX_CONTROL_FRAME).await;
        assert!(matches!(result, Err(ProtoError::EmptyFrame)));
    }

    #[tokio::test]
    async fn read_frame_rejects_a_body_larger_than_the_limit() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(16);
        // Declares four bytes but the caller only allows one. The length is
        // rejected before any body buffer is allocated.
        client.write_all(&[0, 0, 0, 4]).await.unwrap();
        let result: Result<ClientMessage, ProtoError> = read_frame_with_limit(&mut server, 1).await;
        assert!(matches!(result, Err(ProtoError::FrameTooLarge(4))));
    }

    #[tokio::test]
    async fn read_frame_fails_instead_of_hanging_on_a_truncated_body() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(16);
        client.write_all(&[0, 0, 0, 8]).await.unwrap();
        drop(client);
        let result: Result<ClientMessage, ProtoError> =
            read_frame_with_limit(&mut server, MAX_CONTROL_FRAME).await;
        assert!(result.is_err());
    }

    #[test]
    fn v4_pull_hello_ready_and_manifest_roundtrip() {
        let hello = ClientMessageV4::Hello(ClientHelloV4 {
            version: PROTOCOL_VERSION_V4,
            mode: TransferMode::Pull,
            destination: "project/".into(),
            entries: Vec::new(),
            profile: HardwareProfile::Default,
        });
        match decode(&encode(&hello).unwrap()).unwrap() {
            ClientMessageV4::Hello(hello) => {
                assert_eq!(hello.version, PROTOCOL_VERSION_V4);
                assert_eq!(hello.mode, TransferMode::Pull);
                assert_eq!(hello.destination, "project/");
                assert!(hello.entries.is_empty());
            }
            _ => panic!("wrong variant"),
        }
        assert_eq!(
            encode(&ClientMessageV4::Ready).unwrap(),
            encode(&ClientMessageV4::Ready).unwrap()
        );

        let manifest = ServerMessageV4::Manifest(ServerManifest {
            entries: vec![ManifestEntry {
                id: 1,
                path: "a.txt".into(),
                kind: EntryKind::File,
                size: 3,
                mode: 0o644,
                mtime: 1,
                link_target: None,
            }],
            total_files: 1,
            total_bytes: 3,
        });
        match decode(&encode(&manifest).unwrap()).unwrap() {
            ServerMessageV4::Manifest(manifest) => {
                assert_eq!(manifest.total_files, 1);
                assert_eq!(manifest.entries[0].path, "a.txt");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn v4_server_hello_carries_the_mode_policy() {
        let hello = ServerMessageV4::Hello(ServerHelloV4 {
            version: PROTOCOL_VERSION_V4,
            accepted: true,
            message: "ready".into(),
            effective_profile: HardwareProfile::Light,
            server_cap: HardwareProfile::Default,
            profile_accepted: false,
            allow_push: false,
            allow_pull: true,
        });
        match decode(&encode(&hello).unwrap()).unwrap() {
            ServerMessageV4::Hello(hello) => {
                assert!(!hello.allow_push);
                assert!(hello.allow_pull);
            }
            _ => panic!("wrong variant"),
        }
    }
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[test]
    fn profiles_are_ordered_for_caps() {
        assert!(HardwareProfile::Light < HardwareProfile::Default);
        assert!(HardwareProfile::Default < HardwareProfile::Aggressive);
        assert_eq!(
            HardwareProfile::Aggressive.min(HardwareProfile::Default),
            HardwareProfile::Default
        );
    }

    #[test]
    fn v3_hello_and_profile_change_roundtrip() {
        let hello = ClientMessageV3::Hello(ClientHelloV3 {
            version: PROTOCOL_VERSION_V3,
            mode: TransferMode::Push,
            destination: "dest".into(),
            entries: Vec::new(),
            profile: HardwareProfile::Aggressive,
        });
        let decoded: ClientMessageV3 = decode(&encode(&hello).unwrap()).unwrap();
        match decoded {
            ClientMessageV3::Hello(hello) => {
                assert_eq!(hello.version, PROTOCOL_VERSION_V3);
                assert_eq!(hello.profile, HardwareProfile::Aggressive);
            }
            _ => panic!("wrong variant"),
        }

        let changed = ServerMessageV3::ProfileChanged {
            request_id: 7,
            accepted: false,
            effective_profile: HardwareProfile::Default,
            server_cap: HardwareProfile::Default,
            message: "capped".into(),
        };
        let decoded: ServerMessageV3 = decode(&encode(&changed).unwrap()).unwrap();
        match decoded {
            ServerMessageV3::ProfileChanged {
                request_id,
                accepted,
                effective_profile,
                ..
            } => {
                assert_eq!(request_id, 7);
                assert!(!accepted);
                assert_eq!(effective_profile, HardwareProfile::Default);
            }
            _ => panic!("wrong variant"),
        }
    }
}
