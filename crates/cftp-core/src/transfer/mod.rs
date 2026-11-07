//! Push, pull, and serve entry points.
//!
//! The public API lives here and is re-exported from this module. Internally
//! the engine is split by role:
//!
//! - `client` holds the shared client connection setup and option checks.
//! - `push` is the client side of a push and `pull` the client side of a pull.
//! - `server` owns the listening endpoint and the per-session accept loop.
//! - `receive` is the receiving side of a push: it runs the control loop and
//!   validates manifests, while `receiver` decodes streams and writes files.
//! - `pull_server` answers a client pull: it resolves the source, scans it,
//!   sends a manifest, and streams the tree.
//! - `produce` turns local files into packs and chunks for the send path.
//! - `send` writes encoded items to QUIC data streams.
//! - `options` holds the request and statistics types.

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
use tokio::sync::{Semaphore, mpsc, watch};

use crate::auth::{ClientAuth, ServerAuth};
use crate::codec;
use crate::identity::{DEFAULT_SERVER_NAME, Mtu};
use crate::link::{NetworkOptimizer, NetworkSample, compression_levels, detect_link_speed};
use crate::path;
use crate::progress::{ProgressEvent, ProgressSender, TransferProgress};
use crate::scan;
use crate::socket::bind_endpoint;
use crate::tuning::{ENCODED_QUEUE_CAPACITY, Tuning, TuningController, apply_connection_tuning};

/// How long to wait for the QUIC handshake and the control stream to appear.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound for concurrent session slots. `Semaphore::new` panics above
/// tokio's `MAX_PERMITS`, and a huge slot count only invites resource
/// exhaustion, so a caller-supplied `max_sessions` is clamped to this.
const MAX_SESSION_SLOTS: usize = 4096;
/// How long either peer will wait for the other's hello/manifest.
const HELLO_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a server waits for a client's first control message. This is
/// separate from [`HELLO_TIMEOUT`] so a client that authenticates and then goes
/// silent cannot occupy a session slot for two minutes.
const CLIENT_HELLO_TIMEOUT: Duration = Duration::from_secs(30);
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
/// Maximum number of partially received files held open at once. This bounds
/// file descriptors (a typical soft limit is 1024) without limiting normal
/// transfers, which rarely write more than one pack's worth of files at a time.
const MAX_PENDING_FILES: usize = 256;
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

mod client;
mod options;
mod produce;
mod pull;
mod pull_server;
mod push;
mod receive;
mod receiver;
mod send;
mod server;
#[cfg(test)]
mod tests;

// Bring the sibling modules' items into this module's scope so each submodule
// can reach them with `use super::*`. Glob imports do not trigger the
// unused-import lint and stay private to this subtree.
use client::*;
use produce::*;
use pull_server::*;
use push::*;
use receive::*;
use receiver::*;
use send::*;
use server::*;

pub use options::{PullOptions, PullStats, PushOptions, PushStats, TransportStats};
pub use pull::pull;
pub use push::push;
pub use server::{
    MAX_CONCURRENT_SESSIONS, ServeOptions, ServePolicy, ServeStats, SessionReporter, serve,
    serve_once,
};
