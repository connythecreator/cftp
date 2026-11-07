#![forbid(unsafe_code)]

//! Transfer engine and transport for cftp.
//!
//! This crate is usable without the `cftp` binary. It is intended to be
//! embedded in another program: a desktop client, a server-side attachment
//! cache, or a download manager.
//!
//! # Entry points
//!
//! - [`transfer::push`] scans a local tree and sends it to a server.
//! - [`transfer::pull`] asks a server for a path under its root and writes the
//!   returned tree locally. The server is authoritative over the path.
//! - [`transfer::serve`] is the server half. It can run one session
//!   ([`transfer::serve_once`]) or stay alive for repeated sessions.
//!
//! All three are `async` and need a Tokio runtime plus an authentication
//! backend from [`auth`]. A multi-thread runtime is recommended: compression,
//! hashing, and disk writes run on blocking threads, and the reader pool scales
//! with the hardware profile.
//!
//! # Progress and cancellation
//!
//! Pass a channel from [`progress::channel`] in [`transfer::PullOptions`] or
//! [`transfer::PushOptions`]. Events arrive while the transfer runs; the
//! channel is unbounded, so a slow UI never blocks the engine. Keep the
//! receiver alive until the transfer future completes.
//!
//! Cancellation is cancellation of the future. Dropping the `push`, `pull`, or
//! `serve` future (for example with [`tokio::task::JoinHandle::abort`]) closes
//! the QUIC connection; a server sees the session end. This is enough for a
//! "cancel download" button:
//!
//! ```no_run
//! use std::path::PathBuf;
//! use std::net::SocketAddr;
//!
//! use cftp_core::prelude::*;
//!
//! # async fn run(addr: SocketAddr, auth: ClientAuth) -> anyhow::Result<()> {
//! let (progress, mut events) = cftp_core::progress::channel();
//! let options = PullOptions::new("media/movie.mkv", PathBuf::from("./downloads"))
//!     .with_progress(progress);
//! let task = tokio::spawn(async move { pull(addr, &auth, options).await });
//! tokio::spawn(async move {
//!     while let Some(event) = events.recv().await {
//!         println!("{event:?}");
//!     }
//! });
//!
//! // A cancel button calls:
//! task.abort();
//! # Ok(())
//! # }
//! ```
//!
//! Aborting the task drops the `pull` future, which closes the connection and
//! leaves no partially written file behind: the receiver only renames a
//! temporary file into place after its hash has been verified.
//!
//! # One transfer, one connection
//!
//! Each `push` or `pull` opens its own QUIC connection. A download manager that
//! fetches many files should bound its concurrency: a persistent server serves
//! at most [`transfer::MAX_CONCURRENT_SESSIONS`] sessions at once by default,
//! and more clients wait for a slot. Set [`transfer::ServeOptions::max_sessions`]
//! on the server and a matching limit in the caller.
//!
//! See [`prelude`] for the common types and the `examples/` directory for
//! runnable servers, clients, and a small download manager.

pub mod auth;
pub mod codec;
mod congestion;
pub mod identity;
pub mod link;
pub mod local;
pub mod path;
pub mod progress;
pub mod scan;
mod socket;
pub mod ssh;
pub mod transfer;
pub mod trust;
pub mod tuning;
pub mod wire;

/// Types most applications need to embed cftp.
pub mod prelude {
    pub use crate::auth::{ClientAuth, HostKeyPolicy, ServerAuth};
    pub use crate::progress::{ProgressEvent, ProgressSender, TransferProgress};
    pub use crate::transfer::{
        MAX_CONCURRENT_SESSIONS, PullOptions, PullStats, PushOptions, PushStats, ServeOptions,
        ServePolicy, ServeStats, TransportStats, pull, push, serve, serve_once,
    };
    pub use crate::tuning::{HardwareProfile, Tuning};
}
