#![forbid(unsafe_code)]

//! Transfer engine and transport for cftp.
//!
//! The crate is usable without the `cftp` binary. [`transfer::push`] and
//! [`transfer::pull`] run the client halves of a transfer and
//! [`transfer::serve`] runs the server half; all are `async` and only need a
//! Tokio runtime plus an authentication backend from [`auth`]. See [`prelude`]
//! for the common types and the `examples/client.rs` and `examples/server.rs`
//! binaries in this crate.
//!
//! ```no_run
//! use std::net::SocketAddr;
//!
//! use cftp_core::prelude::*;
//!
//! # async fn run(auth: ServerAuth, addr: SocketAddr) -> anyhow::Result<()> {
//! let mut opts = ServeOptions::new("./incoming", addr);
//! opts.one_shot = false;
//! serve(&auth, opts).await?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod codec;
pub mod identity;
pub mod link;
pub mod local;
pub mod path;
pub mod progress;
pub mod scan;
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
        PullOptions, PullStats, PushOptions, PushStats, ServeOptions, ServePolicy, ServeStats,
        pull, push, serve, serve_once,
    };
    pub use crate::tuning::{HardwareProfile, Tuning};
}
