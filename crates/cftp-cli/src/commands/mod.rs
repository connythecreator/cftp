//! CLI command implementations.
//!
//! Each command owns its argument handling and progress reporting; the transfer
//! engine itself lives in `cftp-core`. Shared imports live here so every
//! command module can pull them in with `use super::*`.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cftp_cli::cli::{
    AuthMode, GetArgs, IdentityInitArgs, KeygenArgs, PubkeyArgs, PutArgs, ServeArgs,
};
use cftp_cli::hotkeys::Hotkeys;
use cftp_core::auth::{
    ClientAuth, HostKeyPolicy, ServerAuth, SshClientAuth, SshServerAuth, UnknownHostKey,
};
use cftp_core::identity::{Identity, IdentityPaths, init_identity};
use cftp_core::link::lan_addresses;
use cftp_core::local::{LocalCopyStats, copy_local};
use cftp_core::progress::{ProgressEvent, ProgressSender, TransferProgress};
use cftp_core::ssh::{
    SshPaths, SshPublicKey, generate_keypair, load_or_create_host_key, load_private_key,
    write_private_key,
};
use cftp_core::transfer::{
    PullOptions, PullStats, PushOptions, PushStats, ServeOptions, ServePolicy, ServeStats,
    TransportStats, pull, push, serve,
};
use cftp_core::trust::{AuthorizedKeys, HostKeyStatus, KnownHostsStore};
use cftp_core::tuning::{HardwareProfile, Tuning};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::net::lookup_host;
use url::Url;

mod get;
mod identity;
mod progress;
mod put;
mod remote;
mod serve;

pub(crate) use get::*;
pub(crate) use identity::*;
use progress::*;
pub(crate) use put::*;
use remote::*;
pub(crate) use serve::*;
