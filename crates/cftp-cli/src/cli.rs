//! Command-line interface definitions for the `cftp` binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "cftp", version, about = "Fast file transfers over QUIC")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage the local mTLS identity used for transfers.
    Identity {
        #[command(subcommand)]
        command: IdentityCommand,
    },
    /// Run a one-shot receiving endpoint.
    Serve(ServeArgs),
    /// Push a file or directory tree to a remote endpoint.
    Put(PutArgs),
}

#[derive(Subcommand)]
pub enum IdentityCommand {
    /// Generate a local CA and a leaf certificate/key.
    Init(IdentityInitArgs),
}

#[derive(Args)]
pub struct IdentityInitArgs {
    /// Directory for ca.pem, identity.pem, and identity.key.
    #[arg(long)]
    pub dir: Option<PathBuf>,
    /// Replace existing identity files.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args)]
pub struct ServeArgs {
    /// Directory that received files are written under.
    #[arg(long)]
    pub root: PathBuf,
    /// UDP address to listen on.
    #[arg(long, default_value = "0.0.0.0:4433")]
    pub listen: SocketAddr,
    /// Leaf certificate presented to clients.
    #[arg(long)]
    pub identity: Option<PathBuf>,
    /// Private key for the leaf certificate.
    #[arg(long)]
    pub key: Option<PathBuf>,
    /// CA used to verify client certificates.
    #[arg(long)]
    pub client_ca: Option<PathBuf>,
    /// Do not render a progress bar.
    #[arg(long)]
    pub quiet: bool,
}

#[derive(Args)]
pub struct PutArgs {
    /// Source file or directory.
    ///
    /// A trailing slash on a directory transfers its contents rather than the
    /// directory itself.
    pub src: PathBuf,
    /// Remote cftp:// URL or local destination directory.
    ///
    /// Remote example: cftp://server:4433/project
    /// Local example: ./backup
    ///
    /// Without a trailing slash on the source, the source directory name is
    /// preserved under the destination.
    pub dest: String,
    /// Leaf certificate presented to the server.
    #[arg(long)]
    pub identity: Option<PathBuf>,
    /// Private key for the leaf certificate.
    #[arg(long)]
    pub key: Option<PathBuf>,
    /// CA used to verify the server certificate.
    #[arg(long)]
    pub server_ca: Option<PathBuf>,
    /// TLS server name expected in the server certificate.
    #[arg(long)]
    pub server_name: Option<String>,
    /// Do not render a progress bar.
    #[arg(long)]
    pub quiet: bool,
    /// Size of large-file chunks in bytes.
    #[arg(long, default_value_t = 1024 * 1024)]
    pub chunk_size: usize,
    /// Target size of a small-file pack in bytes.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    pub pack_target: usize,
    /// Maximum files per small-file pack.
    #[arg(long, default_value_t = 128)]
    pub pack_max_files: usize,
    /// Files at or below this size are packed.
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub small_file_max: u64,
}
