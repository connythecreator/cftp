//! Command-line interface definitions for the `cftp` binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use cftp_core::tuning::HardwareProfile;

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum AuthMode {
    /// Use certificate mode when certificate flags are supplied, otherwise SSH keys.
    #[default]
    Auto,
    /// Use SSH key authentication.
    Ssh,
    /// Use the existing CA and certificate identity.
    Cert,
}

#[derive(Args, Debug, Clone, Copy, Default)]
#[command(group = clap::ArgGroup::new("hardware").args(["light", "default_profile", "aggressive"]).multiple(false))]
pub struct HardwareArgs {
    /// Minimize CPU, memory, and network usage.
    #[arg(long)]
    pub light: bool,
    /// Use the default tuning.
    #[arg(long = "default")]
    pub default_profile: bool,
    /// Saturate available CPU, memory, and network.
    #[arg(long)]
    pub aggressive: bool,
}

impl HardwareArgs {
    pub fn selected(self) -> Option<HardwareProfile> {
        if self.light {
            Some(HardwareProfile::Light)
        } else if self.aggressive {
            Some(HardwareProfile::Aggressive)
        } else if self.default_profile {
            Some(HardwareProfile::Default)
        } else {
            None
        }
    }
}

#[derive(Args)]
pub struct ServeArgs {
    /// Directory that received files are written under.
    #[arg(long)]
    pub root: PathBuf,
    /// Authentication backend.
    #[arg(long, value_enum, default_value_t = AuthMode::Auto)]
    pub auth: AuthMode,
    /// SSH private key used as the server host key.
    #[arg(long)]
    pub host_key: Option<PathBuf>,
    /// SSH authorized_keys file used to verify clients.
    #[arg(long)]
    pub authorized_keys: Option<PathBuf>,
    /// UDP address to listen on.
    #[arg(long, default_value = "0.0.0.0:4433")]
    pub listen: SocketAddr,
    /// Maximum hardware profile the server will accept.
    #[command(flatten)]
    pub hardware: HardwareArgs,
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
    /// Authentication backend.
    #[arg(long, value_enum, default_value_t = AuthMode::Auto)]
    pub auth: AuthMode,
    /// SSH private key presented to the server.
    #[arg(long)]
    pub ssh_key: Option<PathBuf>,
    /// SSH known_hosts file used to verify the server host key.
    #[arg(long)]
    pub known_hosts: Option<PathBuf>,
    /// Add an unknown server host key to known_hosts without prompting.
    #[arg(long)]
    pub accept_new: bool,
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
    /// Hardware profile. Hotkeys can change it during the transfer.
    #[command(flatten)]
    pub hardware: HardwareArgs,
    /// Size of large-file chunks in bytes.
    #[arg(long, conflicts_with = "hardware")]
    pub chunk_size: Option<usize>,
    /// Target size of a small-file pack in bytes.
    #[arg(long, conflicts_with = "hardware")]
    pub pack_target: Option<usize>,
    /// Maximum files per small-file pack.
    #[arg(long, conflicts_with = "hardware")]
    pub pack_max_files: Option<usize>,
    /// Files at or below this size are packed.
    #[arg(long, conflicts_with = "hardware")]
    pub small_file_max: Option<u64>,
}
