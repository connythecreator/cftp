#![forbid(unsafe_code)]

mod commands;

use anyhow::Result;
use cftp_cli::cli::{Cli, Command, IdentityCommand};
use clap::Parser;

use commands::*;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cftp=info".into()),
        )
        .with_target(false)
        // Logs go to stderr: stdout carries transfer results, and the
        // `--bootstrap` handshake reads `CFTP-READY` from it.
        .with_writer(std::io::stderr)
        .init();

    let cli: Cli = Cli::parse();
    match cli.command {
        Command::Identity { command } => match command {
            IdentityCommand::Init(args) => init(args),
        },
        Command::Keygen(args) => keygen(args),
        Command::Pubkey(args) => pubkey(args),
        Command::Serve(args) => run_serve(args).await,
        Command::Put(args) => put(args).await,
        Command::Get(args) => get(args).await,
    }
}
