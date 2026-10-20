use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use cftp_cli::cli::{Cli, Command, IdentityCommand, IdentityInitArgs, PutArgs, ServeArgs};
use cftp_core::identity::{Identity, IdentityPaths, init_identity};
use cftp_core::local::copy_local;
use cftp_core::progress::{ProgressEvent, ProgressSender};
use cftp_core::transfer::{PushOptions, ServeOptions, push, serve_once};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::net::lookup_host;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cftp=info".into()),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Identity { command } => match command {
            IdentityCommand::Init(args) => init(args),
        },
        Command::Serve(args) => serve(args).await,
        Command::Put(args) => put(args).await,
    }
}

fn init(args: IdentityInitArgs) -> Result<()> {
    let paths = match args.dir {
        Some(dir) => IdentityPaths::in_dir(dir),
        None => IdentityPaths::user()?,
    };
    let info = init_identity(&paths, args.force)?;
    println!("identity directory: {}", info.paths.dir.display());
    println!("CA certificate:     {}", info.paths.ca.display());
    println!("leaf certificate:   {}", info.paths.cert.display());
    println!("private key:        {}", info.paths.key.display());
    println!("CA SHA-256:         {}", info.ca_fingerprint);
    println!("leaf SHA-256:       {}", info.cert_fingerprint);
    println!();
    println!("Share ca.pem with peers that should trust this identity.");
    Ok(())
}

async fn serve(args: ServeArgs) -> Result<()> {
    let defaults = IdentityPaths::user()?;
    let identity_path = args.identity.unwrap_or(defaults.cert);
    let key_path = args.key.unwrap_or(defaults.key);
    let ca_path = args.client_ca.unwrap_or(defaults.ca);
    let identity = Identity::load(&identity_path, &key_path, &ca_path)?;

    println!(
        "listening on {} (root {})",
        args.listen,
        args.root.display()
    );
    println!("client CA SHA-256: {}", identity.ca_fingerprint);

    let (progress, progress_task) = progress_channel(args.quiet);
    let options = ServeOptions {
        root: args.root,
        listen: args.listen,
        progress: Some(progress),
        on_ready: None,
    };
    let result = serve_once(&identity, options).await;
    let _ = progress_task.await;
    let stats = result?;
    println!(
        "received {} files, {} bytes from {}",
        stats.files,
        stats.bytes,
        stats
            .peer
            .map(|peer| peer.to_string())
            .unwrap_or_else(|| "unknown peer".to_string())
    );
    Ok(())
}

async fn put(args: PutArgs) -> Result<()> {
    match parse_destination(&args.dest)? {
        Destination::Local(destination) => {
            let (progress, progress_task) = progress_channel(args.quiet);
            let result = copy_local(&args.src, &destination, Some(progress)).await;
            let _ = progress_task.await;
            let stats = result?;
            println!(
                "copied {} files, {} bytes to {}",
                stats.files,
                stats.bytes,
                destination.display()
            );
            Ok(())
        }
        Destination::Remote {
            host,
            port,
            destination,
        } => {
            let addr = lookup_host((host.as_str(), port))
                .await
                .with_context(|| format!("failed to resolve {host}:{port}"))?
                .next()
                .with_context(|| format!("{host}:{port} did not resolve to an address"))?;

            let defaults = IdentityPaths::user()?;
            let identity_path = args.identity.unwrap_or(defaults.cert);
            let key_path = args.key.unwrap_or(defaults.key);
            let ca_path = args.server_ca.unwrap_or(defaults.ca);
            let identity = Identity::load(&identity_path, &key_path, &ca_path)?;

            let (progress, progress_task) = progress_channel(args.quiet);
            let options = PushOptions {
                destination,
                server_name: args
                    .server_name
                    .unwrap_or_else(|| cftp_core::identity::DEFAULT_SERVER_NAME.to_string()),
                progress: Some(progress),
                chunk_size: args.chunk_size,
                pack_target: args.pack_target,
                pack_max_files: args.pack_max_files,
                small_file_max: args.small_file_max,
            };

            let result = push(&args.src, addr, &identity, options).await;
            let _ = progress_task.await;
            let stats = result?;
            println!(
                "sent {} files, {} bytes ({} packs, {} chunks)",
                stats.files, stats.bytes, stats.packs, stats.chunks
            );
            Ok(())
        }
    }
}

enum Destination {
    Remote {
        host: String,
        port: u16,
        destination: String,
    },
    Local(PathBuf),
}

fn parse_destination(dest: &str) -> Result<Destination> {
    if dest.contains("://") {
        let url = Url::parse(dest).with_context(|| format!("invalid destination URL: {dest}"))?;
        if url.scheme() != "cftp" {
            bail!("destination must use the cftp:// scheme");
        }
        let host = url
            .host_str()
            .with_context(|| format!("destination has no host: {dest}"))?
            .to_string();
        let port = url.port().unwrap_or(4433);
        let path = url.path().trim_start_matches('/').to_string();
        return Ok(Destination::Remote {
            host,
            port,
            destination: path,
        });
    }

    if dest.starts_with("cftp:") {
        bail!("invalid cftp URL: {dest}");
    }
    Ok(Destination::Local(PathBuf::from(dest)))
}

fn progress_channel(quiet: bool) -> (ProgressSender, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        if quiet {
            while rx.recv().await.is_some() {}
            return;
        }

        let bar = ProgressBar::new(0);
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
        let mut total_files = 0u64;
        while let Some(event) = rx.recv().await {
            match event {
                ProgressEvent::Scan { files, bytes } => {
                    total_files = files;
                    bar.set_length(bytes);
                    bar.set_message(format!("{files} files"));
                }
                ProgressEvent::Sending {
                    bytes_done,
                    files_done,
                    ..
                }
                | ProgressEvent::Receiving {
                    bytes_done,
                    files_done,
                    ..
                } => {
                    bar.set_position(bytes_done);
                    bar.set_message(format!("{files_done}/{total_files} files"));
                }
                ProgressEvent::Complete { .. } => {
                    bar.finish_with_message("complete");
                }
            }
        }
    });
    (tx, task)
}
