#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use cftp_cli::cli::{Cli, Command, IdentityCommand, IdentityInitArgs, PutArgs, ServeArgs};
use cftp_core::identity::{Identity, IdentityPaths, init_identity};
use cftp_core::local::copy_local;
use cftp_core::progress::{ProgressEvent, ProgressSender, TransferProgress};
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
        let mut tracker = RateTracker::new();
        let mut last_message = String::from("0 files");
        while let Some(event) = rx.recv().await {
            match event {
                ProgressEvent::Scan { files, bytes } => {
                    bar.set_length(bytes);
                    last_message = format!("{files} files");
                    bar.set_message(last_message.clone());
                }
                ProgressEvent::Sending(stats) | ProgressEvent::Receiving(stats) => {
                    bar.set_length(stats.bytes_total);
                    bar.set_position(stats.bytes_done);
                    let rates = tracker.update(&stats);
                    last_message = format_progress(&stats, &rates);
                    bar.set_message(last_message.clone());
                }
                ProgressEvent::Complete { .. } => {
                    bar.finish_with_message(format!("complete | {last_message}"));
                }
            }
        }
    });
    (tx, task)
}

#[derive(Clone, Copy, Default)]
struct Rates {
    read: f64,
    sent: f64,
    received: f64,
    written: f64,
}

struct RateTracker {
    last: Instant,
    read: u64,
    sent: u64,
    received: u64,
    written: u64,
    rates: Rates,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            last: Instant::now(),
            read: 0,
            sent: 0,
            received: 0,
            written: 0,
            rates: Rates::default(),
        }
    }

    fn update(&mut self, stats: &TransferProgress) -> Rates {
        let elapsed = stats
            .at
            .checked_duration_since(self.last)
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0);
        let remote = (stats.remote_elapsed_millis > 0).then(|| {
            let seconds = stats.remote_elapsed_millis as f64 / 1000.0;
            (
                stats.received_bytes as f64 / seconds,
                stats.written_bytes as f64 / seconds,
            )
        });

        if let Some((received, written)) = remote {
            self.rates.received = received;
            self.rates.written = written;
        }

        if elapsed < 0.05 {
            return self.rates;
        }

        let instant =
            |current: u64, previous: u64| current.saturating_sub(previous) as f64 / elapsed;
        let alpha = 0.4;
        let smooth = |old: f64, new: f64| {
            if old == 0.0 {
                new
            } else {
                old * (1.0 - alpha) + new * alpha
            }
        };

        self.rates.read = smooth(self.rates.read, instant(stats.read_bytes, self.read));
        self.rates.sent = smooth(self.rates.sent, instant(stats.sent_bytes, self.sent));
        if remote.is_none() {
            self.rates.received = smooth(
                self.rates.received,
                instant(stats.received_bytes, self.received),
            );
            self.rates.written = smooth(
                self.rates.written,
                instant(stats.written_bytes, self.written),
            );
        }
        self.last = stats.at;
        self.read = stats.read_bytes;
        self.sent = stats.sent_bytes;
        self.received = stats.received_bytes;
        self.written = stats.written_bytes;
        self.rates
    }
}

fn format_progress(stats: &TransferProgress, rates: &Rates) -> String {
    let mut parts = vec![format!("{}/{} files", stats.files_done, stats.files_total)];
    if stats.read_bytes > 0 {
        parts.push(format!("read {}", format_rate(rates.read)));
    }
    if stats.sent_bytes > 0 {
        parts.push(format!("sent {}", format_rate(rates.sent)));
    }
    if stats.received_bytes > 0 {
        parts.push(format!("recv {}", format_rate(rates.received)));
    }
    if stats.written_bytes > 0 {
        parts.push(format!("write {}", format_rate(rates.written)));
    }
    parts.join(" | ")
}

fn format_rate(bytes_per_second: f64) -> String {
    let bits = bytes_per_second * 8.0;
    if bits >= 1_000_000_000.0 {
        format!("{:.2} Gbps", bits / 1_000_000_000.0)
    } else if bits >= 1_000_000.0 {
        format!("{:.1} Mbps", bits / 1_000_000.0)
    } else if bits >= 1_000.0 {
        format!("{:.0} Kbps", bits / 1_000.0)
    } else {
        format!("{bits:.0} bps")
    }
}
