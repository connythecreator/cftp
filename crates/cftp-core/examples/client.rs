//! Minimal client built on `cftp-core`, without the `cftp` binary.
//!
//! ```sh
//! cargo run -p cftp-core --example client -- ./project cftp://receiver:4433/project
//! cargo run -p cftp-core --example client -- get cftp://receiver:4433/project ./downloads
//! ```
//!
//! The first form pushes a local tree to the server. The second form pulls a
//! path under the server's root into a local directory. Add `--accept-new` to
//! trust and persist an unknown host key. Without it an unknown key fails the
//! handshake and prints the fingerprint, like SSH.

#![forbid(unsafe_code)]

use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use cftp_core::auth::{ClientAuth, HostKeyPolicy, UnknownHostKey, load_client_auth};
use cftp_core::identity::DEFAULT_SERVER_NAME;
use cftp_core::progress::{ProgressEvent, ProgressSender};
use cftp_core::ssh::SshPaths;
use cftp_core::transfer::{PullOptions, PullStats, PushOptions, PushStats, pull, push};
use cftp_core::trust::KnownHostsStore;
use tokio::net::lookup_host;

const USAGE: &str = "usage:\n  client <src> cftp://host[:port]/path [--accept-new]\n  \
                     client get cftp://host[:port]/path <dest-dir> [--accept-new]";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let first = args.next().context(USAGE)?;

    if first == "get" {
        let remote = args.next().context(USAGE)?;
        let destination: PathBuf = args.next().context(USAGE)?.into();
        let accept_new = args.any(|arg| arg == "--accept-new");
        let (host, port, source) = parse_remote(&remote)?;
        return run_pull(&host, port, source, destination, accept_new).await;
    }

    let destination = args.next().context(USAGE)?;
    let accept_new = args.any(|arg| arg == "--accept-new");
    run_push(&first, &destination, accept_new).await
}

async fn run_push(src: &str, destination: &str, accept_new: bool) -> Result<()> {
    let (host, port, path) = parse_remote(destination)?;
    let addr = resolve(&host, port).await?;
    let auth = client_auth(&host, port, accept_new)?;
    let (progress, progress_task) = progress_channel();

    let options = PushOptions {
        destination: path,
        server_name: DEFAULT_SERVER_NAME.to_string(),
        progress: Some(progress),
        ..PushOptions::default()
    };

    let result = push(Path::new(src), addr, &auth, options).await;
    // `push` consumed `options`, so the progress sender is gone and the drain
    // task ends on its own.
    let _ = progress_task.await;
    match result {
        Ok(stats) => {
            report_push(&stats);
            Ok(())
        }
        Err(err) => report_error(err),
    }
}

async fn run_pull(
    host: &str,
    port: u16,
    source: String,
    destination: PathBuf,
    accept_new: bool,
) -> Result<()> {
    let addr = resolve(host, port).await?;
    let auth = client_auth(host, port, accept_new)?;
    let (progress, progress_task) = progress_channel();

    let options = PullOptions {
        source,
        destination,
        server_name: DEFAULT_SERVER_NAME.to_string(),
        progress: Some(progress),
        ..PullOptions::default()
    };

    let result = pull(addr, &auth, options).await;
    // `pull` consumed `options`, so the progress sender is gone and the drain
    // task ends on its own.
    let _ = progress_task.await;
    match result {
        Ok(stats) => {
            report_pull(&stats);
            Ok(())
        }
        Err(err) => report_error(err),
    }
}

fn client_auth(host: &str, port: u16, accept_new: bool) -> Result<ClientAuth> {
    let ssh = SshPaths::user()?;
    let key = ssh
        .existing_client_key()
        .with_context(|| format!("no SSH key found in {}", ssh.dir.display()))?;
    let known_hosts = KnownHostsStore::load(&ssh.known_hosts)?;
    let policy = if accept_new {
        HostKeyPolicy::AcceptNew
    } else {
        HostKeyPolicy::Ask
    };
    load_client_auth(&key, known_hosts, host.to_string(), port, policy, |path| {
        bail!(
            "SSH key {} is encrypted; decrypt it or start the CLI for a prompt",
            path.display()
        )
    })
}

async fn resolve(host: &str, port: u16) -> Result<std::net::SocketAddr> {
    lookup_host((host, port))
        .await
        .with_context(|| format!("could not resolve {host}:{port}"))?
        .next()
        .with_context(|| format!("{host}:{port} did not resolve to an address"))
}

fn progress_channel() -> (ProgressSender, tokio::task::JoinHandle<()>) {
    let (progress, mut events): (ProgressSender, _) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                ProgressEvent::Scan { files, bytes } => {
                    println!("scanned {files} files, {bytes} bytes");
                }
                ProgressEvent::Sending(update) | ProgressEvent::Receiving(update) => println!(
                    "moved {}/{} bytes, {}/{} files",
                    update.bytes_done, update.bytes_total, update.files_done, update.files_total
                ),
                ProgressEvent::Notice(message) => println!("{message}"),
                ProgressEvent::Complete { files, bytes } => {
                    println!("transfer complete: {files} files, {bytes} bytes");
                }
            }
        }
    });
    (progress, task)
}

fn report_push(stats: &PushStats) {
    println!(
        "sent {} files, {} bytes ({} packs, {} chunks, profile {})",
        stats.files, stats.bytes, stats.packs, stats.chunks, stats.effective_profile
    );
}

fn report_pull(stats: &PullStats) {
    println!(
        "received {} files, {} bytes over the wire, {} bytes written (profile {})",
        stats.files, stats.wire_bytes, stats.bytes, stats.effective_profile
    );
}

fn report_error(err: anyhow::Error) -> Result<()> {
    if let Some(unknown) = err.downcast_ref::<UnknownHostKey>() {
        eprintln!("{unknown}");
        eprintln!("fingerprint: {}", unknown.fingerprint);
        eprintln!("re-run with --accept-new to trust this host key");
    }
    Err(err)
}

/// Splits `cftp://host[:port]/path` into its parts. IPv6 literals may be
/// bracketed (`cftp://[::1]:4433/path`).
fn parse_remote(destination: &str) -> Result<(String, u16, String)> {
    let rest = destination
        .strip_prefix("cftp://")
        .with_context(|| format!("destination must be a cftp:// URL: {destination}"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        bail!("destination has no host: {destination}");
    }

    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let (host, tail) = bracketed
            .split_once(']')
            .with_context(|| format!("malformed IPv6 host: {authority}"))?;
        let port = match tail.strip_prefix(':') {
            Some(port) => port.parse().context("invalid port")?,
            None if tail.is_empty() => 4433,
            None => bail!("unexpected text after IPv6 host: {tail}"),
        };
        (host.to_string(), port)
    } else if authority.matches(':').count() == 1 {
        let (host, port) = authority.rsplit_once(':').expect("one colon");
        (host.to_string(), port.parse().context("invalid port")?)
    } else {
        // No port, or an unbracketed IPv6 literal (which has several colons).
        (authority.to_string(), 4433)
    };

    Ok((host, port, path.to_string()))
}
