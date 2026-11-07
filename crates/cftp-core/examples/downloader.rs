//! A small download manager built on `cftp-core`.
//!
//! This example is the shape a browser download manager or a chat client's
//! attachment cache can use: a queue of remote paths, a bounded number of
//! concurrent pulls, progress events per job, and a cancel signal that aborts
//! in-flight transfers.
//!
//! ```sh
//! cargo run -p cftp-core --example downloader -- \
//!     --dest ./downloads --jobs 4 \
//!     cftp://receiver:4433/media/movie.mkv \
//!     cftp://receiver:4433/media/album/
//! ```
//!
//! Add `--accept-new` to trust an unknown host key, or pin it in
//! `~/.config/cftp/known_hosts` first. `--cancel-after SECS` simulates the user
//! pressing cancel: every in-flight `pull` future is dropped and its connection
//! closes.
//!
//! Each job opens its own QUIC connection. Keep `--jobs` at or below the
//! server's `max_sessions` (four by default); extra clients wait for a slot.

#![forbid(unsafe_code)]

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use cftp_core::auth::{ClientAuth, HostKeyPolicy, load_client_auth};
use cftp_core::prelude::*;
use cftp_core::progress::{ProgressEvent, channel};
use cftp_core::ssh::SshPaths;
use cftp_core::trust::KnownHostsStore;
use tokio::net::lookup_host;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

const USAGE: &str = "usage: downloader --dest <dir> [--jobs N] [--accept-new] \
                     [--cancel-after SECS] <cftp://host[:port]/path>...";

struct Args {
    destination: PathBuf,
    urls: Vec<String>,
    jobs: usize,
    accept_new: bool,
    cancel_after: Option<u64>,
}

enum Outcome {
    Finished(Result<PullStats>),
    Cancelled,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let permits = Arc::new(Semaphore::new(args.jobs.max(1)));
    let (cancel_tx, cancel_rx) = watch::channel(false);

    if let Some(seconds) = args.cancel_after {
        let cancel_tx = cancel_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            eprintln!("cancel-after: stopping in-flight downloads");
            let _ = cancel_tx.send(true);
        });
    }

    let mut tasks = JoinSet::new();
    for url in args.urls {
        let (host, port, source) = parse_remote(&url)?;
        let addr = resolve(&host, port).await?;
        let destination = args.destination.clone();
        let permits = permits.clone();
        let mut cancel = cancel_rx.clone();
        let accept_new = args.accept_new;

        tasks.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .context("download queue closed")?;
            let auth = client_auth(&host, port, accept_new)?;
            let (progress, mut events) = channel();
            let label = url.clone();
            let printer = tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    match event {
                        ProgressEvent::Notice(message) => eprintln!("[{label}] {message}"),
                        ProgressEvent::Complete { files, bytes } => {
                            eprintln!("[{label}] complete: {files} files, {bytes} bytes")
                        }
                        _ => {}
                    }
                }
            });

            let options = PullOptions::new(source, destination).with_progress(progress);
            let outcome = tokio::select! {
                result = pull(addr, &auth, options) => Outcome::Finished(result),
                _ = cancel.changed() => Outcome::Cancelled,
            };

            // `pull` dropped the sender, so the printer stops on its own.
            let _ = printer.await;
            match outcome {
                Outcome::Finished(Ok(stats)) => {
                    eprintln!(
                        "[{url}] stored {} files, {} bytes (profile {})",
                        stats.files, stats.bytes, stats.effective_profile
                    );
                    Ok(())
                }
                Outcome::Finished(Err(err)) => Err(err.context(format!("download {url} failed"))),
                Outcome::Cancelled => {
                    eprintln!("[{url}] cancelled");
                    bail!("cancelled")
                }
            }
        });
    }

    let mut failures = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                failures += 1;
                eprintln!("{err:#}");
            }
            Err(err) => {
                failures += 1;
                eprintln!("download task failed: {err}");
            }
        }
    }

    if failures > 0 {
        bail!("{failures} download(s) failed");
    }
    println!("all downloads finished into {}", args.destination.display());
    Ok(())
}

fn parse_args() -> Result<Args> {
    let mut destination: Option<PathBuf> = None;
    let mut jobs = 4_usize;
    let mut accept_new = false;
    let mut cancel_after = None;
    let mut urls = Vec::new();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dest" => {
                destination = Some(PathBuf::from(args.next().context(USAGE)?));
            }
            "--jobs" => {
                jobs = args
                    .next()
                    .context(USAGE)?
                    .parse()
                    .context("invalid --jobs")?;
            }
            "--cancel-after" => {
                cancel_after = Some(
                    args.next()
                        .context(USAGE)?
                        .parse()
                        .context("invalid --cancel-after")?,
                );
            }
            "--accept-new" => accept_new = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other if other.starts_with("--") => bail!("unknown option {other}\n{USAGE}"),
            url => urls.push(url.to_string()),
        }
    }

    if urls.is_empty() {
        bail!("{USAGE}");
    }
    let destination = destination.context("--dest is required")?;

    Ok(Args {
        destination,
        urls,
        jobs,
        accept_new,
        cancel_after,
    })
}

fn client_auth(host: &str, port: u16, accept_new: bool) -> Result<ClientAuth> {
    let ssh = SshPaths::user()?;
    let key = ssh
        .existing_client_key()
        .with_context(|| format!("no SSH key found in {}", ssh.ssh_dir.display()))?;
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

async fn resolve(host: &str, port: u16) -> Result<SocketAddr> {
    lookup_host((host, port))
        .await
        .with_context(|| format!("could not resolve {host}:{port}"))?
        .next()
        .with_context(|| format!("{host}:{port} did not resolve to an address"))
}

/// Splits `cftp://host[:port]/path` into its parts. IPv6 literals may be
/// bracketed (`cftp://[::1]:4433/path`).
fn parse_remote(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("cftp://")
        .with_context(|| format!("not a cftp:// URL: {url}"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        bail!("URL has no host: {url}");
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
        (authority.to_string(), 4433)
    };

    Ok((host, port, path.to_string()))
}
