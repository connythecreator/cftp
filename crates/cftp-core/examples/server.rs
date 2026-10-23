//! Minimal receiver built on `cftp-core`, without the `cftp` binary.
//!
//! ```sh
//! cargo run -p cftp-core --example server -- ./incoming 0.0.0.0:4433
//! ```
//!
//! The server stays alive and accepts repeated transfers. It loads the host key
//! and `authorized_keys` from `~/.ssh`. Encrypted host keys are rejected because
//! this example has no passphrase prompt; use the CLI for those.

#![forbid(unsafe_code)]

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use cftp_core::auth::load_server_auth;
use cftp_core::progress::{ProgressEvent, ProgressSender};
use cftp_core::ssh::SshPaths;
use cftp_core::transfer::{ServeOptions, ServeStats, serve};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let root: PathBuf = args
        .next()
        .context("usage: server <root> [listen-addr]")?
        .into();
    let listen: SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:4433".to_string())
        .parse()
        .context("listen address must be host:port")?;

    let ssh: SshPaths = SshPaths::user()?;
    let auth = load_server_auth(&ssh.host_key, &ssh.authorized_keys, |path| {
        bail!(
            "host key {} is encrypted; decrypt it or start the CLI for a prompt",
            path.display()
        )
    })?;

    let (progress, mut events): (ProgressSender, _) = tokio::sync::mpsc::unbounded_channel();
    let progress_task = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                ProgressEvent::Notice(message) => println!("{message}"),
                ProgressEvent::Receiving(update) => println!(
                    "receiving {}/{} bytes, {}/{} files",
                    update.bytes_done, update.bytes_total, update.files_done, update.files_total
                ),
                ProgressEvent::Complete { files, bytes } => {
                    println!("session complete: {files} files, {bytes} bytes");
                }
                ProgressEvent::Scan { .. } | ProgressEvent::Sending(_) => {}
            }
        }
    });

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (session_tx, mut sessions) = tokio::sync::mpsc::unbounded_channel::<ServeStats>();
    let session_task = tokio::spawn(async move {
        while let Some(stats) = sessions.recv().await {
            println!(
                "received {} files, {} bytes from {}",
                stats.files,
                stats.bytes,
                stats
                    .peer
                    .map(|peer| peer.to_string())
                    .unwrap_or_else(|| "unknown peer".to_string())
            );
        }
    });

    let mut options = ServeOptions::new(root.clone(), listen);
    options.one_shot = false;
    options.progress = Some(progress);
    options.on_ready = Some(ready_tx);
    options.on_session = Some(session_tx);

    // Run in a task so the bound address can be printed once the socket exists.
    let server = tokio::spawn(async move { serve(&auth, options).await });
    match tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx).await {
        Ok(Ok(addr)) => println!("listening on {addr} (root {})", root.display()),
        _ => bail!("server did not become ready"),
    }

    let result = server.await.context("server task failed")?;
    progress_task.abort();
    session_task.abort();
    let stats = result?;
    println!(
        "server stopped after {} files, {} bytes",
        stats.files, stats.bytes
    );
    Ok(())
}
