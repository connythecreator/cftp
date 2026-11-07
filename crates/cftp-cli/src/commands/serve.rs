use super::*;

pub(crate) async fn run_serve(args: ServeArgs) -> Result<()> {
    let bootstrap: bool = args.bootstrap;
    // In bootstrap mode stdout is a machine-readable channel (`put/get
    // --via-ssh` parses it), so every human-facing line goes to stderr instead.
    macro_rules! info {
        ($($arg:tt)*) => {
            if bootstrap {
                eprintln!($($arg)*);
            } else {
                println!($($arg)*);
            }
        };
    }

    // Accept an SSH-bootstrapped session: the client's public key arrives on
    // stdin, which also keeps it out of the remote process list.
    let bootstrap_key: Option<SshPublicKey> = if bootstrap {
        use std::io::BufRead as _;
        let mut line = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut line)
            .context("failed to read the bootstrap public key from stdin")?;
        Some(
            SshPublicKey::from_openssh(&line)
                .context("stdin did not contain an OpenSSH public key")?,
        )
    } else {
        None
    };

    let use_cert: bool = matches!(args.auth, AuthMode::Cert)
        || (matches!(args.auth, AuthMode::Auto)
            && (args.identity.is_some() || args.key.is_some() || args.client_ca.is_some()));
    if use_cert && bootstrap {
        bail!("--bootstrap requires SSH key authentication");
    }

    // Create the root so a fresh machine can receive files immediately.
    if !args.root.exists() {
        std::fs::create_dir_all(&args.root)
            .with_context(|| format!("failed to create root {}", args.root.display()))?;
    }

    let mut host_public: Option<String> = None;
    let auth: ServerAuth = if use_cert {
        let defaults = IdentityPaths::user()?;
        let identity_path = args.identity.unwrap_or(defaults.cert);
        let key_path = args.key.unwrap_or(defaults.key);
        let ca_path = args.client_ca.unwrap_or(defaults.ca);
        let identity = Identity::load(&identity_path, &key_path, &ca_path)?;
        info!("client CA SHA-256: {}", identity.ca_fingerprint);
        ServerAuth::Cert(identity)
    } else {
        let paths = SshPaths::user()?;
        let host_key_path = args.host_key.unwrap_or(paths.host_key);
        let authorized_keys_path = args.authorized_keys.unwrap_or(paths.authorized_keys);
        // A first `serve` generates its host key automatically, so two fresh
        // installs need no manual key setup on the receiving side.
        let (host_key, created) = load_or_create_host_key(&host_key_path, prompt_passphrase)?;
        if created {
            info!("generated host key:  {}", host_key_path.display());
        }
        let authorized_keys = match bootstrap_key {
            Some(key) => AuthorizedKeys::single(key),
            None => AuthorizedKeys::load(&authorized_keys_path)?,
        };
        let public = host_key.public_openssh()?;
        info!("host key:           {}", host_key_path.display());
        info!("host public key:    {public}");
        info!("host fingerprint:   {}", host_key.fingerprint());
        if bootstrap {
            info!("authorized keys:    one key supplied over ssh");
        } else {
            info!("authorized keys:    {}", authorized_keys_path.display());
            info!(
                "accepted keys:      {}",
                authorized_keys.fingerprints().count()
            );
        }
        host_public = Some(public);
        ServerAuth::Ssh(SshServerAuth::new(host_key, authorized_keys))
    };

    let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
        progress_channel(args.quiet || bootstrap);
    let max_profile: HardwareProfile = args.hardware.selected().unwrap_or(HardwareProfile::Default);
    info!("hardware profile cap: {max_profile}");
    let policy: ServePolicy = ServePolicy {
        allow_push: !args.no_push,
        allow_pull: !args.no_pull,
    };
    info!(
        "policy: {}",
        match (policy.allow_push, policy.allow_pull) {
            (true, true) => "push and pull",
            (true, false) => "push only",
            (false, true) => "pull only",
            (false, false) => "no transfers",
        }
    );
    let root_display: String = args.root.display().to_string();
    let listen: std::net::SocketAddr = args.listen;
    let one_shot: bool = args.one_shot || bootstrap;
    let max_sessions: usize = args.max_sessions;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (session_tx, mut session_rx) = tokio::sync::mpsc::unbounded_channel::<ServeStats>();
    let reporter = tokio::spawn(async move {
        while let Some(stats) = session_rx.recv().await {
            let line = format!(
                "transferred {} files, {} bytes with {} (profile {})",
                stats.files,
                stats.bytes,
                stats
                    .peer
                    .map(|peer| peer.to_string())
                    .unwrap_or_else(|| "unknown peer".to_string()),
                stats.effective_profile
            );
            if bootstrap {
                eprintln!("{line}");
            } else {
                println!("{line}");
            }
        }
    });
    let options: ServeOptions = ServeOptions {
        root: args.root,
        listen,
        progress: Some(progress),
        on_ready: Some(ready_tx),
        max_profile,
        force_v2: false,
        policy,
        one_shot,
        max_sessions: max_sessions.max(1),
        on_session: Some(session_tx),
    };
    // Run the server in a task so the bound address can be reported once the
    // socket actually exists (and so a failed bind is reported as an error).
    let server = tokio::spawn(async move { serve(&auth, options).await });
    if let Ok(addr) = ready_rx.await {
        if bootstrap {
            use std::io::Write as _;
            let public = host_public
                .as_deref()
                .context("bootstrap requires an SSH host key")?;
            println!("CFTP-READY {} {}", addr.port(), public);
            std::io::stdout()
                .flush()
                .context("failed to flush the bootstrap handshake")?;
        } else {
            let port: u16 = addr.port();
            info!("listening on {addr} (root {root_display})");
            let addresses = lan_addresses();
            if addresses.is_empty() {
                info!("other machines can reach this server at cftp://<this-host>:{port}/");
            } else {
                for address in addresses {
                    info!("reachable at cftp://{address}:{port}/");
                }
            }
            info!("from another machine: cftp put <path> cftp://<this-host>:{port}/<destination>");
            if one_shot {
                info!("waiting for one transfer (--one-shot)");
            } else {
                info!("waiting for transfers (up to {max_sessions} at a time)");
            }
        }
    }
    let result: Result<ServeStats> = match server.await {
        Ok(result) => result,
        Err(err) => Err(anyhow::anyhow!("server task panicked: {err}")),
    };
    let _ = progress_task.await;
    let stats: ServeStats = result?;
    if one_shot && !bootstrap {
        println!(
            "transferred {} files, {} bytes with {} (profile {})",
            stats.files,
            stats.bytes,
            stats
                .peer
                .map(|peer| peer.to_string())
                .unwrap_or_else(|| "unknown peer".to_string()),
            stats.effective_profile
        );
    }
    reporter.abort();
    Ok(())
}
