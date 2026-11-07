use super::*;

pub(crate) async fn put(args: PutArgs) -> Result<()> {
    let via: Option<ViaSsh> = args.via_ssh.as_deref().map(parse_via_ssh).transpose()?;
    if via.is_some() && matches!(args.auth, AuthMode::Cert) {
        bail!("--via-ssh works with SSH key authentication only");
    }
    match parse_destination(&args.dest, via.as_ref())? {
        Destination::Local(destination) => {
            let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
                progress_channel(args.quiet);
            let result = copy_local(&args.src, &destination, Some(progress)).await;
            let _ = progress_task.await;
            let stats: LocalCopyStats = result?;
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
            port: initial_port,
            destination,
        } => {
            let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
                progress_channel(args.quiet);

            let use_cert: bool = matches!(args.auth, AuthMode::Cert)
                || (matches!(args.auth, AuthMode::Auto)
                    && (args.identity.is_some() || args.key.is_some() || args.server_ca.is_some()));

            let mut port: u16 = initial_port;
            let mut bootstrap: Option<SshBootstrap> = None;
            let auth: ClientAuth = if use_cert {
                let defaults = IdentityPaths::user()?;
                let identity_path = args.identity.unwrap_or(defaults.cert);
                let key_path = args.key.unwrap_or(defaults.key);
                let ca_path = args.server_ca.unwrap_or(defaults.ca);
                let identity = Identity::load(&identity_path, &key_path, &ca_path)?;
                ClientAuth::Cert(identity)
            } else {
                let paths = SshPaths::user()?;
                let key_path = match args.ssh_key {
                    Some(path) => path,
                    None => paths.existing_client_key().with_context(|| {
                        format!(
                            "no SSH private key found in {}; create one with `cftp keygen` or pass --ssh-key",
                            paths.ssh_dir.display()
                        )
                    })?,
                };
                let known_hosts_path = args.known_hosts.unwrap_or(paths.known_hosts);
                let key = load_private_key(&key_path, prompt_passphrase)?;
                if let Some(via) = &via {
                    let public = key.public_openssh()?;
                    let guard = start_ssh_bootstrap(via, &public, &known_hosts_path).await?;
                    port = guard.port;
                    bootstrap = Some(guard);
                }
                let known_hosts = KnownHostsStore::load(&known_hosts_path)?;
                let policy = if via.is_some() {
                    // The key was just learned over the authenticated SSH
                    // channel, so accept it without another prompt.
                    HostKeyPolicy::Strict
                } else if args.accept_new {
                    HostKeyPolicy::AcceptNew
                } else {
                    HostKeyPolicy::Ask
                };
                ClientAuth::Ssh(SshClientAuth::new(
                    key,
                    known_hosts,
                    host.clone(),
                    port,
                    policy,
                ))
            };

            let _ = progress.send(ProgressEvent::Notice(format!("resolving {host}:{port}")));
            let mut resolved = match tokio::time::timeout(
                Duration::from_secs(30),
                lookup_host((host.as_str(), port)),
            )
            .await
            {
                Err(_) => bail!("timed out after 30s resolving {host}:{port}"),
                Ok(result) => result.with_context(|| format!("failed to resolve {host}:{port}"))?,
            };
            let addr = resolved
                .next()
                .with_context(|| format!("{host}:{port} did not resolve to an address"))?;

            let selected: Option<HardwareProfile> = args.hardware.selected();
            let requested_profile: HardwareProfile = selected.unwrap_or(HardwareProfile::Default);
            let tuning: Tuning = match selected {
                Some(profile) => Tuning::for_profile(profile, None),
                None => Tuning::default().with_overrides(
                    args.chunk_size,
                    args.pack_target,
                    args.pack_max_files,
                    args.small_file_max,
                ),
            };

            let hotkeys: Option<Hotkeys> = Hotkeys::start(requested_profile, args.quiet)?;
            if hotkeys.is_some() && !args.quiet {
                eprintln!("profiles: [l]ight [d]efault [a]ggressive");
            }
            let options: PushOptions = PushOptions {
                destination,
                server_name: args
                    .server_name
                    .clone()
                    .unwrap_or_else(|| cftp_core::identity::DEFAULT_SERVER_NAME.to_string()),
                progress: Some(progress),
                profile: requested_profile,
                tuning,
                profile_flag_used: selected.is_some(),
                force_v2: false,
                profile_requests: hotkeys.as_ref().map(|hotkeys| hotkeys.receiver.clone()),
            };

            let mut prompted: bool = false;
            let result: Result<PushStats> = loop {
                match push(&args.src, addr, &auth, options.clone()).await {
                    Ok(stats) => break Ok(stats),
                    Err(err) => {
                        if let Some(unknown) = err.downcast_ref::<UnknownHostKey>()
                            && !prompted
                            && !args.accept_new
                        {
                            prompted = true;
                            if confirm_unknown_host(unknown)? {
                                if let ClientAuth::Ssh(auth) = &auth {
                                    auth.accept_host_key(&unknown.public_key)?;
                                    println!(
                                        "added host key for {}:{} to {}",
                                        unknown.host,
                                        unknown.port,
                                        auth.known_hosts_path().display()
                                    );
                                }
                                continue;
                            }
                        }
                        break Err(err);
                    }
                }
            };
            drop(options);
            drop(hotkeys);
            let _ = progress_task.await;
            let stats: PushStats = result?;
            if let Some(bootstrap) = bootstrap.take()
                && let Err(err) = bootstrap.finish().await
            {
                eprintln!("warning: {err:#}");
            }
            println!(
                "sent {} files, {} bytes ({} packs, {} chunks, profile {})",
                stats.files, stats.bytes, stats.packs, stats.chunks, stats.effective_profile
            );
            if args.stats {
                print_transport(&stats.transport);
            }
            Ok(())
        }
    }
}

/// Prints one stable, greppable line of transport counters.
pub(super) fn print_transport(stats: &TransportStats) {
    println!(
        "stats: rtt_ms={:.2} min_rtt_ms={:.2} cwnd={} mtu={} sent_packets={} lost_packets={} \
         congestion_events={} udp_tx_datagrams={} udp_rx_datagrams={} udp_tx_bytes={} \
         udp_rx_bytes={}",
        stats.rtt_micros as f64 / 1000.0,
        stats.min_rtt_micros as f64 / 1000.0,
        stats.cwnd,
        stats.mtu,
        stats.sent_packets,
        stats.lost_packets,
        stats.congestion_events,
        stats.udp_tx_datagrams,
        stats.udp_rx_datagrams,
        stats.udp_tx_bytes,
        stats.udp_rx_bytes,
    );
}
