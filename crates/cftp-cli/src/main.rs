#![forbid(unsafe_code)]

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cftp_cli::cli::{
    AuthMode, Cli, Command, GetArgs, IdentityCommand, IdentityInitArgs, PutArgs, ServeArgs,
};
use cftp_cli::hotkeys::Hotkeys;
use cftp_core::auth::{
    ClientAuth, HostKeyPolicy, ServerAuth, SshClientAuth, SshServerAuth, UnknownHostKey,
};
use cftp_core::identity::{Identity, IdentityPaths, init_identity};
use cftp_core::local::{LocalCopyStats, copy_local};
use cftp_core::progress::{ProgressEvent, ProgressSender, TransferProgress};
use cftp_core::ssh::{SshPaths, load_private_key};
use cftp_core::transfer::{
    PullOptions, PullStats, PushOptions, PushStats, ServeOptions, ServePolicy, ServeStats, pull,
    push, serve,
};
use cftp_core::trust::{AuthorizedKeys, KnownHostsStore};
use cftp_core::tuning::{HardwareProfile, Tuning};
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

    let cli: Cli = Cli::parse();
    match cli.command {
        Command::Identity { command } => match command {
            IdentityCommand::Init(args) => init(args),
        },
        Command::Serve(args) => run_serve(args).await,
        Command::Put(args) => put(args).await,
        Command::Get(args) => get(args).await,
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

async fn run_serve(args: ServeArgs) -> Result<()> {
    let use_cert: bool = matches!(args.auth, AuthMode::Cert)
        || (matches!(args.auth, AuthMode::Auto)
            && (args.identity.is_some() || args.key.is_some() || args.client_ca.is_some()));

    let auth: ServerAuth = if use_cert {
        let defaults = IdentityPaths::user()?;
        let identity_path = args.identity.unwrap_or(defaults.cert);
        let key_path = args.key.unwrap_or(defaults.key);
        let ca_path = args.client_ca.unwrap_or(defaults.ca);
        let identity = Identity::load(&identity_path, &key_path, &ca_path)?;
        println!("client CA SHA-256: {}", identity.ca_fingerprint);
        ServerAuth::Cert(identity)
    } else {
        let paths = SshPaths::user()?;
        let host_key_path = args.host_key.unwrap_or(paths.host_key);
        let authorized_keys_path = args.authorized_keys.unwrap_or(paths.authorized_keys);
        if !host_key_path.is_file() {
            bail!(
                "no SSH host key found at {}; create one with `ssh-keygen -t ed25519` or pass --host-key",
                host_key_path.display()
            );
        }
        let host_key = load_private_key(&host_key_path, prompt_passphrase)?;
        let authorized_keys = AuthorizedKeys::load(&authorized_keys_path)?;
        println!("host key:           {}", host_key_path.display());
        println!("host public key:    {}", host_key.public_openssh()?);
        println!("host fingerprint:   {}", host_key.fingerprint());
        println!("authorized keys:    {}", authorized_keys_path.display());
        println!(
            "accepted keys:      {}",
            authorized_keys.fingerprints().count()
        );
        ServerAuth::Ssh(SshServerAuth::new(host_key, authorized_keys))
    };

    let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
        progress_channel(args.quiet);
    let max_profile: HardwareProfile = args.hardware.selected().unwrap_or(HardwareProfile::Default);
    println!("hardware profile cap: {max_profile}");
    let policy: ServePolicy = ServePolicy {
        allow_push: !args.no_push,
        allow_pull: !args.no_pull,
    };
    println!(
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
    let one_shot: bool = args.one_shot;
    let max_sessions: usize = args.max_sessions;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (session_tx, mut session_rx) = tokio::sync::mpsc::unbounded_channel::<ServeStats>();
    let reporter = tokio::spawn(async move {
        while let Some(stats) = session_rx.recv().await {
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
        println!("listening on {addr} (root {root_display})");
        if one_shot {
            println!("waiting for one transfer (--one-shot)");
        } else {
            println!("waiting for transfers (up to {max_sessions} at a time)");
        }
    }
    let result: Result<ServeStats> = match server.await {
        Ok(result) => result,
        Err(err) => Err(anyhow::anyhow!("server task panicked: {err}")),
    };
    let _ = progress_task.await;
    let stats: ServeStats = result?;
    if one_shot {
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

async fn put(args: PutArgs) -> Result<()> {
    match parse_destination(&args.dest)? {
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
            port,
            destination,
        } => {
            let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
                progress_channel(args.quiet);
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

            let use_cert: bool = matches!(args.auth, AuthMode::Cert)
                || (matches!(args.auth, AuthMode::Auto)
                    && (args.identity.is_some() || args.key.is_some() || args.server_ca.is_some()));

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
                            "no SSH private key found in {}; create one with `ssh-keygen -t ed25519` or pass --ssh-key",
                            paths.dir.display()
                        )
                    })?,
                };
                let known_hosts_path = args.known_hosts.unwrap_or(paths.known_hosts);
                let known_hosts = KnownHostsStore::load(&known_hosts_path)?;
                let policy = if args.accept_new {
                    HostKeyPolicy::AcceptNew
                } else {
                    HostKeyPolicy::Ask
                };
                let key = load_private_key(&key_path, prompt_passphrase)?;
                ClientAuth::Ssh(SshClientAuth::new(
                    key,
                    known_hosts,
                    host.clone(),
                    port,
                    policy,
                ))
            };

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
            println!(
                "sent {} files, {} bytes ({} packs, {} chunks, profile {})",
                stats.files, stats.bytes, stats.packs, stats.chunks, stats.effective_profile
            );
            Ok(())
        }
    }
}

async fn get(args: GetArgs) -> Result<()> {
    match parse_destination(&args.src)? {
        Destination::Local(source) => {
            let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
                progress_channel(args.quiet);
            let result = copy_local(&source, &args.dest, Some(progress)).await;
            let _ = progress_task.await;
            let stats: LocalCopyStats = result?;
            println!(
                "copied {} files, {} bytes to {}",
                stats.files,
                stats.bytes,
                args.dest.display()
            );
            Ok(())
        }
        Destination::Remote {
            host,
            port,
            destination: source,
        } => {
            let (progress, progress_task): (ProgressSender, tokio::task::JoinHandle<()>) =
                progress_channel(args.quiet);
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

            let use_cert: bool = matches!(args.auth, AuthMode::Cert)
                || (matches!(args.auth, AuthMode::Auto)
                    && (args.identity.is_some() || args.key.is_some() || args.server_ca.is_some()));

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
                            "no SSH private key found in {}; create one with `ssh-keygen -t ed25519` or pass --ssh-key",
                            paths.dir.display()
                        )
                    })?,
                };
                let known_hosts_path = args.known_hosts.unwrap_or(paths.known_hosts);
                let known_hosts = KnownHostsStore::load(&known_hosts_path)?;
                let policy = if args.accept_new {
                    HostKeyPolicy::AcceptNew
                } else {
                    HostKeyPolicy::Ask
                };
                let key = load_private_key(&key_path, prompt_passphrase)?;
                ClientAuth::Ssh(SshClientAuth::new(
                    key,
                    known_hosts,
                    host.clone(),
                    port,
                    policy,
                ))
            };

            let selected: Option<HardwareProfile> = args.hardware.selected();
            let requested_profile: HardwareProfile = selected.unwrap_or(HardwareProfile::Default);
            let tuning: Tuning = match selected {
                Some(profile) => Tuning::for_profile(profile, None),
                None => Tuning::default(),
            };
            let options: PullOptions = PullOptions {
                source,
                destination: args.dest,
                server_name: args
                    .server_name
                    .clone()
                    .unwrap_or_else(|| cftp_core::identity::DEFAULT_SERVER_NAME.to_string()),
                progress: Some(progress),
                profile: requested_profile,
                tuning,
            };

            let mut prompted: bool = false;
            let result: Result<PullStats> = loop {
                match pull(addr, &auth, options.clone()).await {
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
            // The options own a progress sender; drop it so the renderer task
            // can see the channel close and exit.
            drop(options);
            let _ = progress_task.await;
            let stats: PullStats = result?;
            println!(
                "received {} files, {} bytes (profile {})",
                stats.files, stats.bytes, stats.effective_profile
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
        let bar: Option<ProgressBar> = if quiet {
            None
        } else {
            let bar = ProgressBar::new(0);
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg}",
                )
                .unwrap()
                .progress_chars("=>-"),
            );
            // Redraw on a timer so the spinner and elapsed time keep moving even
            // when the peer is between events.
            bar.enable_steady_tick(Duration::from_millis(250));
            Some(bar)
        };
        let mut tracker = RateTracker::new();
        let mut last_message = String::from("0 files");
        let mut last_event = Instant::now();
        let mut last_stall = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                event = rx.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    last_event = Instant::now();
                    last_stall = Instant::now();
                    match event {
                        ProgressEvent::Scan { files, bytes } => {
                            if let Some(bar) = &bar {
                                bar.set_length(bytes);
                            }
                            last_message = format!("{files} files");
                            if let Some(bar) = &bar {
                                bar.set_message(last_message.clone());
                            }
                        }
                        ProgressEvent::Sending(stats) | ProgressEvent::Receiving(stats) => {
                            if let Some(bar) = &bar {
                                bar.set_length(stats.bytes_total);
                                bar.set_position(stats.bytes_done);
                            }
                            let rates = tracker.update(&stats);
                            last_message = format_progress(&stats, &rates);
                            if let Some(bar) = &bar {
                                bar.set_message(last_message.clone());
                            }
                        }
                        ProgressEvent::Complete { .. } => {
                            if let Some(bar) = &bar {
                                bar.finish_with_message(format!("complete | {last_message}"));
                            }
                        }
                        ProgressEvent::Notice(message) => match &bar {
                            Some(bar) => bar.println(message),
                            None => eprintln!("{message}"),
                        },
                    }
                }
                _ = ticker.tick() => {
                    // Event-driven progress freezes completely when a peer goes
                    // quiet; say so instead of leaving a stale display.
                    if last_event.elapsed() >= Duration::from_secs(15)
                        && last_stall.elapsed() >= Duration::from_secs(15)
                    {
                        last_stall = Instant::now();
                        let message = format!(
                            "still working: no progress update for {}s",
                            last_event.elapsed().as_secs()
                        );
                        match &bar {
                            Some(bar) => bar.println(message),
                            None => eprintln!("{message}"),
                        }
                    }
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

fn prompt_passphrase(path: &Path) -> Result<String> {
    if !std::io::stdin().is_terminal() {
        bail!(
            "SSH key {} is encrypted and stdin is not a terminal; decrypt it or use an unencrypted key",
            path.display()
        );
    }
    rpassword::prompt_password(format!("Passphrase for {}: ", path.display()))
        .with_context(|| format!("failed to read passphrase for {}", path.display()))
}

fn confirm_unknown_host(unknown: &UnknownHostKey) -> Result<bool> {
    if !std::io::stdin().is_terminal() {
        bail!(
            "the authenticity of host '{}:{}' can't be established ({}); add the key to known_hosts or pass --accept-new",
            unknown.host,
            unknown.port,
            unknown.fingerprint
        );
    }

    eprintln!(
        "The authenticity of host '{}:{}' can't be established.",
        unknown.host, unknown.port
    );
    eprintln!("Key fingerprint is {}.", unknown.fingerprint);
    eprint!("Are you sure you want to continue connecting (yes/no)? ");
    use std::io::Write as _;
    std::io::stderr().flush()?;

    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "yes" | "y"
    ))
}
