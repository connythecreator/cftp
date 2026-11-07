//! Listening endpoint and per-session accept loop.

use super::*;

/// Maximum number of sessions a persistent server runs at once. Further
/// connections wait for a slot instead of being accepted, which bounds the
/// memory and file descriptors a burst of clients can consume.
pub const MAX_CONCURRENT_SESSIONS: usize = 4;

/// Receives one [`ServeStats`] value per completed session.
pub type SessionReporter = mpsc::UnboundedSender<ServeStats>;

/// Server-side mode policy.
///
/// The server is authoritative: a disabled mode is rejected with a message and
/// the choice is reported to clients in the `cftp/4` server hello.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServePolicy {
    pub allow_push: bool,
    pub allow_pull: bool,
}

impl Default for ServePolicy {
    fn default() -> Self {
        Self {
            allow_push: true,
            allow_pull: true,
        }
    }
}

impl ServePolicy {
    pub fn push_only() -> Self {
        Self {
            allow_push: true,
            allow_pull: false,
        }
    }

    pub fn pull_only() -> Self {
        Self {
            allow_push: false,
            allow_pull: true,
        }
    }
}

#[derive(Debug)]
pub struct ServeOptions {
    pub root: PathBuf,
    pub listen: SocketAddr,
    pub progress: Option<ProgressSender>,
    pub on_ready: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
    pub max_profile: HardwareProfile,
    pub force_v2: bool,
    /// Which transfer modes this server accepts.
    pub policy: ServePolicy,
    /// Serve exactly one successful session and then close the endpoint.
    ///
    /// Defaults to `true` so existing library callers keep the old one-shot
    /// behaviour. The CLI opts into a persistent server.
    pub one_shot: bool,
    /// Upper bound on concurrent sessions when `one_shot` is false.
    pub max_sessions: usize,
    /// Per-session results for a persistent server.
    pub on_session: Option<SessionReporter>,
}

impl ServeOptions {
    pub fn new(root: impl Into<PathBuf>, listen: SocketAddr) -> Self {
        Self {
            root: root.into(),
            listen,
            progress: None,
            on_ready: None,
            max_profile: HardwareProfile::Default,
            force_v2: false,
            policy: ServePolicy::default(),
            one_shot: true,
            max_sessions: MAX_CONCURRENT_SESSIONS,
            on_session: None,
        }
    }
}

/// Counters from one completed server-side session.
#[derive(Debug, Clone, Default)]
pub struct ServeStats {
    pub files: u64,
    pub bytes: u64,
    pub peer: Option<SocketAddr>,
    pub effective_profile: HardwareProfile,
}

/// Serves transfers until the endpoint closes.
///
/// With `one_shot`, this returns after the first successful session, which is
/// the historical behaviour. Otherwise it keeps accepting sessions, running at
/// most `max_sessions` of them concurrently; further clients wait for a slot.
pub async fn serve(auth: &ServerAuth, mut opts: ServeOptions) -> Result<ServeStats> {
    let root: PathBuf = opts.root.clone();
    let meta: fs::Metadata = fs::metadata(&root)
        .with_context(|| format!("failed to inspect root {}", root.display()))?;
    if !meta.is_dir() {
        bail!("root is not a directory: {}", root.display());
    }
    if !opts.policy.allow_push && !opts.policy.allow_pull {
        bail!("server policy disables both push and pull");
    }

    let current_profile: HardwareProfile = opts.max_profile.min(HardwareProfile::Default);
    let initial_tuning: Tuning = Tuning::for_profile(current_profile, None);
    let mtu: Mtu = Mtu::for_listen(opts.listen);
    let endpoint: Endpoint = bind_endpoint(
        opts.listen,
        Some(auth.server_config(&initial_tuning, opts.force_v2, mtu)?),
        &initial_tuning,
        mtu,
    )?;
    let local_addr: SocketAddr = endpoint.local_addr()?;
    if let Some(on_ready) = opts.on_ready.take() {
        let _ = on_ready.send(local_addr);
    }

    if opts.one_shot {
        let connection: Connection = accept_connection(&endpoint).await?;
        let result: Result<ServeStats> = serve_connection(
            &connection,
            &root,
            opts.progress,
            opts.max_profile,
            current_profile,
            opts.force_v2,
            opts.policy,
        )
        .await;
        let _ = tokio::time::timeout(Duration::from_secs(1), endpoint.wait_idle()).await;
        endpoint.close(0u32.into(), b"done");
        return result;
    }

    let max_sessions: usize = opts.max_sessions.clamp(1, MAX_SESSION_SLOTS);
    tracing::info!(
        "serving on {local_addr} (root {}, up to {max_sessions} sessions)",
        root.display(),
    );
    let slots: Arc<Semaphore> = Arc::new(Semaphore::new(max_sessions));
    let mut sessions: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        let incoming: Option<quinn::Incoming> = tokio::select! {
            incoming = endpoint.accept() => incoming,
            Some(finished) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(err) = finished {
                    tracing::warn!("session task failed: {err}");
                }
                continue;
            }
        };
        let Some(incoming) = incoming else {
            break;
        };
        // Skip failed handshakes instead of exiting: an unauthenticated client
        // (or a QUIC scanner) must not take the server down. The timeout stops a
        // peer that opens a connection and then stalls from blocking the accept
        // loop forever.
        let connection: Connection = match tokio::time::timeout(HANDSHAKE_TIMEOUT, incoming).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(err)) => {
                tracing::warn!("ignoring a failed client handshake: {err}");
                continue;
            }
            Err(_) => {
                tracing::warn!(
                    "dropping a client that did not finish the handshake within {}s",
                    HANDSHAKE_TIMEOUT.as_secs()
                );
                continue;
            }
        };
        // Backpressure: stop accepting while every slot is busy. Permits are
        // released when a session task ends, even if it panics.
        let Ok(permit) = slots.clone().acquire_owned().await else {
            break;
        };
        let root: PathBuf = root.clone();
        let progress: Option<ProgressSender> = opts.progress.clone();
        let reporter: Option<SessionReporter> = opts.on_session.clone();
        let max_profile: HardwareProfile = opts.max_profile;
        let force_v2: bool = opts.force_v2;
        let policy: ServePolicy = opts.policy;
        sessions.spawn(async move {
            let _permit: tokio::sync::OwnedSemaphorePermit = permit;
            match serve_connection(
                &connection,
                &root,
                progress,
                max_profile,
                current_profile,
                force_v2,
                policy,
            )
            .await
            {
                Ok(stats) => {
                    if let Some(reporter) = reporter {
                        let _ = reporter.send(stats);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "session from {} failed: {err:#}",
                        connection.remote_address()
                    );
                }
            }
        });
    }

    while let Some(finished) = sessions.join_next().await {
        if let Err(err) = finished {
            tracing::warn!("session task failed: {err}");
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), endpoint.wait_idle()).await;
    endpoint.close(0u32.into(), b"done");
    Ok(ServeStats::default())
}

/// Serves exactly one successful session and then closes the endpoint.
pub async fn serve_once(auth: &ServerAuth, mut opts: ServeOptions) -> Result<ServeStats> {
    opts.one_shot = true;
    serve(auth, opts).await
}

pub(super) async fn accept_connection(endpoint: &Endpoint) -> Result<Connection> {
    loop {
        let incoming: quinn::Incoming = endpoint
            .accept()
            .await
            .context("listener closed before a client connected")?;
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, incoming).await {
            Ok(Ok(connection)) => return Ok(connection),
            Ok(Err(err)) => {
                tracing::warn!("ignoring a failed client handshake: {err}");
            }
            Err(_) => {
                tracing::warn!(
                    "dropping a client that did not finish the handshake within {}s",
                    HANDSHAKE_TIMEOUT.as_secs()
                );
            }
        }
    }
}

async fn serve_connection(
    connection: &Connection,
    root: &Path,
    progress: Option<ProgressSender>,
    max_profile: HardwareProfile,
    initial_profile: HardwareProfile,
    force_v2: bool,
    policy: ServePolicy,
) -> Result<ServeStats> {
    let peer: SocketAddr = connection.remote_address();
    let wire: WireVersion = if force_v2 {
        WireVersion::V2
    } else {
        negotiated_version(connection)
    };

    let (control_send, control_recv): (SendStream, RecvStream) =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.accept_bi()).await {
            Err(_) => bail!(
                "client did not open a control stream within {}s",
                HANDSHAKE_TIMEOUT.as_secs()
            ),
            Ok(result) => result.context("client did not open a control stream")?,
        };

    let result: Result<ServeStats> = receive_connection(
        connection.clone(),
        control_send,
        control_recv,
        root.to_path_buf(),
        progress,
        peer,
        wire,
        max_profile,
        initial_profile,
        policy,
    )
    .await;

    // Give the peer a moment to collect the final control frames before the
    // connection is torn down; an unresponsive peer costs at most a second.
    let _ = tokio::time::timeout(Duration::from_secs(1), connection.closed()).await;
    connection.close(0u32.into(), b"done");
    result
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn send_server_error(
    control_send: &mut SendStream,
    wire: WireVersion,
    err: &anyhow::Error,
) {
    let _ = tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        write_server_error(
            control_send,
            wire,
            Abort {
                message: err.to_string(),
            },
        ),
    )
    .await;
    let _ = control_send.finish();
}
