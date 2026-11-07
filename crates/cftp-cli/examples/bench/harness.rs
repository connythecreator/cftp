use super::*;

pub(super) static LIVE_CHILDREN: Mutex<Vec<u32>> = Mutex::new(Vec::new());
pub(super) fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        for pid in registered_children() {
            #[cfg(unix)]
            {
                let _ = Command::new("kill")
                    .arg("-9")
                    .arg(pid.to_string())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            #[cfg(not(unix))]
            {
                let _ = pid;
            }
        }
        previous(info);
    }));
}

pub(super) fn registered_children() -> Vec<u32> {
    match LIVE_CHILDREN.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

pub(super) fn register_child(pid: u32) {
    let mut guard = match LIVE_CHILDREN.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.push(pid);
}

pub(super) fn unregister_child(pid: u32) {
    let mut guard = match LIVE_CHILDREN.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.retain(|registered| *registered != pid);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub(super) struct ChildGuard {
    pub(super) child: Child,
    pub(super) status: Option<ExitStatus>,
    pub(super) label: String,
    pub(super) registered: bool,
}

impl ChildGuard {
    pub(super) fn spawn(mut command: Command, label: &str) -> Result<Self, String> {
        let child = command
            .spawn()
            .map_err(|err| format!("failed to start {label}: {err}"))?;
        let pid = child.id();
        register_child(pid);
        Ok(Self {
            child,
            status: None,
            label: label.to_string(),
            registered: true,
        })
    }

    pub(super) fn try_wait_status(&mut self) -> io::Result<Option<ExitStatus>> {
        if self.status.is_some() {
            return Ok(self.status);
        }
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.status = status;
            self.unregister();
        }
        Ok(status)
    }

    pub(super) fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self
                .try_wait_status()
                .map_err(|err| format!("failed to poll {}: {err}", self.label))?
            {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{} did not exit within {timeout:?}; killing it",
                    self.label
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    pub(super) fn kill_now(&mut self) {
        if self.status.is_none() {
            let _ = self.child.kill();
            if let Ok(status) = self.child.wait() {
                self.status = Some(status);
            }
            self.unregister();
        }
    }

    pub(super) fn unregister(&mut self) {
        if self.registered {
            unregister_child(self.child.id());
            self.registered = false;
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_now();
    }
}

/// A server whose output is drained continuously so it cannot block on a full
/// pipe, and which signals readiness when a line contains `listening on`.
pub(super) struct ServerProcess {
    pub(super) guard: ChildGuard,
    pub(super) ready: mpsc::Receiver<()>,
    pub(super) stdout: Arc<Mutex<String>>,
    pub(super) stderr: Arc<Mutex<String>>,
    pub(super) readers: Vec<JoinHandle<()>>,
}

impl ServerProcess {
    pub(super) fn spawn(mut command: Command, label: &str) -> Result<Self, String> {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|err| format!("failed to start {label}: {err}"))?;
        let pid = child.id();
        register_child(pid);
        let stdout_pipe = child
            .stdout
            .take()
            .ok_or_else(|| format!("failed to capture {label} stdout"))?;
        let stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| format!("failed to capture {label} stderr"))?;

        let stdout = Arc::new(Mutex::new(String::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let (ready_tx, ready_rx) = mpsc::channel();
        let readers = vec![
            spawn_line_reader(stdout_pipe, stdout.clone(), Some(ready_tx), "listening on"),
            spawn_line_reader(stderr_pipe, stderr.clone(), None, ""),
        ];

        Ok(Self {
            guard: ChildGuard {
                child,
                status: None,
                label: label.to_string(),
                registered: true,
            },
            ready: ready_rx,
            stdout,
            stderr,
            readers,
        })
    }

    pub(super) fn wait_ready(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = self
                .guard
                .try_wait_status()
                .map_err(|err| format!("failed to poll {}: {err}", self.guard.label))?
            {
                return Err(format!(
                    "{} exited before becoming ready ({status}):\n{}",
                    self.guard.label,
                    self.stderr_snapshot()
                ));
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "{} did not become ready within {READY_TIMEOUT:?}:\n{}",
                    self.guard.label,
                    self.stderr_snapshot()
                ));
            }

            match self
                .ready
                .recv_timeout(remaining.min(Duration::from_millis(50)))
            {
                Ok(()) => return Ok(()),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(format!(
                        "{} exited before becoming ready:\n{}",
                        self.guard.label,
                        self.stderr_snapshot()
                    ));
                }
            }
        }
    }

    pub(super) fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let status = self.guard.wait_for_exit(timeout)?;
        self.join_readers();
        Ok(status)
    }

    pub(super) fn kill_now(&mut self) {
        self.guard.kill_now();
        self.join_readers();
    }

    pub(super) fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }

    pub(super) fn stdout_snapshot(&self) -> String {
        lock_snapshot(&self.stdout)
    }

    pub(super) fn stderr_snapshot(&self) -> String {
        lock_snapshot(&self.stderr)
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.guard.kill_now();
    }
}

pub(super) fn spawn_line_reader<R>(
    pipe: R,
    buffer: Arc<Mutex<String>>,
    ready: Option<mpsc::Sender<()>>,
    marker: &'static str,
) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        let mut line = String::new();
        let mut ready = ready;
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let mut text = match buffer.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if text.len() < 64 * 1024 {
                        text.push_str(&line);
                    }
                    drop(text);

                    if !marker.is_empty()
                        && line.contains(marker)
                        && let Some(sender) = ready.take()
                    {
                        let _ = sender.send(());
                    }
                }
                Err(_) => break,
            }
        }
    })
}

pub(super) fn lock_snapshot(buffer: &Arc<Mutex<String>>) -> String {
    match buffer.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

pub(super) struct RunResult {
    pub(super) status: ExitStatus,
    pub(super) elapsed: Duration,
    pub(super) stdout: String,
    pub(super) stderr: String,
}

/// Run a command with piped output, enforcing a wall-clock timeout.
pub(super) fn run_timed(
    label: &str,
    mut command: Command,
    timeout: Duration,
) -> Result<RunResult, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let start = Instant::now();
    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to start {label}: {err}"))?;
    let stdout = child.stdout.take().map(spawn_pipe_reader);
    let stderr = child.stderr.take().map(spawn_pipe_reader);
    let deadline = start + timeout;

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _stdout = stdout.map(join_reader).unwrap_or_default();
                    let stderr = stderr.map(join_reader).unwrap_or_default();
                    return Err(format!(
                        "{label} timed out after {timeout:?}: {}",
                        brief(&stderr)
                    ));
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("failed to wait for {label}: {err}"));
            }
        }
    };

    let elapsed = start.elapsed();
    let stdout = stdout.map(join_reader).unwrap_or_default();
    let stderr = stderr.map(join_reader).unwrap_or_default();
    Ok(RunResult {
        status,
        elapsed,
        stdout,
        stderr,
    })
}

pub(super) fn spawn_pipe_reader<R>(pipe: R) -> JoinHandle<String>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(pipe).read_to_string(&mut text);
        text
    })
}

pub(super) fn join_reader(reader: JoinHandle<String>) -> String {
    reader.join().unwrap_or_default()
}

/// Flushes dirty pages before a dataset is measured.
///
/// Dataset generation leaves more than a gigabyte queued in the page cache on
/// the huge run. The first tool measured afterwards absorbs the resulting
/// writeback, while later peers start from a clean cache. Flushing before the
/// first measurement gives every peer the same starting state.
pub(super) fn flush_writes() {
    #[cfg(unix)]
    {
        let _ = Command::new("sync").status();
    }
}

pub(super) fn brief(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "(no output)".to_string();
    }
    let mut chars: Vec<char> = trimmed.chars().collect();
    if chars.len() > 800 {
        chars = chars.split_off(chars.len() - 800);
    }
    chars.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Ports and tool discovery
// ---------------------------------------------------------------------------

pub(super) fn free_udp_port() -> Result<u16, String> {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .map_err(|err| format!("failed to reserve a UDP port: {err}"))?;
    socket
        .local_addr()
        .map(|address| address.port())
        .map_err(|err| format!("failed to read the reserved UDP port: {err}"))
}

pub(super) fn free_tcp_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("failed to reserve a TCP port: {err}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|err| format!("failed to read the reserved TCP port: {err}"))
}

pub(super) fn which(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(name);
        if is_executable(&candidate) {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let candidate = directory.join(format!("{name}.exe"));
            if is_executable(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

pub(super) fn is_executable(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

pub(super) fn cftp_binary() -> Result<PathBuf, String> {
    if let Some(path) = option_env!("CARGO_BIN_EXE_cftp") {
        let path = PathBuf::from(path);
        if is_executable(&path) {
            return Ok(path);
        }
    }

    let mut candidates = Vec::new();
    if let Ok(executable) = env::current_exe() {
        // target/<profile>/examples/bench -> target/<profile>/cftp
        if let Some(profile_dir) = executable.parent().and_then(Path::parent) {
            candidates.push(profile_dir.join("cftp"));
        }
    }
    if let Some(target_dir) = env::var_os("CARGO_TARGET_DIR") {
        let target_dir = PathBuf::from(target_dir);
        candidates.push(target_dir.join("release").join("cftp"));
        candidates.push(target_dir.join("debug").join("cftp"));
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Some(workspace_dir) = manifest_dir.parent().and_then(Path::parent) {
        candidates.push(workspace_dir.join("target").join("release").join("cftp"));
        candidates.push(workspace_dir.join("target").join("debug").join("cftp"));
    }

    for candidate in candidates {
        if is_executable(&candidate) {
            return Ok(candidate);
        }
    }

    Err(
        "could not locate the cftp binary; run `cargo build --release -p cftp-cli --bin cftp` \
         or use `make bench`"
            .to_string(),
    )
}

pub(super) fn supports_one_shot(cftp: &Path) -> bool {
    match Command::new(cftp).args(["serve", "--help"]).output() {
        Ok(output) => {
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&output.stderr));
            text.contains("--one-shot")
        }
        Err(_) => false,
    }
}

// ---------------------------------------------------------------------------
// Measurements
// ---------------------------------------------------------------------------
