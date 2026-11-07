use super::*;

pub(super) struct Ctx<'a> {
    pub(super) cfg: &'a Config,
    pub(super) cftp: &'a Path,
    pub(super) identity: &'a Path,
    pub(super) work: &'a Path,
    pub(super) one_shot: bool,
}

pub(super) struct Tools {
    pub(super) cp: Option<PathBuf>,
    pub(super) scp: Option<PathBuf>,
    pub(super) sftp: Option<PathBuf>,
    pub(super) curl: Option<PathBuf>,
    pub(super) python: Option<PathBuf>,
    pub(super) scp_enabled: bool,
    pub(super) sftp_enabled: bool,
    pub(super) ftp_enabled: bool,
    pub(super) http_enabled: bool,
    pub(super) http_put_enabled: bool,
    pub(super) https_put_enabled: bool,
}

pub(super) fn detect_tools(cfg: &Config, notes: &mut Notes) -> Tools {
    let cp = which("cp");
    if cp.is_none() {
        notes.push("cp -a: skipped (cp not found on PATH)");
    }

    let scp = which("scp");
    let scp_enabled = if cfg.ssh_target.is_none() {
        notes.push("scp: skipped (no ssh target)");
        false
    } else if scp.is_none() {
        notes.push("scp: skipped (scp not found on PATH)");
        false
    } else {
        true
    };

    let sftp = which("sftp");
    let sftp_enabled = if cfg.sftp_target.is_none() {
        notes.push("sftp: skipped (no sftp target)");
        false
    } else if sftp.is_none() {
        notes.push("sftp: skipped (sftp not found on PATH)");
        false
    } else {
        true
    };

    let curl = which("curl");
    let ftp_enabled = if cfg.ftp_url.is_none() {
        notes.push("ftp: skipped (CFTP_BENCH_FTP_URL not set)");
        false
    } else if curl.is_none() {
        notes.push("ftp: skipped (curl not found on PATH)");
        false
    } else {
        true
    };

    let python = which("python3");
    let http_enabled = if curl.is_none() {
        notes.push("http: skipped (curl not found on PATH)");
        false
    } else if cfg.http_url.is_some() {
        true
    } else if python.is_none() {
        notes.push("http: skipped (python3 not found on PATH and CFTP_BENCH_HTTP_URL not set)");
        false
    } else {
        true
    };

    let http_put_enabled = if cfg.http_put_url.is_none() {
        false
    } else if curl.is_none() {
        notes.push("http upload: skipped (curl not found on PATH)");
        false
    } else {
        true
    };

    let https_put_enabled = if cfg.https_put_url.is_none() {
        false
    } else if curl.is_none() {
        notes.push("https upload: skipped (curl not found on PATH)");
        false
    } else {
        true
    };

    Tools {
        cp,
        scp,
        sftp,
        curl,
        python,
        scp_enabled,
        sftp_enabled,
        ftp_enabled,
        http_enabled,
        http_put_enabled,
        https_put_enabled,
    }
}

pub(super) fn measure_cftp(ctx: &Ctx<'_>, dataset: &Dataset) -> Result<Vec<Duration>, String> {
    let mut times = Vec::with_capacity(ctx.cfg.iterations);
    for iteration in 0..ctx.cfg.iterations {
        let destination = run_dir(ctx, dataset, "cftp", iteration);
        fs::create_dir_all(&destination)
            .map_err(|err| format!("failed to create {}: {err}", destination.display()))?;
        let elapsed = cftp_once(ctx, dataset, &destination)?;
        let name = &dataset.name;
        eprintln!("{name}: cftp put {:.3}s", elapsed.as_secs_f64());
        times.push(elapsed);
        cleanup_path(ctx, &destination);
    }
    Ok(times)
}

pub(super) fn cftp_once(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    destination: &Path,
) -> Result<Duration, String> {
    if let Some(endpoint) = &ctx.cfg.endpoint {
        return cftp_once_remote(ctx, dataset, destination, endpoint);
    }
    let port = free_udp_port()?;
    let listen = format!("127.0.0.1:{port}");

    let mut serve = Command::new(ctx.cftp);
    serve
        .arg("serve")
        .arg("--root")
        .arg(destination)
        .arg("--listen")
        .arg(&listen)
        .arg("--identity")
        .arg(ctx.identity.join("identity.pem"))
        .arg("--key")
        .arg(ctx.identity.join("identity.key"))
        .arg("--client-ca")
        .arg(ctx.identity.join("ca.pem"))
        .arg("--quiet")
        .arg("--aggressive");
    if ctx.one_shot {
        serve.arg("--one-shot");
    }
    let mut server = ServerProcess::spawn(serve, "cftp serve")?;
    server.wait_ready()?;

    let url = format!("cftp://127.0.0.1:{port}/incoming");
    let mut put = Command::new(ctx.cftp);
    put.arg("put")
        .arg(&dataset.root)
        .arg(&url)
        .arg("--identity")
        .arg(ctx.identity.join("identity.pem"))
        .arg("--key")
        .arg(ctx.identity.join("identity.key"))
        .arg("--server-ca")
        .arg(ctx.identity.join("ca.pem"))
        .arg("--quiet")
        .arg("--stats")
        .arg("--aggressive");

    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    let put_result = run_timed("cftp put", put, timeout);
    let run = match put_result {
        Ok(run) => run,
        Err(err) => {
            let server_stderr = server.stderr_snapshot();
            server.kill_now();
            return Err(format!("{err}\nserver stderr: {}", brief(&server_stderr)));
        }
    };

    let server_exit = server.wait_for_exit(SERVER_EXIT_TIMEOUT);
    if !run.status.success() {
        let stdout = server.stdout_snapshot();
        let stderr = server.stderr_snapshot();
        return Err(format!(
            "cftp put failed ({}): client stderr: {}; server stdout: {}; server stderr: {}",
            run.status,
            brief(&run.stderr),
            brief(&stdout),
            brief(&stderr)
        ));
    }
    server_exit?;

    let source_name = dataset
        .root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "dataset path {} has no usable file name",
                dataset.root.display()
            )
        })?;
    let landed = destination.join("incoming").join(source_name);
    verify_tree(&dataset.root, &landed)?;
    report_transport(&dataset.name, &run.stdout);
    Ok(run.elapsed)
}

/// Pushes one dataset to an already-running server named by `--endpoint`.
///
/// The peer cannot be inspected from here, so there is no tree verification and
/// no cleanup of the remote copy.
pub(super) fn cftp_once_remote(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    destination: &Path,
    endpoint: &str,
) -> Result<Duration, String> {
    let run_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("run path {} has no usable name", destination.display()))?;
    let url = format!(
        "cftp://{endpoint}/cftp-bench-{}-{}-{run_name}",
        std::process::id(),
        dataset.name
    );

    let mut put = Command::new(ctx.cftp);
    put.arg("put")
        .arg(&dataset.root)
        .arg(&url)
        .arg("--auth")
        .arg("cert")
        .arg("--identity")
        .arg(ctx.identity.join("identity.pem"))
        .arg("--key")
        .arg(ctx.identity.join("identity.key"))
        .arg("--server-ca")
        .arg(ctx.identity.join("ca.pem"))
        .arg("--quiet")
        .arg("--stats")
        .arg("--aggressive");

    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    let run = run_timed("cftp put", put, timeout)?;
    if !run.status.success() {
        return Err(format!(
            "cftp put to {endpoint} failed ({}): {}",
            run.status,
            brief(&run.stderr)
        ));
    }
    report_transport(&dataset.name, &run.stdout);
    Ok(run.elapsed)
}

/// Echoes the `stats:` line printed by `cftp put --stats`, when present.
pub(super) fn report_transport(dataset: &str, stdout: &str) {
    for line in stdout.lines().filter(|line| line.starts_with("stats: ")) {
        eprintln!("{dataset}: cftp {line}");
    }
}

pub(super) fn measure_copy_dir(ctx: &Ctx<'_>, dataset: &Dataset) -> Result<Vec<Duration>, String> {
    let mut times = Vec::with_capacity(ctx.cfg.iterations);
    for iteration in 0..ctx.cfg.iterations {
        let destination = run_dir(ctx, dataset, "std-copy", iteration);
        fs::create_dir_all(&destination)
            .map_err(|err| format!("failed to create {}: {err}", destination.display()))?;
        let start = Instant::now();
        copy_dir(&dataset.root, &destination).map_err(|err| format!("std copy failed: {err}"))?;
        let elapsed = start.elapsed();
        verify_tree(&dataset.root, &destination)?;
        let name = &dataset.name;
        eprintln!("{name}: std copy {:.3}s", elapsed.as_secs_f64());
        times.push(elapsed);
        cleanup_path(ctx, &destination);
    }
    Ok(times)
}

pub(super) fn measure_cp(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    cp: &Path,
) -> Result<Vec<Duration>, String> {
    let mut times = Vec::with_capacity(ctx.cfg.iterations);
    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    for iteration in 0..ctx.cfg.iterations {
        let destination = run_dir(ctx, dataset, "cp", iteration);
        fs::create_dir_all(&destination)
            .map_err(|err| format!("failed to create {}: {err}", destination.display()))?;

        let mut command = Command::new(cp);
        command.arg("-a").arg(&dataset.root).arg(&destination);
        let result = run_timed("cp -a", command, timeout)?;
        if !result.status.success() {
            return Err(format!("exit {}: {}", result.status, brief(&result.stderr)));
        }

        let source_name = dataset
            .root
            .file_name()
            .ok_or_else(|| format!("dataset path {} has no file name", dataset.root.display()))?;
        verify_tree(&dataset.root, &destination.join(source_name))?;
        times.push(result.elapsed);
        cleanup_path(ctx, &destination);
    }
    Ok(times)
}

pub(super) fn measure_scp(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    scp: &Path,
    target: &str,
) -> Result<Vec<Duration>, String> {
    let mut times = Vec::with_capacity(ctx.cfg.iterations);
    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    let pid = std::process::id();
    let remote_base = ctx.cfg.ssh_dest.trim_end_matches('/');

    for iteration in 0..ctx.cfg.iterations {
        let remote = format!(
            "{remote_base}/cftp-bench-{pid}-{}-{iteration}",
            dataset.name
        );
        let mut command = Command::new(scp);
        command
            .arg("-r")
            .arg("-q")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=10")
            .arg(&dataset.root)
            .arg(format!("{target}:{remote}"));
        let result = run_timed("scp", command, timeout)?;
        if !result.status.success() {
            return Err(format!("exit {}: {}", result.status, brief(&result.stderr)));
        }
        times.push(result.elapsed);
        cleanup_path(ctx, Path::new(&remote));
    }
    Ok(times)
}

/// Uploads a tree with the OpenSSH `sftp` client in batch mode.
///
/// `put -R` copies the directory recursively over a single connection, which
/// is the closest analogue to `scp -r` and to `cftp put`.
pub(super) fn measure_sftp(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    sftp: &Path,
    target: &str,
) -> Result<Vec<Duration>, String> {
    let mut times = Vec::with_capacity(ctx.cfg.iterations);
    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    let pid = std::process::id();
    let remote_base = ctx.cfg.sftp_dest.trim_end_matches('/');

    for iteration in 0..ctx.cfg.iterations {
        let remote = format!(
            "{remote_base}/cftp-bench-{pid}-{}-{iteration}",
            dataset.name
        );
        let batch = ctx
            .work
            .join(format!("sftp-{pid}-{}-{iteration}.batch", dataset.name));
        let contents = format!("put -R {} {remote}\n", dataset.root.display());
        fs::write(&batch, contents)
            .map_err(|err| format!("failed to write {}: {err}", batch.display()))?;
        let mut command = Command::new(sftp);
        command
            .arg("-q")
            .arg("-b")
            .arg(&batch)
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=10")
            .arg(target);
        let result = run_timed("sftp", command, timeout);
        let _ = fs::remove_file(&batch);
        let result = result?;
        if !result.status.success() {
            return Err(format!("exit {}: {}", result.status, brief(&result.stderr)));
        }
        times.push(result.elapsed);
        cleanup_path(ctx, Path::new(&remote));
    }
    Ok(times)
}

pub(super) fn measure_ftp(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    curl: &Path,
    base_url: &str,
) -> Result<Vec<Duration>, String> {
    let files = walk_files(&dataset.root)
        .map_err(|err| format!("failed to walk {}: {err}", dataset.root.display()))?;
    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    let mut times = Vec::with_capacity(ctx.cfg.iterations);

    for _ in 0..ctx.cfg.iterations {
        let mut uploaded: Vec<String> = Vec::with_capacity(files.len());
        let start = Instant::now();
        for (path, _) in &files {
            let relative = path
                .strip_prefix(&dataset.root)
                .map_err(|_| format!("{} is not under the dataset root", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            let url = format!("{}/{}", base_url.trim_end_matches('/'), relative);

            let mut command = Command::new(curl);
            command
                .arg("--silent")
                .arg("--show-error")
                .arg("--fail")
                .arg("--ftp-create-dirs")
                .arg("--connect-timeout")
                .arg("10")
                .arg("--max-time")
                .arg(timeout.as_secs().to_string())
                .arg("-T")
                .arg(path)
                .arg(&url);
            let result = run_timed("curl", command, timeout)?;
            if !result.status.success() {
                return Err(format!(
                    "upload of {relative} failed (exit {}): {}",
                    result.status,
                    brief(&result.stderr)
                ));
            }
            uploaded.push(relative);
        }
        times.push(start.elapsed());
        if let Some(root) = ctx.cfg.ftp_root.as_deref() {
            for relative in &uploaded {
                cleanup_path(ctx, &root.join(relative));
            }
        }
    }
    Ok(times)
}

/// Uploads every file with its own `curl` process, the way a web upload path
/// is used. HTTP and HTTPS share this; only the URL and trust anchor differ.
pub(super) fn measure_web_put(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    curl: &Path,
    base_url: &str,
    ca: Option<&Path>,
    label: &str,
    root: Option<&Path>,
) -> Result<Vec<Duration>, String> {
    let files = walk_files(&dataset.root)
        .map_err(|err| format!("failed to walk {}: {err}", dataset.root.display()))?;
    let timeout = ctx.cfg.command_timeout(dataset.bytes);
    let pid = std::process::id();
    let mut times = Vec::with_capacity(ctx.cfg.iterations);

    for iteration in 0..ctx.cfg.iterations {
        let destination = format!("cftp-bench-{pid}-{}-{iteration}", dataset.name);
        let start = Instant::now();
        for (path, _) in &files {
            let relative = path
                .strip_prefix(&dataset.root)
                .map_err(|_| format!("{} is not under the dataset root", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            let url = format!(
                "{}/{destination}/{relative}",
                base_url.trim_end_matches('/')
            );

            let mut command = Command::new(curl);
            command
                .arg("--silent")
                .arg("--show-error")
                .arg("--fail")
                .arg("--connect-timeout")
                .arg("10")
                .arg("--max-time")
                .arg(timeout.as_secs().to_string());
            if let Some(ca) = ca {
                command.arg("--cacert").arg(ca);
            }
            command.arg("-T").arg(path).arg(&url);
            let result = run_timed("curl", command, timeout)?;
            if !result.status.success() {
                return Err(format!(
                    "{label} upload of {relative} failed (exit {}): {}",
                    result.status,
                    brief(&result.stderr)
                ));
            }
        }
        times.push(start.elapsed());
        if let Some(root) = root {
            cleanup_path(ctx, &root.join(&destination));
        }
    }
    Ok(times)
}

pub(super) fn measure_http(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    curl: &Path,
    python: Option<&Path>,
    remote_url: Option<&str>,
) -> Result<Vec<Duration>, String> {
    let large = dataset.root.join("large.bin");
    let size = fs::metadata(&large)
        .map_err(|err| format!("failed to inspect {}: {err}", large.display()))?
        .len();
    let timeout = ctx.cfg.command_timeout(size);
    let mut times = Vec::with_capacity(ctx.cfg.iterations);

    for _ in 0..ctx.cfg.iterations {
        if let Some(base) = remote_url {
            let url = format!("{}/large.bin", base.trim_end_matches('/'));
            times.push(curl_download(curl, &url, timeout)?);
            continue;
        }

        let python = python.ok_or_else(|| "python3 not found on PATH".to_string())?;
        let port = free_tcp_port()?;
        let mut command = Command::new(python);
        command
            .arg("-m")
            .arg("http.server")
            .arg(port.to_string())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--directory")
            .arg(&dataset.root)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut server = ChildGuard::spawn(command, "python3 http.server")?;
        wait_tcp_ready(port, &mut server)?;

        let url = format!("http://127.0.0.1:{port}/large.bin");
        let result = curl_download(curl, &url, timeout);
        server.kill_now();
        times.push(result?);
    }
    Ok(times)
}

pub(super) fn curl_download(curl: &Path, url: &str, timeout: Duration) -> Result<Duration, String> {
    let sink = if cfg!(windows) { "NUL" } else { "/dev/null" };
    let mut command = Command::new(curl);
    command
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--connect-timeout")
        .arg("10")
        .arg("--max-time")
        .arg(timeout.as_secs().to_string())
        .arg("--output")
        .arg(sink)
        .arg(url);
    let result = run_timed("curl", command, timeout)?;
    if !result.status.success() {
        return Err(format!("exit {}: {}", result.status, brief(&result.stderr)));
    }
    Ok(result.elapsed)
}

pub(super) fn wait_tcp_ready(port: u16, server: &mut ChildGuard) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    loop {
        if let Some(status) = server
            .try_wait_status()
            .map_err(|err| format!("failed to poll {}: {err}", server.label))?
        {
            return Err(format!(
                "{} exited before becoming ready ({status})",
                server.label
            ));
        }
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} did not accept connections within {READY_TIMEOUT:?}",
                server.label
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub(super) fn run_dir(ctx: &Ctx<'_>, dataset: &Dataset, tool: &str, iteration: usize) -> PathBuf {
    let path = ctx
        .work
        .join("runs")
        .join(&dataset.name)
        .join(format!("{tool}-{iteration}"));
    let _ = fs::remove_dir_all(&path);
    path
}

/// Removes one upload after it has been measured, when `--cleanup` is set.
///
/// Only paths created by this run are passed here: they embed the process id
/// and the dataset name. The release gate runs every peer inside one
/// container, so their destination trees are reachable from the harness; for
/// a remote peer the path simply does not exist locally and nothing happens.
pub(super) fn cleanup_path(ctx: &Ctx<'_>, path: &Path) {
    if !ctx.cfg.cleanup || ctx.cfg.keep {
        return;
    }
    let result = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            eprintln!("cleanup: {}: {error}", path.display());
            return;
        }
    };
    if let Err(error) = result {
        eprintln!("cleanup: {}: {error}", path.display());
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------
