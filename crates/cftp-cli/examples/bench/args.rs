use super::*;

pub(super) struct Config {
    pub(super) quick: bool,
    pub(super) keep: bool,
    pub(super) iterations: usize,
    pub(super) datasets: Vec<String>,
    pub(super) small_count: usize,
    pub(super) small_size: u64,
    pub(super) large_size: u64,
    pub(super) huge_files: usize,
    pub(super) huge_size: u64,
    pub(super) mixed_dirs: usize,
    pub(super) mixed_per_dir: usize,
    pub(super) mixed_small_size: u64,
    pub(super) mixed_large_size: u64,
    pub(super) ssh_target: Option<String>,
    pub(super) ssh_dest: String,
    pub(super) sftp_target: Option<String>,
    pub(super) sftp_dest: String,
    pub(super) ftp_url: Option<String>,
    pub(super) http_url: Option<String>,
    pub(super) http_put_url: Option<String>,
    pub(super) https_put_url: Option<String>,
    /// Certificate used to verify the HTTPS peer; without it curl uses its
    /// system trust store.
    pub(super) https_ca: Option<PathBuf>,
    /// Filesystem directory that HTTP PUT uploads land under. Used only to
    /// reclaim space with `--cleanup`.
    pub(super) http_put_root: Option<PathBuf>,
    /// Filesystem directory that HTTPS PUT uploads land under.
    pub(super) https_put_root: Option<PathBuf>,
    /// Filesystem directory that FTP uploads land under.
    pub(super) ftp_root: Option<PathBuf>,
    /// Delete each upload once it has been measured. Without this, a gate run
    /// keeps one copy of every dataset per peer until the processes exit.
    pub(super) cleanup: bool,
    pub(super) timeout_secs: u64,
    /// Run cftp against an already-running server instead of spawning one.
    pub(super) endpoint: Option<String>,
    /// Certificate directory used with `endpoint`.
    pub(super) cert_dir: Option<PathBuf>,
    /// Fail the run when cftp is slower than the fastest competitor.
    pub(super) gate: bool,
    /// Percentage by which cftp may trail the fastest competitor.
    pub(super) gate_tolerance_pct: f64,
    /// Where to write machine-readable results.
    pub(super) json: Option<PathBuf>,
    /// Where to write a standalone Markdown results page.
    pub(super) markdown: Option<PathBuf>,
    /// Where to write the README fragment (no title or methodology).
    pub(super) readme_block: Option<PathBuf>,
    /// Human-readable description of the machine or environment.
    pub(super) label: Option<String>,
}

impl Config {
    pub(super) fn command_timeout(&self, bytes: u64) -> Duration {
        if self.timeout_secs > 0 {
            return Duration::from_secs(self.timeout_secs);
        }
        // Generous: a slow machine should still finish, but a hung transfer
        // must not hang the harness forever.
        let seconds = 120u64.saturating_add(bytes / (1 << 20)).min(3600);
        Duration::from_secs(seconds)
    }
}

#[derive(Default)]
pub(super) struct RawArgs {
    pub(super) quick: bool,
    pub(super) keep: bool,
    pub(super) iterations: Option<usize>,
    pub(super) datasets: Option<String>,
    pub(super) small_count: Option<usize>,
    pub(super) small_size: Option<u64>,
    pub(super) large_size: Option<u64>,
    pub(super) huge_files: Option<usize>,
    pub(super) huge_size: Option<u64>,
    pub(super) mixed_dirs: Option<usize>,
    pub(super) mixed_per_dir: Option<usize>,
    pub(super) mixed_small_size: Option<u64>,
    pub(super) mixed_large_size: Option<u64>,
    pub(super) ssh_target: Option<String>,
    pub(super) ssh_dest: Option<String>,
    pub(super) sftp_target: Option<String>,
    pub(super) sftp_dest: Option<String>,
    pub(super) ftp_url: Option<String>,
    pub(super) http_url: Option<String>,
    pub(super) http_put_url: Option<String>,
    pub(super) https_put_url: Option<String>,
    pub(super) https_ca: Option<String>,
    pub(super) http_put_root: Option<String>,
    pub(super) https_put_root: Option<String>,
    pub(super) ftp_root: Option<String>,
    pub(super) cleanup: bool,
    pub(super) timeout_secs: Option<u64>,
    pub(super) endpoint: Option<String>,
    pub(super) cert_dir: Option<String>,
    pub(super) gate: bool,
    pub(super) gate_tolerance_pct: Option<f64>,
    pub(super) json: Option<String>,
    pub(super) markdown: Option<String>,
    pub(super) readme_block: Option<String>,
    pub(super) label: Option<String>,
}

pub(super) fn parse_config() -> Result<Config, String> {
    let raw = parse_raw()?;
    let quick = raw.quick;

    let (
        default_small_count,
        default_small_size,
        default_large_size,
        default_huge_size,
        default_mixed_dirs,
        default_mixed_per_dir,
        default_mixed_large_size,
    ) = if quick {
        (
            200usize,
            4 * 1024u64,
            4 * 1024 * 1024u64,
            64 * 1024 * 1024u64,
            4usize,
            4usize,
            2 * 1024 * 1024u64,
        )
    } else {
        (
            2000usize,
            4 * 1024u64,
            256 * 1024 * 1024u64,
            1024 * 1024 * 1024u64,
            8usize,
            16usize,
            32 * 1024 * 1024u64,
        )
    };

    let small_count = raw
        .small_count
        .or(env_parsed("CFTP_BENCH_SMALL_COUNT")?)
        .unwrap_or(default_small_count);
    let small_size = raw
        .small_size
        .or(env_size("CFTP_BENCH_SMALL_SIZE")?)
        .unwrap_or(default_small_size);
    let large_size = raw
        .large_size
        .or(env_size("CFTP_BENCH_LARGE_SIZE")?)
        .unwrap_or(default_large_size);
    let huge_files = raw
        .huge_files
        .or(env_parsed("CFTP_BENCH_HUGE_FILES")?)
        .unwrap_or(1);
    let huge_size = raw
        .huge_size
        .or(env_size("CFTP_BENCH_HUGE_SIZE")?)
        .unwrap_or(default_huge_size);
    let mixed_dirs = raw
        .mixed_dirs
        .or(env_parsed("CFTP_BENCH_MIXED_DIRS")?)
        .unwrap_or(default_mixed_dirs);
    let mixed_per_dir = raw
        .mixed_per_dir
        .or(env_parsed("CFTP_BENCH_MIXED_PER_DIR")?)
        .unwrap_or(default_mixed_per_dir);
    let mixed_small_size = raw
        .mixed_small_size
        .or(env_size("CFTP_BENCH_MIXED_SMALL_SIZE")?)
        .unwrap_or(small_size.max(1));
    let mixed_large_size = raw
        .mixed_large_size
        .or(env_size("CFTP_BENCH_MIXED_LARGE_SIZE")?)
        .unwrap_or(default_mixed_large_size);
    let iterations = raw
        .iterations
        .or(env_parsed("CFTP_BENCH_ITERATIONS")?)
        .unwrap_or(1);

    if iterations == 0 {
        return Err("--iterations must be at least 1".to_string());
    }
    if small_count > 0 && small_size == 0 {
        return Err("--small-size must be greater than zero".to_string());
    }
    if large_size == 0 {
        return Err("--large-size must be greater than zero".to_string());
    }
    if huge_files > 0 && huge_size == 0 {
        return Err("--huge-size must be greater than zero".to_string());
    }
    if mixed_small_size == 0 && mixed_per_dir > 0 {
        return Err("--mixed-small-size must be greater than zero".to_string());
    }
    if mixed_large_size == 0 {
        return Err("--mixed-large-size must be greater than zero".to_string());
    }

    let datasets = match raw.datasets {
        Some(list) => parse_dataset_list(&list)?,
        None => vec![
            "many-small".to_string(),
            "large".to_string(),
            "mixed".to_string(),
            "huge".to_string(),
        ],
    };

    let ssh_target = raw
        .ssh_target
        .or_else(|| env_nonempty("CFTP_BENCH_SSH"))
        .filter(|target| !target.trim().is_empty());
    let ssh_dest = raw
        .ssh_dest
        .or_else(|| env_nonempty("CFTP_BENCH_SSH_DEST"))
        .unwrap_or_else(|| "/tmp".to_string());
    let sftp_target = raw
        .sftp_target
        .or_else(|| env_nonempty("CFTP_BENCH_SFTP"))
        .filter(|target| !target.trim().is_empty());
    let sftp_dest = raw
        .sftp_dest
        .or_else(|| env_nonempty("CFTP_BENCH_SFTP_DEST"))
        .unwrap_or_else(|| "/tmp".to_string());
    let ftp_url = raw.ftp_url.or_else(|| env_nonempty("CFTP_BENCH_FTP_URL"));
    let http_url = raw.http_url.or_else(|| env_nonempty("CFTP_BENCH_HTTP_URL"));
    let http_put_url = raw
        .http_put_url
        .or_else(|| env_nonempty("CFTP_BENCH_HTTP_PUT_URL"));
    let https_put_url = raw
        .https_put_url
        .or_else(|| env_nonempty("CFTP_BENCH_HTTPS_PUT_URL"));
    let https_ca = raw
        .https_ca
        .or_else(|| env_nonempty("CFTP_BENCH_HTTPS_CA"))
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from);
    let endpoint = raw.endpoint.or_else(|| env_nonempty("CFTP_BENCH_ENDPOINT"));
    if let Some(endpoint) = &endpoint
        && (endpoint.contains("://") || !endpoint.contains(':'))
    {
        return Err(format!(
            "--endpoint must be HOST:PORT without a scheme, got {endpoint:?}"
        ));
    }
    let cert_dir = raw
        .cert_dir
        .or_else(|| env_nonempty("CFTP_BENCH_CERT_DIR"))
        .map(PathBuf::from)
        .filter(|dir| !dir.as_os_str().is_empty());

    if raw.gate || env_truthy("CFTP_BENCH_GATE") {
        if iterations < 3 {
            return Err(
                "--gate requires --iterations 3 or more; a release gate must not decide from \
                 a single noisy run"
                    .to_string(),
            );
        }
        if ssh_target.is_none() {
            return Err(
                "--gate requires --ssh-target (or CFTP_BENCH_SSH) so scp is measured".to_string(),
            );
        }
        if sftp_target.is_none() {
            return Err(
                "--gate requires --sftp-target (or CFTP_BENCH_SFTP) so SFTP is measured"
                    .to_string(),
            );
        }
        if https_put_url.is_none() {
            return Err(
                "--gate requires --https-put-url (or CFTP_BENCH_HTTPS_PUT_URL) so HTTPS \
                 uploads are measured"
                    .to_string(),
            );
        }
        if ftp_url.is_none() {
            return Err(
                "--gate requires --ftp-url (or CFTP_BENCH_FTP_URL) so the plaintext reference \
                 table is complete"
                    .to_string(),
            );
        }
        if http_put_url.is_none() {
            return Err(
                "--gate requires --http-put-url (or CFTP_BENCH_HTTP_PUT_URL) so the plaintext \
                 reference table is complete"
                    .to_string(),
            );
        }
    }

    let gate_tolerance_pct = raw
        .gate_tolerance_pct
        .or(env_parsed("CFTP_BENCH_GATE_TOLERANCE")?)
        .unwrap_or(5.0);
    if !gate_tolerance_pct.is_finite() || gate_tolerance_pct < 0.0 {
        return Err("--gate-tolerance must be a non-negative number".to_string());
    }

    Ok(Config {
        quick,
        keep: raw.keep,
        iterations,
        datasets,
        small_count,
        small_size,
        large_size,
        huge_files,
        huge_size,
        mixed_dirs,
        mixed_per_dir,
        mixed_small_size,
        mixed_large_size,
        ssh_target,
        ssh_dest,
        sftp_target,
        sftp_dest,
        ftp_url,
        http_url,
        http_put_url,
        https_put_url,
        https_ca,
        http_put_root: raw
            .http_put_root
            .or_else(|| env_nonempty("CFTP_BENCH_HTTP_PUT_ROOT"))
            .map(PathBuf::from),
        https_put_root: raw
            .https_put_root
            .or_else(|| env_nonempty("CFTP_BENCH_HTTPS_PUT_ROOT"))
            .map(PathBuf::from),
        ftp_root: raw
            .ftp_root
            .or_else(|| env_nonempty("CFTP_BENCH_FTP_ROOT"))
            .map(PathBuf::from),
        cleanup: raw.cleanup || env_truthy("CFTP_BENCH_CLEANUP"),
        timeout_secs: raw.timeout_secs.unwrap_or(0),
        endpoint,
        cert_dir,
        gate: raw.gate || env_truthy("CFTP_BENCH_GATE"),
        gate_tolerance_pct,
        json: raw
            .json
            .or_else(|| env_nonempty("CFTP_BENCH_JSON"))
            .map(PathBuf::from),
        markdown: raw
            .markdown
            .or_else(|| env_nonempty("CFTP_BENCH_MARKDOWN"))
            .map(PathBuf::from),
        readme_block: raw
            .readme_block
            .or_else(|| env_nonempty("CFTP_BENCH_README_BLOCK"))
            .map(PathBuf::from),
        label: raw.label.or_else(|| env_nonempty("CFTP_BENCH_LABEL")),
    })
}

pub(super) fn parse_raw() -> Result<RawArgs, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut raw = RawArgs::default();
    let mut index = 0;

    while index < args.len() {
        let arg = &args[index];
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag, Some(value)),
            None => (arg.as_str(), None),
        };

        match flag {
            "--quick" => {
                reject_inline(flag, inline)?;
                raw.quick = true;
            }
            "--keep" => {
                reject_inline(flag, inline)?;
                raw.keep = true;
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--iterations" => {
                raw.iterations = Some(parse_usize(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            "--datasets" => raw.datasets = Some(value_for(flag, inline, &args, &mut index)?),
            "--small-count" => {
                raw.small_count = Some(parse_usize(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            "--small-size" => {
                raw.small_size = Some(parse_size(&value_for(flag, inline, &args, &mut index)?)?);
            }
            "--large-size" => {
                raw.large_size = Some(parse_size(&value_for(flag, inline, &args, &mut index)?)?);
            }
            "--huge-files" => {
                raw.huge_files = Some(parse_usize(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            "--huge-size" => {
                raw.huge_size = Some(parse_size(&value_for(flag, inline, &args, &mut index)?)?);
            }
            "--mixed-dirs" => {
                raw.mixed_dirs = Some(parse_usize(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            "--mixed-per-dir" => {
                raw.mixed_per_dir = Some(parse_usize(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            "--mixed-small-size" => {
                raw.mixed_small_size =
                    Some(parse_size(&value_for(flag, inline, &args, &mut index)?)?);
            }
            "--mixed-large-size" => {
                raw.mixed_large_size =
                    Some(parse_size(&value_for(flag, inline, &args, &mut index)?)?);
            }
            "--ssh-target" => raw.ssh_target = Some(value_for(flag, inline, &args, &mut index)?),
            "--ssh-dest" => raw.ssh_dest = Some(value_for(flag, inline, &args, &mut index)?),
            "--sftp-target" => raw.sftp_target = Some(value_for(flag, inline, &args, &mut index)?),
            "--sftp-dest" => raw.sftp_dest = Some(value_for(flag, inline, &args, &mut index)?),
            "--ftp-url" => raw.ftp_url = Some(value_for(flag, inline, &args, &mut index)?),
            "--http-url" => raw.http_url = Some(value_for(flag, inline, &args, &mut index)?),
            "--http-put-url" => {
                raw.http_put_url = Some(value_for(flag, inline, &args, &mut index)?);
            }
            "--https-put-url" => {
                raw.https_put_url = Some(value_for(flag, inline, &args, &mut index)?);
            }
            "--https-ca" => raw.https_ca = Some(value_for(flag, inline, &args, &mut index)?),
            "--http-put-root" => {
                raw.http_put_root = Some(value_for(flag, inline, &args, &mut index)?);
            }
            "--https-put-root" => {
                raw.https_put_root = Some(value_for(flag, inline, &args, &mut index)?);
            }
            "--ftp-root" => raw.ftp_root = Some(value_for(flag, inline, &args, &mut index)?),
            "--cleanup" => {
                reject_inline(flag, inline)?;
                raw.cleanup = true;
            }
            "--gate" => {
                reject_inline(flag, inline)?;
                raw.gate = true;
            }
            "--gate-tolerance" => {
                raw.gate_tolerance_pct = Some(parse_f64(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            "--json" => raw.json = Some(value_for(flag, inline, &args, &mut index)?),
            "--markdown" => raw.markdown = Some(value_for(flag, inline, &args, &mut index)?),
            "--readme-block" => {
                raw.readme_block = Some(value_for(flag, inline, &args, &mut index)?);
            }
            "--label" => raw.label = Some(value_for(flag, inline, &args, &mut index)?),
            "--endpoint" => raw.endpoint = Some(value_for(flag, inline, &args, &mut index)?),
            "--cert-dir" => raw.cert_dir = Some(value_for(flag, inline, &args, &mut index)?),
            "--timeout" => {
                raw.timeout_secs = Some(parse_u64(
                    flag,
                    &value_for(flag, inline, &args, &mut index)?,
                )?);
            }
            _ if flag.starts_with('-') => return Err(format!("unknown option {arg:?}")),
            _ => return Err(format!("unexpected argument {arg:?}")),
        }
        index += 1;
    }

    Ok(raw)
}

pub(super) fn reject_inline(flag: &str, inline: Option<&str>) -> Result<(), String> {
    if inline.is_some() {
        Err(format!("{flag} does not take a value"))
    } else {
        Ok(())
    }
}

pub(super) fn value_for(
    flag: &str,
    inline: Option<&str>,
    args: &[String],
    index: &mut usize,
) -> Result<String, String> {
    if let Some(value) = inline {
        return Ok(value.to_string());
    }
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

pub(super) fn parse_usize(flag: &str, text: &str) -> Result<usize, String> {
    text.trim()
        .parse::<usize>()
        .map_err(|_| format!("{flag} has invalid value {text:?}"))
}

pub(super) fn parse_u64(flag: &str, text: &str) -> Result<u64, String> {
    text.trim()
        .parse::<u64>()
        .map_err(|_| format!("{flag} has invalid value {text:?}"))
}

/// Parse a byte size such as `4096`, `4KiB`, or `256MiB`.
pub(super) fn parse_size(text: &str) -> Result<u64, String> {
    let trimmed = text.trim();
    let lower = trimmed.to_ascii_lowercase();
    let suffixes: [(&str, u64); 10] = [
        ("kib", 1024),
        ("mib", 1024 * 1024),
        ("gib", 1024 * 1024 * 1024),
        ("kb", 1000),
        ("mb", 1000 * 1000),
        ("gb", 1000 * 1000 * 1000),
        ("k", 1024),
        ("m", 1024 * 1024),
        ("g", 1024 * 1024 * 1024),
        ("b", 1),
    ];
    let (number, multiplier) = suffixes
        .iter()
        .find_map(|(suffix, multiplier)| {
            lower
                .strip_suffix(suffix)
                .map(|number| (number.trim().to_string(), *multiplier))
        })
        .unwrap_or_else(|| (lower.clone(), 1));

    if number.is_empty() {
        return Err(format!("invalid size {text:?}"));
    }
    let value = number
        .parse::<f64>()
        .map_err(|_| format!("invalid size {text:?}"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("invalid size {text:?}"));
    }
    let bytes = value * multiplier as f64;
    if bytes > u64::MAX as f64 {
        return Err(format!("size {text:?} is too large"));
    }
    Ok(bytes.round() as u64)
}

pub(super) fn env_nonempty(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn env_truthy(name: &str) -> bool {
    matches!(
        env::var(name).ok().as_deref().map(str::trim),
        Some("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
    )
}

pub(super) fn parse_f64(flag: &str, text: &str) -> Result<f64, String> {
    text.trim()
        .parse::<f64>()
        .map_err(|_| format!("{flag} has invalid value {text:?}"))
}

pub(super) fn env_parsed<T>(name: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<T>()
            .map(Some)
            .map_err(|err| format!("{name} has invalid value {value:?}: {err}")),
        _ => Ok(None),
    }
}

pub(super) fn env_size(name: &str) -> Result<Option<u64>, String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => parse_size(&value)
            .map(Some)
            .map_err(|err| format!("{name}: {err}")),
        _ => Ok(None),
    }
}

pub(super) fn parse_dataset_list(list: &str) -> Result<Vec<String>, String> {
    let mut datasets = Vec::new();
    for item in list.split(',') {
        let name = item.trim();
        if name.is_empty() {
            continue;
        }
        if !matches!(name, "many-small" | "large" | "mixed" | "huge") {
            return Err(format!(
                "unknown dataset {name:?} (expected many-small, large, mixed, or huge)"
            ));
        }
        if !datasets.iter().any(|existing| existing == name) {
            datasets.push(name.to_string());
        }
    }
    if datasets.is_empty() {
        return Err("--datasets selected no datasets".to_string());
    }
    Ok(datasets)
}

pub(super) fn print_usage() {
    println!(
        "\
cftp benchmark harness

Usage: bench [OPTIONS]

Options:
  --quick                     Small datasets for a fast smoke run
  --iterations <N>            Runs per measurement (default: 1)
  --datasets <LIST>           Comma-separated: many-small,large,mixed,huge
  --small-count <N>           many-small file count (default: 2000; quick: 200)
  --small-size <SIZE>         many-small file size (default: 4KiB)
  --large-size <SIZE>         large dataset file size (default: 256MiB; quick: 4MiB)
  --huge-files <N>            huge dataset file count (default: 1)
  --huge-size <SIZE>          huge dataset file size (default: 1GiB; quick: 64MiB)
  --mixed-dirs <N>            mixed dataset top-level directories (default: 8; quick: 4)
  --mixed-per-dir <N>         mixed small files per directory (default: 16; quick: 4)
  --mixed-small-size <SIZE>   mixed small file size (default: 4KiB)
  --mixed-large-size <SIZE>   mixed large file size (default: 32MiB; quick: 2MiB)
  --ssh-target <USER@HOST>    Enable scp comparison (or CFTP_BENCH_SSH)
  --ssh-dest <DIR>            Remote parent directory for scp (default: /tmp)
  --sftp-target <USER@HOST>   Enable SFTP comparison (or CFTP_BENCH_SFTP)
  --sftp-dest <DIR>           Remote parent directory for SFTP (default: /tmp)
  --ftp-url <URL>             Enable FTP upload comparison (or CFTP_BENCH_FTP_URL)
  --http-url <URL>            Download large.bin from this base URL (or
                              CFTP_BENCH_HTTP_URL); otherwise a local python3
                              http.server baseline is used when available
  --http-put-url <URL>        Upload every file with HTTP PUT to this base URL
                              (or CFTP_BENCH_HTTP_PUT_URL). Required by --gate.
  --https-put-url <URL>       Upload every file with HTTPS PUT to this base URL
                              (or CFTP_BENCH_HTTPS_PUT_URL). Required by --gate.
  --https-ca <PEM>            CA certificate used to verify the HTTPS peer
                              (or CFTP_BENCH_HTTPS_CA); default: system store
  --http-put-root <DIR>       Filesystem directory behind --http-put-url; with
                              --cleanup, uploads are removed after measuring
                              (or CFTP_BENCH_HTTP_PUT_ROOT)
  --https-put-root <DIR>      Filesystem directory behind --https-put-url
                              (or CFTP_BENCH_HTTPS_PUT_ROOT)
  --ftp-root <DIR>            Filesystem directory behind --ftp-url (or
                              CFTP_BENCH_FTP_ROOT)
  --cleanup                   Remove each upload after measuring it, so large
                              datasets need one copy on disk instead of one per
                              peer and iteration (or CFTP_BENCH_CLEANUP)
  --gate                      Require every comparison on every dataset and
                              fail when cftp is more than the tolerance slower
                              than the fastest security-equivalent peer (scp,
                              SFTP, HTTPS). FTP and HTTP are plaintext and are
                              reported, not gated. Requires --iterations >= 3.
  --gate-tolerance <PCT>      Allowed cftp deficit in percent (default: 5)
  --json <PATH>               Write machine-readable results as JSON
  --markdown <PATH>           Write a standalone Markdown results page
  --readme-block <PATH>       Write the README results fragment
  --label <TEXT>              Environment description stored with results
  --endpoint <HOST:PORT>      Benchmark cftp against this already-running
                              server instead of spawning one (or
                              CFTP_BENCH_ENDPOINT). Received bytes are not
                              verified in this mode.
  --cert-dir <DIR>            Certificate directory for --endpoint (or
                              CFTP_BENCH_CERT_DIR; default: ~/.config/cftp)
  --timeout <SECS>            Per-command timeout (default: automatic)
  --keep                      Keep the temporary workspace
  -h, --help                  Print this help

Environment:
  CFTP_BENCH_SSH, CFTP_BENCH_SSH_DEST, CFTP_BENCH_SFTP, CFTP_BENCH_SFTP_DEST,
  CFTP_BENCH_FTP_URL, CFTP_BENCH_HTTP_URL, CFTP_BENCH_HTTP_PUT_URL,
  CFTP_BENCH_HTTPS_PUT_URL, CFTP_BENCH_HTTPS_CA, CFTP_BENCH_HTTP_PUT_ROOT,
  CFTP_BENCH_HTTPS_PUT_ROOT, CFTP_BENCH_FTP_ROOT, CFTP_BENCH_CLEANUP,
  CFTP_BENCH_ENDPOINT,
  CFTP_BENCH_CERT_DIR, CFTP_BENCH_GATE, CFTP_BENCH_GATE_TOLERANCE,
  CFTP_BENCH_JSON, CFTP_BENCH_MARKDOWN, CFTP_BENCH_README_BLOCK,
  CFTP_BENCH_LABEL, CFTP_BENCH_ITERATIONS, CFTP_BENCH_SMALL_COUNT,
  CFTP_BENCH_SMALL_SIZE, CFTP_BENCH_LARGE_SIZE, CFTP_BENCH_HUGE_FILES,
  CFTP_BENCH_HUGE_SIZE, CFTP_BENCH_MIXED_DIRS, CFTP_BENCH_MIXED_PER_DIR,
  CFTP_BENCH_MIXED_SMALL_SIZE, CFTP_BENCH_MIXED_LARGE_SIZE

Sizes accept plain bytes or KiB/MiB/GiB (and KB/MB/GB) suffixes."
    );
}

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------
