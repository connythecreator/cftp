#![forbid(unsafe_code)]
//! Self-contained performance harness for cftp.
//!
//! The harness generates three datasets in a temporary directory, transfers
//! each one over loopback with `cftp serve`/`cftp put` (mutual TLS), and
//! compares the result with local recursive copies and, when the relevant
//! tools and targets exist, with `scp`, FTP, and HTTP.
//!
//! External comparisons are opt-in and never fail the run: a missing tool, a
//! missing target, or a failing server is reported as skipped or failed while
//! the cftp and local results are still produced.
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p cftp-cli --example bench -- --quick
//! ```
//!
//! See `bench/README.md` for the full description and fairness caveats.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MIB: f64 = 1024.0 * 1024.0;
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const SERVER_EXIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Processes that must be killed if the harness panics. The release profile
/// uses `panic = "abort"`, so destructors do not run on panic; the panic hook
/// consults this registry instead.
static LIVE_CHILDREN: Mutex<Vec<u32>> = Mutex::new(Vec::new());

fn main() {
    install_panic_hook();
    if let Err(err) = run() {
        eprintln!("bench: {err}");
        std::process::exit(1);
    }
}

fn install_panic_hook() {
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

fn registered_children() -> Vec<u32> {
    match LIVE_CHILDREN.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn register_child(pid: u32) {
    let mut guard = match LIVE_CHILDREN.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.push(pid);
}

fn unregister_child(pid: u32) {
    let mut guard = match LIVE_CHILDREN.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.retain(|registered| *registered != pid);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct Config {
    quick: bool,
    keep: bool,
    iterations: usize,
    datasets: Vec<String>,
    small_count: usize,
    small_size: u64,
    large_size: u64,
    mixed_dirs: usize,
    mixed_per_dir: usize,
    mixed_small_size: u64,
    mixed_large_size: u64,
    ssh_target: Option<String>,
    ssh_dest: String,
    ftp_url: Option<String>,
    http_url: Option<String>,
    timeout_secs: u64,
}

impl Config {
    fn command_timeout(&self, bytes: u64) -> Duration {
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
struct RawArgs {
    quick: bool,
    keep: bool,
    iterations: Option<usize>,
    datasets: Option<String>,
    small_count: Option<usize>,
    small_size: Option<u64>,
    large_size: Option<u64>,
    mixed_dirs: Option<usize>,
    mixed_per_dir: Option<usize>,
    mixed_small_size: Option<u64>,
    mixed_large_size: Option<u64>,
    ssh_target: Option<String>,
    ssh_dest: Option<String>,
    ftp_url: Option<String>,
    http_url: Option<String>,
    timeout_secs: Option<u64>,
}

fn parse_config() -> Result<Config, String> {
    let raw = parse_raw()?;
    let quick = raw.quick;

    let (
        default_small_count,
        default_small_size,
        default_large_size,
        default_mixed_dirs,
        default_mixed_per_dir,
        default_mixed_large_size,
    ) = if quick {
        (
            200usize,
            4 * 1024u64,
            4 * 1024 * 1024u64,
            4usize,
            4usize,
            2 * 1024 * 1024u64,
        )
    } else {
        (
            2000usize,
            4 * 1024u64,
            256 * 1024 * 1024u64,
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
    let ftp_url = raw.ftp_url.or_else(|| env_nonempty("CFTP_BENCH_FTP_URL"));
    let http_url = raw.http_url.or_else(|| env_nonempty("CFTP_BENCH_HTTP_URL"));

    Ok(Config {
        quick,
        keep: raw.keep,
        iterations,
        datasets,
        small_count,
        small_size,
        large_size,
        mixed_dirs,
        mixed_per_dir,
        mixed_small_size,
        mixed_large_size,
        ssh_target,
        ssh_dest,
        ftp_url,
        http_url,
        timeout_secs: raw.timeout_secs.unwrap_or(0),
    })
}

fn parse_raw() -> Result<RawArgs, String> {
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
            "--ftp-url" => raw.ftp_url = Some(value_for(flag, inline, &args, &mut index)?),
            "--http-url" => raw.http_url = Some(value_for(flag, inline, &args, &mut index)?),
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

fn reject_inline(flag: &str, inline: Option<&str>) -> Result<(), String> {
    if inline.is_some() {
        Err(format!("{flag} does not take a value"))
    } else {
        Ok(())
    }
}

fn value_for(
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

fn parse_usize(flag: &str, text: &str) -> Result<usize, String> {
    text.trim()
        .parse::<usize>()
        .map_err(|_| format!("{flag} has invalid value {text:?}"))
}

fn parse_u64(flag: &str, text: &str) -> Result<u64, String> {
    text.trim()
        .parse::<u64>()
        .map_err(|_| format!("{flag} has invalid value {text:?}"))
}

/// Parse a byte size such as `4096`, `4KiB`, or `256MiB`.
fn parse_size(text: &str) -> Result<u64, String> {
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

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_parsed<T>(name: &str) -> Result<Option<T>, String>
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

fn env_size(name: &str) -> Result<Option<u64>, String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => parse_size(&value)
            .map(Some)
            .map_err(|err| format!("{name}: {err}")),
        _ => Ok(None),
    }
}

fn parse_dataset_list(list: &str) -> Result<Vec<String>, String> {
    let mut datasets = Vec::new();
    for item in list.split(',') {
        let name = item.trim();
        if name.is_empty() {
            continue;
        }
        if !matches!(name, "many-small" | "large" | "mixed") {
            return Err(format!(
                "unknown dataset {name:?} (expected many-small, large, or mixed)"
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

fn print_usage() {
    println!(
        "\
cftp benchmark harness

Usage: bench [OPTIONS]

Options:
  --quick                     Small datasets for a fast smoke run
  --iterations <N>            Runs per measurement (default: 1)
  --datasets <LIST>           Comma-separated: many-small,large,mixed
  --small-count <N>           many-small file count (default: 2000; quick: 200)
  --small-size <SIZE>         many-small file size (default: 4KiB)
  --large-size <SIZE>         large dataset file size (default: 256MiB; quick: 4MiB)
  --mixed-dirs <N>            mixed dataset top-level directories (default: 8; quick: 4)
  --mixed-per-dir <N>         mixed small files per directory (default: 16; quick: 4)
  --mixed-small-size <SIZE>   mixed small file size (default: 4KiB)
  --mixed-large-size <SIZE>   mixed large file size (default: 32MiB; quick: 2MiB)
  --ssh-target <USER@HOST>    Enable scp comparison (or CFTP_BENCH_SSH)
  --ssh-dest <DIR>            Remote parent directory for scp (default: /tmp)
  --ftp-url <URL>             Enable FTP upload comparison (or CFTP_BENCH_FTP_URL)
  --http-url <URL>            Download large.bin from this base URL (or
                              CFTP_BENCH_HTTP_URL); otherwise a local python3
                              http.server baseline is used when available
  --timeout <SECS>            Per-command timeout (default: automatic)
  --keep                      Keep the temporary workspace
  -h, --help                  Print this help

Environment:
  CFTP_BENCH_SSH, CFTP_BENCH_SSH_DEST, CFTP_BENCH_FTP_URL, CFTP_BENCH_HTTP_URL,
  CFTP_BENCH_ITERATIONS, CFTP_BENCH_SMALL_COUNT, CFTP_BENCH_SMALL_SIZE,
  CFTP_BENCH_LARGE_SIZE, CFTP_BENCH_MIXED_DIRS, CFTP_BENCH_MIXED_PER_DIR,
  CFTP_BENCH_MIXED_SMALL_SIZE, CFTP_BENCH_MIXED_LARGE_SIZE

Sizes accept plain bytes or KiB/MiB/GiB (and KB/MB/GB) suffixes."
    );
}

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

struct Dataset {
    name: String,
    root: PathBuf,
    files: u64,
    bytes: u64,
}

fn generate_dataset(base: &Path, name: &str, cfg: &Config) -> Result<Dataset, String> {
    let root = base.join(name);
    fs::create_dir_all(&root)
        .map_err(|err| format!("failed to create {}: {err}", root.display()))?;

    match name {
        "many-small" => {
            for index in 0..cfg.small_count {
                let path = root.join(format!("file-{index:05}.bin"));
                write_pattern(&path, cfg.small_size, seed_for(index as u64))
                    .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
            }
        }
        "large" => {
            let path = root.join("large.bin");
            write_pattern(&path, cfg.large_size, seed_for(1))
                .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
        }
        "mixed" => {
            let path = root.join("large.bin");
            write_pattern(&path, cfg.mixed_large_size, seed_for(2))
                .map_err(|err| format!("failed to write {}: {err}", path.display()))?;

            let medium_size = cfg.mixed_small_size.saturating_mul(16).max(1);
            for dir_index in 0..cfg.mixed_dirs {
                let top = root.join(format!("dir-{dir_index:02}"));
                let nested = top.join("nested").join("level-2");
                fs::create_dir_all(&nested)
                    .map_err(|err| format!("failed to create {}: {err}", nested.display()))?;

                for file_index in 0..cfg.mixed_per_dir {
                    let path = nested.join(format!("file-{file_index:04}.bin"));
                    let seed = seed_for((dir_index * 1000 + file_index + 10) as u64);
                    write_pattern(&path, cfg.mixed_small_size, seed)
                        .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
                }

                let medium = top.join(format!("medium-{dir_index:02}.bin"));
                write_pattern(&medium, medium_size, seed_for((dir_index + 100) as u64))
                    .map_err(|err| format!("failed to write {}: {err}", medium.display()))?;
            }
        }
        other => return Err(format!("unknown dataset {other:?}")),
    }

    let (files, bytes) = tree_stats(&root)?;
    Ok(Dataset {
        name: name.to_string(),
        root,
        files,
        bytes,
    })
}

fn seed_for(index: u64) -> u64 {
    0x9E37_79B9_7F4A_7C15u64
        .wrapping_mul(index.wrapping_add(1))
        .max(1)
}

/// Write `size` deterministic, practically incompressible bytes.
fn write_pattern(path: &Path, size: u64, seed: u64) -> io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    let mut rng = Rng(seed | 1);
    let mut buffer = vec![0u8; 64 * 1024];

    let mut remaining = size;
    while remaining > 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        rng.fill(&mut buffer[..take]);
        writer.write_all(&buffer[..take])?;
        remaining -= take as u64;
    }
    writer.flush()
}

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn fill(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

fn walk_files(root: &Path) -> io::Result<Vec<(PathBuf, u64)>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                let size = entry.metadata()?.len();
                files.push((entry.path(), size));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn tree_stats(root: &Path) -> Result<(u64, u64), String> {
    let files =
        walk_files(root).map_err(|err| format!("failed to walk {}: {err}", root.display()))?;
    let mut bytes = 0u64;
    for (_, size) in &files {
        bytes = bytes.saturating_add(*size);
    }
    Ok((files.len() as u64, bytes))
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

fn verify_tree(source: &Path, destination: &Path) -> Result<(), String> {
    if !destination.is_dir() {
        return Err(format!(
            "destination {} does not exist after the transfer",
            destination.display()
        ));
    }

    let expected = relative_files(source)?;
    let mut actual = relative_files(destination)?;

    for (relative, expected_size) in &expected {
        let Some(actual_size) = actual.remove(relative) else {
            return Err(format!("missing file {}", relative.display()));
        };
        if actual_size != *expected_size {
            return Err(format!(
                "size mismatch for {}: expected {expected_size} bytes, got {actual_size}",
                relative.display()
            ));
        }
        if !files_equal(&source.join(relative), &destination.join(relative))? {
            return Err(format!("content mismatch for {}", relative.display()));
        }
    }

    if let Some((extra, _)) = actual.iter().next() {
        return Err(format!("unexpected file {}", extra.display()));
    }
    Ok(())
}

fn relative_files(root: &Path) -> Result<BTreeMap<PathBuf, u64>, String> {
    let files =
        walk_files(root).map_err(|err| format!("failed to walk {}: {err}", root.display()))?;
    let mut map = BTreeMap::new();
    for (path, size) in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| format!("{} is not under {}", path.display(), root.display()))?
            .to_path_buf();
        map.insert(relative, size);
    }
    Ok(map)
}

fn files_equal(left: &Path, right: &Path) -> Result<bool, String> {
    let mut left = BufReader::new(
        File::open(left).map_err(|err| format!("failed to open {}: {err}", left.display()))?,
    );
    let mut right = BufReader::new(
        File::open(right).map_err(|err| format!("failed to open {}: {err}", right.display()))?,
    );
    let mut left_buffer = [0u8; 64 * 1024];
    let mut right_buffer = [0u8; 64 * 1024];

    loop {
        let left_read = read_full(&mut left, &mut left_buffer)?;
        let right_read = read_full(&mut right, &mut right_buffer)?;
        if left_read != right_read {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
        if left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
    }
}

fn read_full(reader: &mut impl Read, buffer: &mut [u8]) -> Result<usize, String> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("read failed: {err}")),
        }
    }
    Ok(filled)
}

/// Recursive copy using only `std::fs`.
fn copy_dir(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Child process management
// ---------------------------------------------------------------------------

struct ChildGuard {
    child: Child,
    status: Option<ExitStatus>,
    label: String,
    registered: bool,
}

impl ChildGuard {
    fn spawn(mut command: Command, label: &str) -> Result<Self, String> {
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

    fn try_wait_status(&mut self) -> io::Result<Option<ExitStatus>> {
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

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
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

    fn kill_now(&mut self) {
        if self.status.is_none() {
            let _ = self.child.kill();
            if let Ok(status) = self.child.wait() {
                self.status = Some(status);
            }
            self.unregister();
        }
    }

    fn unregister(&mut self) {
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
struct ServerProcess {
    guard: ChildGuard,
    ready: mpsc::Receiver<()>,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    readers: Vec<JoinHandle<()>>,
}

impl ServerProcess {
    fn spawn(mut command: Command, label: &str) -> Result<Self, String> {
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

    fn wait_ready(&mut self) -> Result<(), String> {
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

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let status = self.guard.wait_for_exit(timeout)?;
        self.join_readers();
        Ok(status)
    }

    fn kill_now(&mut self) {
        self.guard.kill_now();
        self.join_readers();
    }

    fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }

    fn stdout_snapshot(&self) -> String {
        lock_snapshot(&self.stdout)
    }

    fn stderr_snapshot(&self) -> String {
        lock_snapshot(&self.stderr)
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.guard.kill_now();
    }
}

fn spawn_line_reader<R>(
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

fn lock_snapshot(buffer: &Arc<Mutex<String>>) -> String {
    match buffer.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

struct RunResult {
    status: ExitStatus,
    elapsed: Duration,
    stderr: String,
}

/// Run a command with piped output, enforcing a wall-clock timeout.
fn run_timed(label: &str, mut command: Command, timeout: Duration) -> Result<RunResult, String> {
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
    let _stdout = stdout.map(join_reader).unwrap_or_default();
    let stderr = stderr.map(join_reader).unwrap_or_default();
    Ok(RunResult {
        status,
        elapsed,
        stderr,
    })
}

fn spawn_pipe_reader<R>(pipe: R) -> JoinHandle<String>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(pipe).read_to_string(&mut text);
        text
    })
}

fn join_reader(reader: JoinHandle<String>) -> String {
    reader.join().unwrap_or_default()
}

fn brief(text: &str) -> String {
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

fn free_udp_port() -> Result<u16, String> {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .map_err(|err| format!("failed to reserve a UDP port: {err}"))?;
    socket
        .local_addr()
        .map(|address| address.port())
        .map_err(|err| format!("failed to read the reserved UDP port: {err}"))
}

fn free_tcp_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("failed to reserve a TCP port: {err}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|err| format!("failed to read the reserved TCP port: {err}"))
}

fn which(name: &str) -> Option<PathBuf> {
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

fn is_executable(path: &Path) -> bool {
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

fn cftp_binary() -> Result<PathBuf, String> {
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

fn supports_one_shot(cftp: &Path) -> bool {
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

struct Ctx<'a> {
    cfg: &'a Config,
    cftp: &'a Path,
    identity: &'a Path,
    work: &'a Path,
    one_shot: bool,
}

struct Tools {
    cp: Option<PathBuf>,
    scp: Option<PathBuf>,
    curl: Option<PathBuf>,
    python: Option<PathBuf>,
    scp_enabled: bool,
    ftp_enabled: bool,
    http_enabled: bool,
}

fn detect_tools(cfg: &Config, notes: &mut Notes) -> Tools {
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

    Tools {
        cp,
        scp,
        curl,
        python,
        scp_enabled,
        ftp_enabled,
        http_enabled,
    }
}

fn measure_cftp(ctx: &Ctx<'_>, dataset: &Dataset) -> Result<Vec<Duration>, String> {
    let mut times = Vec::with_capacity(ctx.cfg.iterations);
    for iteration in 0..ctx.cfg.iterations {
        let destination = run_dir(ctx, dataset, "cftp", iteration);
        fs::create_dir_all(&destination)
            .map_err(|err| format!("failed to create {}: {err}", destination.display()))?;
        let elapsed = cftp_once(ctx, dataset, &destination)?;
        let name = &dataset.name;
        eprintln!("{name}: cftp put {:.3}s", elapsed.as_secs_f64());
        times.push(elapsed);
    }
    Ok(times)
}

fn cftp_once(ctx: &Ctx<'_>, dataset: &Dataset, destination: &Path) -> Result<Duration, String> {
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
        .arg("--quiet");
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
        .arg("--quiet");

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
    Ok(run.elapsed)
}

fn measure_copy_dir(ctx: &Ctx<'_>, dataset: &Dataset) -> Result<Vec<Duration>, String> {
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
    }
    Ok(times)
}

fn measure_cp(ctx: &Ctx<'_>, dataset: &Dataset, cp: &Path) -> Result<Vec<Duration>, String> {
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
    }
    Ok(times)
}

fn measure_scp(
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
    }
    Ok(times)
}

fn measure_ftp(
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
        }
        times.push(start.elapsed());
    }
    Ok(times)
}

fn measure_http(
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

fn curl_download(curl: &Path, url: &str, timeout: Duration) -> Result<Duration, String> {
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

fn wait_tcp_ready(port: u16, server: &mut ChildGuard) -> Result<(), String> {
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

fn run_dir(ctx: &Ctx<'_>, dataset: &Dataset, tool: &str, iteration: usize) -> PathBuf {
    let path = ctx
        .work
        .join("runs")
        .join(&dataset.name)
        .join(format!("{tool}-{iteration}"));
    let _ = fs::remove_dir_all(&path);
    path
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

struct Row {
    dataset: String,
    tool: &'static str,
    best: Duration,
    median: Duration,
    files: u64,
    bytes: u64,
}

impl Row {
    fn from_times(
        dataset: &Dataset,
        tool: &'static str,
        times: Vec<Duration>,
        files: u64,
        bytes: u64,
    ) -> Self {
        let mut times = times;
        times.sort();
        let best = times.first().copied().unwrap_or_default();
        let median = if times.is_empty() {
            Duration::ZERO
        } else if times.len() % 2 == 1 {
            times[times.len() / 2]
        } else {
            (times[times.len() / 2 - 1] + times[times.len() / 2]) / 2
        };
        Self {
            dataset: dataset.name.clone(),
            tool,
            best,
            median,
            files,
            bytes,
        }
    }
}

#[derive(Default)]
struct Notes(Vec<String>);

impl Notes {
    fn push(&mut self, note: impl Into<String>) {
        let note = note.into();
        if !self.0.contains(&note) {
            self.0.push(note);
        }
    }
}

fn print_table(rows: &[Row], iterations: usize) {
    println!();
    if iterations > 1 {
        println!(
            "Best of {iterations} runs per measurement (median also shown); MiB/s uses the best run."
        );
    } else {
        println!("Single run per measurement; MiB/s uses the wall time.");
    }
    println!(
        "{:<12} {:<16} {:>9} {:>9} {:>9} {:>8} {:>12}",
        "dataset", "tool", "best_s", "median_s", "MiB/s", "files", "bytes"
    );
    for row in rows {
        let best = row.best.as_secs_f64();
        let median = row.median.as_secs_f64();
        let throughput = if best > 0.0 {
            row.bytes as f64 / MIB / best
        } else {
            0.0
        };
        println!(
            "{:<12} {:<16} {:>9.3} {:>9.3} {:>9.2} {:>8} {:>12}",
            row.dataset, row.tool, best, median, throughput, row.files, row.bytes
        );
    }
}

fn print_notes(notes: &Notes) {
    if notes.0.is_empty() {
        return;
    }
    println!();
    println!("Skipped and failed comparisons:");
    for note in &notes.0 {
        println!("  {note}");
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn run() -> Result<(), String> {
    let cfg = parse_config()?;
    let cftp = cftp_binary()?;
    let one_shot = supports_one_shot(&cftp);
    if !one_shot {
        eprintln!(
            "note: {} does not advertise --one-shot; assuming its default serve mode is one-shot",
            cftp.display()
        );
    }

    eprintln!(
        "cftp bench: datasets [{}], {} iteration(s), {}",
        cfg.datasets.join(", "),
        cfg.iterations,
        if cfg.quick {
            "quick mode"
        } else {
            "default sizes"
        }
    );
    eprintln!("cftp binary: {}", cftp.display());

    let temp = tempfile::TempDir::new()
        .map_err(|err| format!("failed to create a temporary directory: {err}"))?;
    let workspace = if cfg.keep {
        let path = temp.keep();
        eprintln!("keeping workspace: {}", path.display());
        path
    } else {
        temp.path().to_path_buf()
    };
    let identity = workspace.join("identity");
    let datasets_base = workspace.join("datasets");
    let runs = workspace.join("runs");
    fs::create_dir_all(&datasets_base)
        .map_err(|err| format!("failed to create {}: {err}", datasets_base.display()))?;
    fs::create_dir_all(&runs)
        .map_err(|err| format!("failed to create {}: {err}", runs.display()))?;

    init_identity(&cftp, &identity)?;

    let mut datasets = Vec::new();
    for name in &cfg.datasets {
        eprintln!("generating {name}...");
        let dataset = generate_dataset(&datasets_base, name, &cfg)?;
        eprintln!(
            "  {name}: {} files, {} bytes ({:.2} MiB)",
            dataset.files,
            dataset.bytes,
            dataset.bytes as f64 / MIB
        );
        datasets.push(dataset);
    }

    let mut notes = Notes::default();
    let tools = detect_tools(&cfg, &mut notes);
    let ctx = Ctx {
        cfg: &cfg,
        cftp: &cftp,
        identity: &identity,
        work: &workspace,
        one_shot,
    };

    let mut rows = Vec::new();
    for dataset in &datasets {
        eprintln!("measuring {}...", dataset.name);
        match bench_dataset(&ctx, dataset, &tools, &mut notes) {
            Ok(mut measured) => rows.append(&mut measured),
            Err(err) => {
                print_table(&rows, cfg.iterations);
                print_notes(&notes);
                return Err(format!("{}: {err}", dataset.name));
            }
        }
    }

    print_table(&rows, cfg.iterations);
    print_notes(&notes);
    Ok(())
}

fn init_identity(cftp: &Path, identity: &Path) -> Result<(), String> {
    let mut command = Command::new(cftp);
    command
        .arg("identity")
        .arg("init")
        .arg("--dir")
        .arg(identity);
    let result = run_timed("cftp identity init", command, Duration::from_secs(60))?;
    if !result.status.success() {
        return Err(format!(
            "cftp identity init failed ({}): {}",
            result.status,
            brief(&result.stderr)
        ));
    }
    Ok(())
}

fn bench_dataset(
    ctx: &Ctx<'_>,
    dataset: &Dataset,
    tools: &Tools,
    notes: &mut Notes,
) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();

    let times = measure_cftp(ctx, dataset)?;
    rows.push(Row::from_times(
        dataset,
        "cftp put",
        times,
        dataset.files,
        dataset.bytes,
    ));

    let times = measure_copy_dir(ctx, dataset)?;
    rows.push(Row::from_times(
        dataset,
        "std copy",
        times,
        dataset.files,
        dataset.bytes,
    ));

    if let Some(cp) = tools.cp.as_deref() {
        match measure_cp(ctx, dataset, cp) {
            Ok(times) => rows.push(Row::from_times(
                dataset,
                "cp -a",
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("cp -a: failed on {} ({err})", dataset.name)),
        }
    }

    if let (Some(scp), true, Some(target)) = (
        tools.scp.as_deref(),
        tools.scp_enabled,
        ctx.cfg.ssh_target.as_deref(),
    ) {
        match measure_scp(ctx, dataset, scp, target) {
            Ok(times) => rows.push(Row::from_times(
                dataset,
                "scp",
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("scp: failed on {} ({err})", dataset.name)),
        }
    }

    if let (Some(curl), true, Some(url)) = (
        tools.curl.as_deref(),
        tools.ftp_enabled,
        ctx.cfg.ftp_url.as_deref(),
    ) {
        match measure_ftp(ctx, dataset, curl, url) {
            Ok(times) => rows.push(Row::from_times(
                dataset,
                "ftp (upload)",
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("ftp: failed on {} ({err})", dataset.name)),
        }
    }

    if let (Some(curl), true) = (tools.curl.as_deref(), tools.http_enabled) {
        let large = dataset.root.join("large.bin");
        if large.is_file() {
            let size = fs::metadata(&large)
                .map_err(|err| format!("failed to inspect {}: {err}", large.display()))?
                .len();
            match measure_http(
                ctx,
                dataset,
                curl,
                tools.python.as_deref(),
                ctx.cfg.http_url.as_deref(),
            ) {
                Ok(times) => rows.push(Row::from_times(dataset, "http (download)", times, 1, size)),
                Err(err) => notes.push(format!("http: failed on {} ({err})", dataset.name)),
            }
        } else {
            notes.push(format!(
                "http: skipped (dataset {} has no large file)",
                dataset.name
            ));
        }
    }

    Ok(rows)
}
