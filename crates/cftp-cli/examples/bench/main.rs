#![forbid(unsafe_code)]
//! Self-contained performance harness for cftp.
//!
//! The harness generates four datasets in a temporary directory, transfers
//! each one over loopback with `cftp serve`/`cftp put` (mutual TLS), and
//! compares the result with local recursive copies and, when the relevant
//! tools and targets exist, with `scp`, SFTP, HTTPS, FTP, and HTTP.
//!
//! The release gate compares cftp with peers that also authenticate and
//! encrypt the payload (`scp`, SFTP, HTTPS). FTP and HTTP are measured and
//! published as plaintext reference rows but never decide the gate: they do no
//! encryption or integrity checking, so matching them is not a like-for-like
//! target.
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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MIB: f64 = 1024.0 * 1024.0;
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const SERVER_EXIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Tool labels are part of the stable output and are matched by the gate.
const TOOL_CFTP: &str = "cftp put";
const TOOL_SCP: &str = "scp";
const TOOL_SFTP: &str = "sftp (upload)";
const TOOL_HTTPS: &str = "https (upload)";
const TOOL_FTP: &str = "ftp (upload)";
const TOOL_HTTP: &str = "http (upload)";

/// Peers that authenticate and encrypt the transfer, as cftp does.
///
/// The gate compares cftp with these. FTP and HTTP are measured and published
/// but deliberately excluded: they send the payload in the clear, so a
/// comparison against them is not a like-for-like one, and no encrypted
/// protocol can match a kernel-to-file plaintext copy on loopback.
const SECURE_TOOLS: [&str; 3] = [TOOL_SCP, TOOL_SFTP, TOOL_HTTPS];

/// Plaintext peers, reported for reference only.
const PLAINTEXT_TOOLS: [&str; 2] = [TOOL_FTP, TOOL_HTTP];

/// Processes that must be killed if the harness panics. The release profile
/// uses `panic = "abort"`, so destructors do not run on panic; the panic hook
mod args;
mod datasets;
mod harness;
mod measure;
mod report;

use args::*;
use datasets::*;
use harness::*;
use measure::*;
use report::*;

fn main() {
    install_panic_hook();
    if let Err(err) = run() {
        eprintln!("bench: {err}");
        std::process::exit(1);
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
    let identity = match (&cfg.endpoint, &cfg.cert_dir) {
        (Some(_), Some(dir)) => dir.clone(),
        (Some(_), None) => {
            cftp_core::identity::IdentityPaths::user()
                .map_err(|err| {
                    format!("could not locate the default certificate directory: {err}")
                })?
                .dir
        }
        (None, _) => workspace.join("identity"),
    };
    let datasets_base = workspace.join("datasets");
    let runs = workspace.join("runs");
    fs::create_dir_all(&datasets_base)
        .map_err(|err| format!("failed to create {}: {err}", datasets_base.display()))?;
    fs::create_dir_all(&runs)
        .map_err(|err| format!("failed to create {}: {err}", runs.display()))?;

    if cfg.endpoint.is_none() {
        init_identity(&cftp, &identity)?;
    }
    eprintln!("certificates: {}", identity.display());

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
    if let Some(endpoint) = &cfg.endpoint {
        notes.push(format!(
            "cftp: measured against {endpoint}; received files were not verified and were \
             not removed from the server"
        ));
    }
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

    let comparisons = compare_gate(&rows, &datasets, cfg.gate_tolerance_pct);
    let gate_passed = if cfg.gate {
        print_gate(&comparisons, cfg.gate_tolerance_pct)
    } else {
        comparisons.iter().all(|comparison| comparison.passed)
    };
    write_results(&cfg, &rows, &comparisons, &datasets, gate_passed)?;
    if let Some(path) = &cfg.json {
        eprintln!("wrote {}", path.display());
    }
    if let Some(path) = &cfg.markdown {
        eprintln!("wrote {}", path.display());
    }
    if let Some(path) = &cfg.readme_block {
        eprintln!("wrote {}", path.display());
    }

    if cfg.gate && !gate_passed {
        return Err(format!(
            "benchmark gate failed: cftp must be within {:.1}% of the fastest \
             security-equivalent peer on every dataset (see the table above)",
            cfg.gate_tolerance_pct
        ));
    }
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
        TOOL_CFTP,
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
                TOOL_SCP,
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("scp: failed on {} ({err})", dataset.name)),
        }
    }

    if let (Some(sftp), true, Some(target)) = (
        tools.sftp.as_deref(),
        tools.sftp_enabled,
        ctx.cfg.sftp_target.as_deref(),
    ) {
        match measure_sftp(ctx, dataset, sftp, target) {
            Ok(times) => rows.push(Row::from_times(
                dataset,
                TOOL_SFTP,
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("sftp: failed on {} ({err})", dataset.name)),
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
                TOOL_FTP,
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("ftp: failed on {} ({err})", dataset.name)),
        }
    }

    if let (Some(curl), true, Some(url)) = (
        tools.curl.as_deref(),
        tools.http_put_enabled,
        ctx.cfg.http_put_url.as_deref(),
    ) {
        match measure_web_put(
            ctx,
            dataset,
            curl,
            url,
            None,
            "HTTP",
            ctx.cfg.http_put_root.as_deref(),
        ) {
            Ok(times) => rows.push(Row::from_times(
                dataset,
                TOOL_HTTP,
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("http upload: failed on {} ({err})", dataset.name)),
        }
    }

    if let (Some(curl), true, Some(url)) = (
        tools.curl.as_deref(),
        tools.https_put_enabled,
        ctx.cfg.https_put_url.as_deref(),
    ) {
        match measure_web_put(
            ctx,
            dataset,
            curl,
            url,
            ctx.cfg.https_ca.as_deref(),
            "HTTPS",
            ctx.cfg.https_put_root.as_deref(),
        ) {
            Ok(times) => rows.push(Row::from_times(
                dataset,
                TOOL_HTTPS,
                times,
                dataset.files,
                dataset.bytes,
            )),
            Err(err) => notes.push(format!("https upload: failed on {} ({err})", dataset.name)),
        }
    }

    // The download comparison is informational. A gate run already measures
    // HTTP uploads, and a second local HTTP server would only add noise.
    if !ctx.cfg.gate
        && let (Some(curl), true) = (tools.curl.as_deref(), tools.http_enabled)
    {
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
