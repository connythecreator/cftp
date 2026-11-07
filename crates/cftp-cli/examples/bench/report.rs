use super::*;

pub(super) struct Row {
    pub(super) dataset: String,
    pub(super) tool: &'static str,
    pub(super) best: Duration,
    pub(super) median: Duration,
    pub(super) files: u64,
    pub(super) bytes: u64,
}

impl Row {
    pub(super) fn from_times(
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
pub(super) struct Notes(Vec<String>);

impl Notes {
    pub(super) fn push(&mut self, note: impl Into<String>) {
        let note = note.into();
        if !self.0.contains(&note) {
            self.0.push(note);
        }
    }
}

pub(super) fn print_table(rows: &[Row], iterations: usize) {
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

pub(super) fn print_notes(notes: &Notes) {
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
// Release gate and machine-readable results
// ---------------------------------------------------------------------------

pub(super) struct Comparison {
    pub(super) dataset: String,
    pub(super) cftp_median: Option<Duration>,
    pub(super) fastest_tool: Option<&'static str>,
    pub(super) fastest_median: Option<Duration>,
    /// cftp time as a percentage of the fastest competitor; 100 means equal.
    pub(super) ratio_pct: Option<f64>,
    pub(super) missing: Vec<&'static str>,
    pub(super) passed: bool,
}

pub(super) fn compare_gate(
    rows: &[Row],
    datasets: &[Dataset],
    tolerance_pct: f64,
) -> Vec<Comparison> {
    datasets
        .iter()
        .map(|dataset| {
            let cftp = find_row(rows, &dataset.name, TOOL_CFTP).map(|row| row.median);
            let measured: Vec<(&'static str, Duration)> = SECURE_TOOLS
                .iter()
                .filter_map(|tool| {
                    find_row(rows, &dataset.name, tool).map(|row| (*tool, row.median))
                })
                .collect();
            let missing = SECURE_TOOLS
                .iter()
                .copied()
                .filter(|tool| {
                    !measured
                        .iter()
                        .any(|(measured_tool, _)| measured_tool == tool)
                })
                .collect::<Vec<_>>();
            let fastest = measured.iter().min_by_key(|(_, median)| *median).copied();
            let ratio_pct = match (cftp, fastest) {
                (Some(cftp), Some((_, fastest))) if fastest > Duration::ZERO => {
                    Some(cftp.as_secs_f64() / fastest.as_secs_f64() * 100.0)
                }
                _ => None,
            };
            let passed = cftp.is_some()
                && missing.is_empty()
                && ratio_pct.is_some_and(|ratio| ratio <= 100.0 + tolerance_pct);
            Comparison {
                dataset: dataset.name.clone(),
                cftp_median: cftp,
                fastest_tool: fastest.map(|(tool, _)| tool),
                fastest_median: fastest.map(|(_, median)| median),
                ratio_pct,
                missing,
                passed,
            }
        })
        .collect()
}

pub(super) fn find_row<'a>(rows: &'a [Row], dataset: &str, tool: &str) -> Option<&'a Row> {
    rows.iter()
        .find(|row| row.dataset == dataset && row.tool == tool)
}

/// Whether a row's transfer authenticates and encrypts its payload.
pub(super) fn tool_is_encrypted(tool: &str) -> bool {
    tool == TOOL_CFTP || SECURE_TOOLS.contains(&tool)
}

/// Short display name for a tool label.
pub(super) fn short_tool(tool: &str) -> &str {
    match tool {
        TOOL_CFTP => "cftp",
        TOOL_SCP => "scp",
        TOOL_SFTP => "SFTP",
        TOOL_HTTPS => "HTTPS",
        TOOL_FTP => "FTP",
        TOOL_HTTP => "HTTP",
        other => other,
    }
}

pub(super) fn print_gate(comparisons: &[Comparison], tolerance_pct: f64) -> bool {
    println!();
    println!(
        "Release gate: cftp must be within {tolerance_pct:.1}% of the fastest \
         security-equivalent peer (scp, SFTP, HTTPS) on every dataset."
    );
    println!(
        "FTP and HTTP send the payload in plaintext; they are reported for reference \
         and are not part of the pass/fail decision."
    );
    let mut passed = !comparisons.is_empty();
    for comparison in comparisons {
        passed &= comparison.passed;
        let cftp = comparison
            .cftp_median
            .map(format_seconds)
            .unwrap_or_else(|| "missing".to_string());
        let fastest = match (comparison.fastest_tool, comparison.fastest_median) {
            (Some(tool), Some(median)) => format!("{tool} {}", format_seconds(median)),
            _ => "missing".to_string(),
        };
        let ratio = comparison
            .ratio_pct
            .map(|ratio| format!("{ratio:.1}%"))
            .unwrap_or_else(|| "-".to_string());
        let verdict = if comparison.passed { "PASS" } else { "FAIL" };
        println!(
            "  {:<12} cftp {:<9} fastest {:<20} cftp/fastest {:<8} {verdict}",
            comparison.dataset, cftp, fastest, ratio
        );
        if !comparison.missing.is_empty() {
            println!(
                "               missing mandatory comparison(s): {}",
                comparison.missing.join(", ")
            );
        }
    }
    println!("Gate result: {}", if passed { "PASS" } else { "FAIL" });
    passed
}

pub(super) fn format_seconds(duration: Duration) -> String {
    format!("{:.3}s", duration.as_secs_f64())
}

pub(super) fn mib_per_second(bytes: u64, duration: Duration) -> f64 {
    if duration.is_zero() {
        0.0
    } else {
        bytes as f64 / MIB / duration.as_secs_f64()
    }
}

pub(super) fn write_results(
    cfg: &Config,
    rows: &[Row],
    comparisons: &[Comparison],
    dataset_meta: &[Dataset],
    gate_passed: bool,
) -> Result<(), String> {
    if let Some(path) = &cfg.json {
        let json = render_json(cfg, rows, comparisons, gate_passed);
        write_output(path, &json)?;
    }
    if let Some(path) = &cfg.markdown {
        let markdown = render_markdown(cfg, rows, comparisons, dataset_meta, gate_passed);
        write_output(path, &markdown)?;
    }
    if let Some(path) = &cfg.readme_block {
        let fragment =
            render_markdown_body(cfg, rows, comparisons, dataset_meta, gate_passed, "###");
        write_output(path, &fragment)?;
    }
    Ok(())
}

pub(super) fn write_output(path: &Path, contents: &str) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
    }
    fs::write(path, contents).map_err(|err| format!("failed to write {}: {err}", path.display()))
}

pub(super) fn render_json(
    cfg: &Config,
    rows: &[Row],
    comparisons: &[Comparison],
    gate_passed: bool,
) -> String {
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str("  \"schema\": 1,\n");
    out.push_str(&format!(
        "  \"cftp_version\": \"{}\",\n",
        env!("CARGO_PKG_VERSION")
    ));
    out.push_str(&format!(
        "  \"generated_at_unix\": {},\n",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0)
    ));
    out.push_str(&format!(
        "  \"label\": {},\n",
        json_string(cfg.label.as_deref().unwrap_or("unspecified environment"))
    ));
    out.push_str(&format!("  \"iterations\": {},\n", cfg.iterations));
    out.push_str(&format!(
        "  \"gate_tolerance_pct\": {},\n",
        cfg.gate_tolerance_pct
    ));
    out.push_str(&format!(
        "  \"gate\": {{\"enabled\": {}, \"passed\": {}, \"tools\": [{}]}},\n",
        cfg.gate,
        gate_passed,
        SECURE_TOOLS
            .iter()
            .map(|tool| json_string(tool))
            .collect::<Vec<_>>()
            .join(", ")
    ));

    out.push_str("  \"rows\": [\n");
    for (index, row) in rows.iter().enumerate() {
        out.push_str("    {");
        out.push_str(&format!(
            "\"dataset\": {}, \"tool\": {}, \"encrypted\": {}, \"best_s\": {:.6}, \
             \"median_s\": {:.6}, \"mib_per_s\": {:.3}, \"files\": {}, \"bytes\": {}",
            json_string(&row.dataset),
            json_string(row.tool),
            tool_is_encrypted(row.tool),
            row.best.as_secs_f64(),
            row.median.as_secs_f64(),
            mib_per_second(row.bytes, row.median),
            row.files,
            row.bytes
        ));
        out.push('}');
        if index + 1 < rows.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str("  ],\n");

    out.push_str("  \"comparisons\": [\n");
    for (index, comparison) in comparisons.iter().enumerate() {
        out.push_str("    {");
        out.push_str(&format!(
            "\"dataset\": {}, \"cftp_median_s\": {}, \"fastest_tool\": {}, \
             \"fastest_median_s\": {}, \"ratio_pct\": {}, \"missing\": [{}], \"passed\": {}",
            json_string(&comparison.dataset),
            json_optional_seconds(comparison.cftp_median),
            json_optional_string(comparison.fastest_tool),
            json_optional_seconds(comparison.fastest_median),
            comparison
                .ratio_pct
                .map(|ratio| format!("{ratio:.3}"))
                .unwrap_or_else(|| "null".to_string()),
            comparison
                .missing
                .iter()
                .map(|tool| json_string(tool))
                .collect::<Vec<_>>()
                .join(", "),
            comparison.passed
        ));
        out.push('}');
        if index + 1 < comparisons.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str("  ]\n");
    out.push_str("}\n");
    out
}

pub(super) fn json_optional_seconds(value: Option<Duration>) -> String {
    value
        .map(|duration| format!("{:.6}", duration.as_secs_f64()))
        .unwrap_or_else(|| "null".to_string())
}

pub(super) fn json_optional_string(value: Option<&str>) -> String {
    value.map(json_string).unwrap_or_else(|| "null".to_string())
}

pub(super) fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if character < '\u{20}' => {
                out.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => out.push(character),
        }
    }
    out.push('"');
    out
}

pub(super) fn render_markdown(
    cfg: &Config,
    rows: &[Row],
    comparisons: &[Comparison],
    datasets: &[Dataset],
    gate_passed: bool,
) -> String {
    let mut out = String::new();
    out.push_str("# Benchmarks\n\n");
    out.push_str(&render_markdown_body(
        cfg,
        rows,
        comparisons,
        datasets,
        gate_passed,
        "##",
    ));
    out.push_str("\n## How this is measured\n\n");
    out.push_str(
        "- cftp, scp, SFTP, HTTPS, FTP, and HTTP run on the same machine over loopback, so no \
         network bottleneck hides protocol differences. The release gate runs them in one \
         Docker container with local sshd, vsftpd, and nginx servers.\n",
    );
    out.push_str(
        "- cftp, scp, and SFTP upload a whole tree over one connection; FTP, HTTP, and HTTPS \
         upload each file with a separate `curl` process. HTTP and HTTPS use nginx with \
         `dav_methods PUT` and `create_full_put_path`; HTTPS adds TLS 1.3 with a certificate \
         that curl verifies.\n",
    );
    out.push_str(
        "- Datasets are deterministic pseudo-random bytes. They are intentionally \
         incompressible, so the comparison does not credit cftp for zstd compression. \
         Source trees and logs compress better and usually transfer faster.\n",
    );
    out.push_str(
        "- Each row is the median of the configured iterations. Verification happens after \
         the timer stops. Every run uploads to a fresh destination.\n",
    );
    out.push_str(&format!(
        "- The release gate requires cftp to be within {:.1}% of the fastest \
         security-equivalent peer (scp, SFTP, HTTPS) on every dataset. A run with a missing \
         peer fails the gate.\n",
        cfg.gate_tolerance_pct
    ));
    out.push_str(
        "- FTP and HTTP are plaintext: they encrypt nothing and verify no integrity. They are \
         published as reference points for the same hardware and are excluded from the gate, \
         because a kernel-to-file plaintext copy is not a fair comparison for an encrypted, \
         integrity-checked protocol.\n",
    );
    out
}

/// Render the results tables. `section` is the heading prefix used for the
/// two transport sections: `##` for a standalone page, `###` for the fragment
/// spliced into README.md under its own `## Benchmarks` heading.
pub(super) fn render_markdown_body(
    cfg: &Config,
    rows: &[Row],
    comparisons: &[Comparison],
    datasets: &[Dataset],
    gate_passed: bool,
    section: &str,
) -> String {
    let mut out = String::new();
    let gate_text = if cfg.gate {
        if gate_passed { "passed" } else { "failed" }
    } else {
        "not enforced"
    };
    out.push_str(&format!(
        "Generated by the cftp benchmark harness (cftp {}) on {}. Median of {} run(s); \
         lower is better. Gate: **{gate_text}** (cftp within {:.1}% of the fastest \
         security-equivalent peer).\n\n",
        env!("CARGO_PKG_VERSION"),
        cfg.label.as_deref().unwrap_or("an unspecified environment"),
        cfg.iterations,
        cfg.gate_tolerance_pct
    ));
    let cell = |dataset: &Dataset, tool: &str| {
        find_row(rows, &dataset.name, tool)
            .map(|row| {
                format!(
                    "{} ({:.1} MiB/s)",
                    format_seconds(row.median),
                    mib_per_second(row.bytes, row.median)
                )
            })
            .unwrap_or_else(|| "not measured".to_string())
    };
    let size = |dataset: &Dataset| {
        format!(
            "{} ({:.1} MiB, {} {})",
            dataset.name,
            dataset.bytes as f64 / MIB,
            dataset.files,
            if dataset.files == 1 { "file" } else { "files" },
        )
    };
    let fastest = |dataset: &Dataset| {
        comparisons
            .iter()
            .find(|comparison| comparison.dataset == dataset.name)
            .and_then(|comparison| comparison.fastest_tool)
            .map(short_tool)
            .unwrap_or("unknown")
            .to_string()
    };

    out.push_str(&format!("{section} Encrypted transports (gate)\n\n"));
    out.push_str("| Dataset | cftp | scp | SFTP | HTTPS | Fastest secure peer |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: | --- |\n");
    for dataset in datasets {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            size(dataset),
            cell(dataset, TOOL_CFTP),
            cell(dataset, TOOL_SCP),
            cell(dataset, TOOL_SFTP),
            cell(dataset, TOOL_HTTPS),
            fastest(dataset)
        ));
    }

    out.push_str(&format!(
        "\n{section} Plaintext reference (not gated)\n\n\
         These peers send the payload in the clear. They are measured to show what the same \
         hardware does without encryption or integrity protection, and are excluded from the \
         gate.\n\n"
    ));
    out.push_str(&format!(
        "| Dataset | {} |\n| --- | {} |\n",
        PLAINTEXT_TOOLS
            .iter()
            .map(|tool| short_tool(tool))
            .collect::<Vec<_>>()
            .join(" | "),
        PLAINTEXT_TOOLS
            .iter()
            .map(|_| "---:")
            .collect::<Vec<_>>()
            .join(" | "),
    ));
    for dataset in datasets {
        let cells = PLAINTEXT_TOOLS
            .iter()
            .map(|tool| cell(dataset, tool))
            .collect::<Vec<_>>()
            .join(" | ");
        out.push_str(&format!("| {} | {cells} |\n", size(dataset)));
    }
    out
}
