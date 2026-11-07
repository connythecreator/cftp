# cftp benchmark harness

`crates/cftp-cli/examples/bench/` is a self-contained performance harness for
cftp (entry point `main.rs`, with argument parsing, datasets, process handling,
measurement, and reporting in sibling modules). It generates datasets in a
temporary directory, transfers them over
loopback with mutual TLS, and compares the result with local recursive copies
and, when the relevant tools and targets are available, with `scp`, SFTP,
HTTPS, FTP, and HTTP.

External comparisons are opt-in. Missing tools, missing targets, and failures
in external servers are reported as skipped or failed and never fail the run.

## Release gate

`make bench-gate` runs the harness in one Docker container over loopback and
fails the build unless cftp is within 5 % of the fastest **security-equivalent**
peer (scp, SFTP, HTTPS) on every dataset. FTP and HTTP are measured and
published as plaintext reference rows; they are excluded from the gate because
they neither encrypt nor integrity-check the payload, so a kernel-to-file
plaintext copy is not a like-for-like target for an authenticated, encrypted
protocol.

The gate needs at least three iterations per measurement, all six peers
configured, and it fails when any of them is missing or fails on a dataset.
Results are written to `bench/results/latest.{json,md}` and the Markdown
fragment is spliced into `README.md` and `docs/wiki/Benchmarks.md` by
`bench/update-readme.sh`.

The gate's default dataset list is `many-small,large,mixed,huge`. Every upload
is removed as soon as it has been measured (`--cleanup`), so the 1 GiB dataset
costs one generated source plus one destination copy instead of one copy per
peer and iteration. `BENCH_HUGE_FILES` and `BENCH_HUGE_SIZE` adjust its
footprint; a machine with little free disk can leave `huge` out of
`BENCH_DATASETS`.

## Quick start

```sh
# Build the release binary and run the small, fast datasets.
make bench BENCH_ARGS="--quick"

# Or invoke the example directly after building the binary.
cargo build --release -p cftp-cli --bin cftp
cargo run --release -p cftp-cli --example bench -- --quick
```

The example locates the `cftp` binary through `CARGO_BIN_EXE_cftp` when Cargo
provides it, then falls back to the matching `target/<profile>/cftp`. Build the
binary first (as `make bench` does); the harness itself does not build it.

## Datasets

All datasets are written under a temporary workspace and are removed at the
end unless `--keep` is passed.

| Dataset      | Default                                             | `--quick`                                          |
| ------------ | --------------------------------------------------- | -------------------------------------------------- |
| `many-small` | 2000 files x 4 KiB (8 MiB)                          | 200 files x 4 KiB (0.78 MiB)                       |
| `large`      | 1 file x 256 MiB                                    | 1 file x 4 MiB                                     |
| `huge`       | 1 file x 1 GiB                                      | 1 file x 64 MiB                                    |
| `mixed`      | 8 nested directories, 137 files, about 33 MiB       | 4 nested directories, 21 files, about 2.31 MiB     |

`mixed` contains a top-level `large.bin`, nested small files, and medium files
so that both the packed and chunked paths are exercised. File contents are
deterministic pseudo-random bytes, which keeps the comparison closer to real
data than highly compressible zeroes while remaining reproducible.

Sizes and counts are configurable with the flags and environment variables
listed at the end of this page. Sizes accept plain bytes or `KiB`/`MiB`/`GiB`
suffixes (`KB`/`MB`/`GB` use powers of 1000).

## What is measured

For every dataset the harness runs:

- **`cftp put`**: `cftp identity init` creates a temporary mTLS identity, a
  fresh `cftp serve --one-shot --quiet --aggressive` listens on an ephemeral
  loopback UDP port, and
  `cftp put <src> cftp://127.0.0.1:<port>/incoming --quiet --aggressive`
  transfers the tree. The server is allowed to start and report readiness
  before the timer starts. After the client exits, the server exit is awaited
  and every received file is compared byte-for-byte with the source.
- **`std copy`**: the same tree is copied with a recursive function that uses
  only `std::fs`, timed the same way and verified byte-for-byte.
- **`cp -a`**: when `cp` exists on `PATH`, the tree is copied with `cp -a` and
  verified byte-for-byte.

Two wall-clock values are reported:

- `best_s`: the fastest run, used for the MiB/s figure.
- `median_s`: the median run, useful for spotting noise.

`MiB/s` is `source bytes / 1048576 / best_s`. The source bytes are logical file
bytes, not bytes on the wire. Verification happens after the timer stops so it
does not affect the measurement.

The server's startup and readiness wait are excluded from `cftp put` timing.
Server startup still adds to the harness's total wall-clock time, and each
iteration starts a new server.

cftp runs with the **aggressive hardware profile** (`--aggressive` on both
`cftp serve` and `cftp put`, and on the client in `--endpoint` mode). The
harness benchmarks peak throughput rather than cftp's conservative default
tuning, so these rows are not representative of a default-profile deployment.

### Against a running server

`--endpoint HOST:PORT` (or `CFTP_BENCH_ENDPOINT`) measures cftp against a
server that is already running, which is how to include a real network path:

```sh
cargo run --release -p cftp-cli --example bench -- \
  --endpoint 192.0.2.10:4433 --cert-dir ~/.config/cftp --iterations 3
```

No server is spawned and no identity is generated. The client presents
`identity.pem` and `identity.key` from `--cert-dir` (`CFTP_BENCH_CERT_DIR`,
default `~/.config/cftp`) and verifies the server with `ca.pem`, so both peers
must trust the same CA. Each iteration uploads to
`cftp-bench-<pid>-<dataset>-<iteration>` under the server root, preserving the
source directory name. Received bytes are **not** verified and are left on the
server: the harness has no command channel to the peer. Point `--ssh-target`
at the same host to compare against `scp` on that path, and remove the uploads
yourself.

## Optional external comparisons

### scp

Enabled only when an SSH target is supplied and `scp` is on `PATH`:

```sh
cargo run --release -p cftp-cli --example bench -- \
  --ssh-target user@host [--ssh-dest /tmp]
```

The target can also be set with `CFTP_BENCH_SSH`. The harness runs
`scp -r -q -o BatchMode=yes <src> <target>:<remote>`, where `<remote>` is
`<ssh-dest>/cftp-bench-<pid>-<dataset>-<iteration>`. `BatchMode=yes` prevents a
password prompt from hanging the run; key or agent authentication is required.
Remote bytes are not verified because the harness cannot assume an SSH command
channel is available.

If no target is given it prints `scp: skipped (no ssh target)`. If the target is
given but `scp` is missing it prints `scp: skipped (scp not found on PATH)`.

### SFTP

Enabled only when `--sftp-target` (or `CFTP_BENCH_SFTP`) is set and `sftp` is
on `PATH`:

```sh
cargo run --release -p cftp-cli --example bench -- \
  --sftp-target user@host [--sftp-dest /tmp]
```

The harness writes a batch file and runs
`sftp -q -b <batch> -o BatchMode=yes <target>` with a single
`put -R <src> <remote>` command, so the whole tree travels over one SSH
connection like `scp -r`. `<remote>` is
`<sftp-dest>/cftp-bench-<pid>-<dataset>-<iteration>`. Host keys are checked
against the client's `known_hosts`; provision it before running, as you would
for `scp`. Remote bytes are not verified.

### HTTPS

Enabled only when `--https-put-url` (or `CFTP_BENCH_HTTPS_PUT_URL`) is set and
`curl` is on `PATH`. Every file is uploaded with
`curl --cacert <--https-ca> -T <file> <url>/<remote>/<relative-path>`, exactly
like the HTTP upload row but over TLS with certificate verification. Use
`--https-ca` (`CFTP_BENCH_HTTPS_CA`) when the server uses a private CA:

```sh
cargo run --release -p cftp-cli --example bench -- \
  --https-put-url https://host/uploads --https-ca ca.pem
```

Without `--https-ca`, curl uses its system trust store. The HTTPS row is part
of the release gate.

### FTP

Enabled only when `CFTP_BENCH_FTP_URL` (or `--ftp-url`) is set and `curl` is on
`PATH`:

```sh
CFTP_BENCH_FTP_URL=ftp://user:password@host/directory \
  cargo run --release -p cftp-cli --example bench --
```

The harness uploads every file individually with
`curl --ftp-create-dirs -T <file> <url>/<relative-path>`. Relative paths are
preserved, so nested trees are recreated on the server. Uploads are not
verified remotely. Process startup for each file is included in the timing,
which matters most for `many-small`.

If the URL is not set it prints `ftp: skipped (CFTP_BENCH_FTP_URL not set)`.

### HTTP

HTTP measures a download, not an upload. The target is always the dataset's
top-level `large.bin`, so `many-small` is skipped with
`http: skipped (dataset many-small has no large file)`.

- With `CFTP_BENCH_HTTP_URL` (or `--http-url`) and `curl`, the harness downloads
  `<url>/large.bin` with `curl --output /dev/null` and times only the curl
  process. This can point at any HTTP server that hosts the file.
- Without `CFTP_BENCH_HTTP_URL`, and when both `curl` and `python3` exist, the
  harness starts `python3 -m http.server <ephemeral-port> --directory <src>`,
  waits until it accepts connections, downloads `large.bin` the same way, and
  then kills the server. The Python server is not part of the timed interval.
- If `curl` is missing, it prints `http: skipped (curl not found on PATH)`. If
  neither a URL nor `python3` is available, it prints
  `http: skipped (python3 not found on PATH and CFTP_BENCH_HTTP_URL not set)`.

## Output and exit status

Results are printed as a stable table:

```text
dataset      tool                best_s  median_s     MiB/s    files        bytes
large        cftp put             0.071     0.071     56.62        1      4194304
large        std copy             0.002     0.002   2302.49        1      4194304
```

`files` and `bytes` describe the source tree for cftp, `std copy`, `cp`, `scp`,
SFTP, FTP, HTTP, and HTTPS. The informational `http (download)` row describes
only the downloaded `large.bin`.

Outside `--gate`, the harness exits non-zero only when the cftp transfer or the
`std::fs` copy fails (including verification). Missing external tools,
unreachable scp targets, and FTP/HTTP/HTTPS errors are recorded in the notes
beneath the table and the run still exits zero.

With `--gate`, a missing or failed security-equivalent peer (scp, SFTP, HTTPS)
fails the gate, and so does a cftp median more than `--gate-tolerance` percent
slower than the fastest of them on any dataset.

## Cleanup and safety

- Every server is held in a child guard that kills and reaps it on drop.
- The release profile uses `panic = "abort"`, so destructors do not run on a
  panic. A registry plus a panic hook kills registered server processes before
  the abort.
- All client and curl invocations have a wall-clock timeout. The default grows
  with dataset size; override it with `--timeout <seconds>`.
- UDP and TCP ports are reserved by binding to `127.0.0.1:0`. There is a small
  race between releasing the port and the server binding it, which is
  acceptable for a benchmark.
- The workspace is a `tempfile::TempDir` and is deleted when the harness ends.
  Pass `--keep` to preserve it; the path is printed at startup.

## Fairness caveats

- **Compression**: datasets are pseudo-random and mostly incompressible, but
  cftp still pays zstd framing and checksum costs. Real-world text, source
  trees, and logs compress well and will show different relative throughput.
- **Loopback**: cftp, local copies, local HTTP, and (when run on the same host)
  scp/FTP avoid a real network. There is no RTT, loss, or bandwidth limit, and
  the operating system page cache can satisfy reads and writes faster than a
  real link would.
- **Different semantics**: cftp, scp, and SFTP upload a tree over one
  connection; FTP, HTTP, and HTTPS upload every file with a separate curl
  process; the informational HTTP row downloads one large file. cftp includes
  scan, compression, hashing, QUIC handshake, and disk writes; a local copy
  includes only file reads and writes. Treat cross-tool rows as coarse
  indicators rather than an apples-to-apples protocol comparison.
- **Plaintext peers**: FTP and HTTP are measured for context but are excluded
  from the release gate, because they provide no encryption or integrity
  checking. Only scp, SFTP, and HTTPS are security-equivalent to cftp.
- **One-shot startup**: each cftp iteration starts a new server and waits for
  readiness. Startup is excluded from `put` timing, but it is part of the
  harness's total runtime and affects small datasets more than large ones.
- **SSH/web setup**: scp and SFTP pay an SSH connection and authentication per
  run; FTP, HTTP, and HTTPS pay one curl process and one connection per file.
  cftp scans once and uses a single QUIC session.
- **Binary profile**: use `--release` or `make bench`. A debug `cftp` binary is
  dramatically slower and makes the cftp numbers meaningless.
- **Single-run noise**: filesystem cache state, CPU frequency scaling, and
  background load can dominate a single run. Use `--iterations 3` or more and
  compare the median.
- **Page cache**: generating a dataset leaves dirty pages queued for writeback,
  and the first peer measured would otherwise absorb that cost while later
  peers start clean. The harness runs `sync` before measuring each dataset so
  every peer begins from the same page-cache state.

## Flags and environment

| Flag                  | Environment                   | Purpose                                              |
| --------------------- | ----------------------------- | ---------------------------------------------------- |
| `--quick`             |                               | Use the small datasets above                         |
| `--iterations <N>`    | `CFTP_BENCH_ITERATIONS`       | Runs per measurement (default 1)                     |
| `--datasets <LIST>`   |                               | `many-small`, `large`, `mixed`, `huge`               |
| `--small-count <N>`   | `CFTP_BENCH_SMALL_COUNT`      | `many-small` file count                              |
| `--small-size <SIZE>` | `CFTP_BENCH_SMALL_SIZE`       | `many-small` file size                               |
| `--large-size <SIZE>` | `CFTP_BENCH_LARGE_SIZE`       | `large` file size                                    |
| `--huge-files <N>`    | `CFTP_BENCH_HUGE_FILES`       | `huge` file count (default 1)                        |
| `--huge-size <SIZE>`  | `CFTP_BENCH_HUGE_SIZE`        | `huge` file size (default 1GiB; quick: 64MiB)        |
| `--mixed-dirs <N>`    | `CFTP_BENCH_MIXED_DIRS`       | `mixed` top-level directories                        |
| `--mixed-per-dir <N>` | `CFTP_BENCH_MIXED_PER_DIR`    | `mixed` small files per directory                    |
| `--mixed-small-size`  | `CFTP_BENCH_MIXED_SMALL_SIZE` | `mixed` small file size                              |
| `--mixed-large-size`  | `CFTP_BENCH_MIXED_LARGE_SIZE` | `mixed` large file size                              |
| `--ssh-target`        | `CFTP_BENCH_SSH`              | Enable scp against `user@host`                       |
| `--ssh-dest`          | `CFTP_BENCH_SSH_DEST`         | Remote parent directory for scp (default `/tmp`)     |
| `--sftp-target`       | `CFTP_BENCH_SFTP`             | Enable SFTP against `user@host`                      |
| `--sftp-dest`         | `CFTP_BENCH_SFTP_DEST`        | Remote parent directory for SFTP (default `/tmp`)    |
| `--ftp-url`           | `CFTP_BENCH_FTP_URL`          | Enable FTP uploads to this base URL                  |
| `--http-put-url`      | `CFTP_BENCH_HTTP_PUT_URL`     | Upload every file with HTTP PUT to this base URL     |
| `--https-put-url`     | `CFTP_BENCH_HTTPS_PUT_URL`    | Upload every file with HTTPS PUT to this base URL    |
| `--https-ca`          | `CFTP_BENCH_HTTPS_CA`         | CA certificate used to verify the HTTPS peer         |
| `--http-put-root`     | `CFTP_BENCH_HTTP_PUT_ROOT`    | Filesystem directory behind `--http-put-url`         |
| `--https-put-root`    | `CFTP_BENCH_HTTPS_PUT_ROOT`   | Filesystem directory behind `--https-put-url`        |
| `--ftp-root`          | `CFTP_BENCH_FTP_ROOT`         | Filesystem directory behind `--ftp-url`              |
| `--cleanup`           | `CFTP_BENCH_CLEANUP`          | Delete each upload after measuring it                |
| `--http-url`          | `CFTP_BENCH_HTTP_URL`         | Download `large.bin` from this base URL              |
| `--endpoint <ADDR>`   | `CFTP_BENCH_ENDPOINT`         | Measure cftp against a running server at `HOST:PORT` |
| `--cert-dir <DIR>`    | `CFTP_BENCH_CERT_DIR`         | Certificate directory for `--endpoint`               |
| `--timeout <SECS>`    |                               | Per-command timeout (default scales with size)       |
| `--keep`              |                               | Keep the temporary workspace                         |
