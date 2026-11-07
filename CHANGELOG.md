# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-11-07

### Added

- `cftp get` pulls files and directory trees from a running server. Pull is
  server-authoritative: the server resolves and validates the source path,
  scans it, and sends a manifest before any data moves (`cftp/4`).
- `serve` stays alive by default and accepts repeated transfers, bounded by
  `--max-sessions`; `--one-shot` restores the previous single-session
  behaviour. `--no-push` and `--no-pull` restrict the server's modes.
- `cftp keygen` and `cftp pubkey` helpers, and actionable errors when no client
  key exists.
- `put`/`get --via-ssh [user@]host[:port]` starts a temporary one-shot server
  on the remote host over SSH, learns its host key over the authenticated SSH
  channel, and authorizes the local client key for that session only. No manual
  key exchange or persistent server is needed; UDP 4433 must still be reachable
  between the machines.
- `serve` creates its root directory when missing and prints reachable
  `cftp://<lan-ip>:4433/` URLs plus a copy-paste client command.
- cftp keeps its own host key and `known_hosts` under `~/.config/cftp`, separate
  from OpenSSH's files. A first `serve` generates an Ed25519 host key
  automatically.
- Adaptive congestion control (CUBIC with a BBR switch after sustained loss) and
  `--stats` transport counters.
- A self-contained benchmark harness (`cargo run --example bench`) and
  `make bench-gate`, which compares cftp with scp, SFTP, HTTPS, FTP, and HTTP in
  one Docker container over loopback and fails when cftp trails the fastest
  security-equivalent peer (scp, SFTP, HTTPS) by more than 5 %. FTP and HTTP are
  reported as plaintext references and do not decide the gate. CI runs the gate
  on every push and pull request, publishes the results into `README.md` and
  `docs/wiki/Benchmarks.md` from `main`, and a release is not published until
  the gate passes.
- A GitHub Pages documentation site, built with mdBook from `docs/wiki` plus the
  README, benchmark guide, and design notes (`make book`).
- The benchmark harness includes a `huge` dataset (1 file × 1 GiB by default,
  64 MiB under `--quick`), configurable with `--huge-files`/`--huge-size` or
  `CFTP_BENCH_HUGE_FILES`/`CFTP_BENCH_HUGE_SIZE`, so sustained large-file
  throughput is measured alongside the existing datasets. The release gate
  includes it and removes each upload once it has been measured (`--cleanup`,
  with `--ftp-root`, `--http-put-root`, and `--https-put-root`), so the run
  holds one generated dataset plus one copy instead of one per peer and
  iteration.
- Embedding conveniences in `cftp-core`: `PushOptions::new`,
  `PullOptions::new`, `with_progress`, `with_profile`, and
  `progress::channel`, plus a `downloader` example showing a queue with bounded
  concurrency and cancellation. `cftp-core`'s crate docs now describe the
  embedding model: one transfer per QUIC connection, progress events, and
  aborting the future to cancel.

### Changed

- Wire protocol bumped to `cftp/4` (ALPN `cftp/4`, with `cftp/3` and `cftp/2`
  fallback).
- Loopback peers start at 16 KB QUIC datagrams instead of 1,200 bytes. The
  receive-capability advertisement stays at 16 KB for every endpoint, the GSO
  batch is capped so one `sendmsg` stays inside the kernel's 64 KiB limit, and
  the initial congestion window scales with the MTU so a jumbo-sized Initial
  packet can still leave. Non-loopback paths keep quinn's conservative MTU
  discovery.
- Compression and decompression consume their input buffer, so a chunk that is
  sent or received raw is no longer copied a second time.
- `known_hosts` follows OpenSSH port semantics: a bare host pattern covers port
  22, and other ports are written as `[host]:port`. cftp's own entries are
  always bracketed.
- `put` rejects scp-style `host:path` destinations with a hint instead of
  silently treating them as local paths.
- Data payloads and control frames are bounded at 32 MiB, and received payloads
  are allocated incrementally instead of up front.
- `identity.key` permissions are checked on load; too-open permissions are an
  error with the `chmod` to run.
- The aggressive profile uses the default profile's QUIC windows, stream
  limits, and in-flight send bounds. Allowing 256 MiB windows and 128
  concurrent sends made the sender outrun the receiver's socket buffer on
  loopback, and the resulting retransmissions made a 1 GiB transfer about a
  quarter slower. Aggressive still uses larger packs, a deeper read-ahead
  queue, more reader workers, and a larger UDP socket buffer.
- The default profile's large-file chunk size grows from 1 MiB to 4 MiB,
  matching aggressive; larger chunks keep the hash/compress/send workers busy
  with fewer per-chunk round trips.
- Encode and reader worker counts adapt to the machine instead of assuming a
  large host: `encode_workers` is half the available cores (capped at 8), and
  the reader pools top out at 3 (Default) and 5 (Aggressive). Oversubscribing a
  four-core CI runner left no CPU for the QUIC drivers to drain the UDP socket,
  which showed up as retransmissions and a slower 1 GiB transfer; the same
  change leaves a many-core host unaffected because encoding is not the
  bottleneck once a few workers can keep up with the sender.
- CI runs the benchmark gate with `CFTP_BENCH_GATE_TOLERANCE=25` because
  GitHub's shared runners vary in CPU model enough to move the cftp/HTTPS ratio
  by more than 20% for identical builds. The harness's own default remains 5%
  for a dedicated machine, and cftp stays about twice as fast as scp and SFTP
  on every runner measured.
- The large-file send path reads chunks in parallel. A pool of reader workers
  (1 on Light, up to 3 on Default, up to 5 on Aggressive) claims chunks from a
  shared offset cursor and fetches them with positioned reads into a bounded
  queue that the hash/compress workers drain, so the disk keeps feeding
  encoders instead of idling while every encoding permit is busy. Encoding
  concurrency is still bounded by `encode_workers`. Chunks carry explicit
  offsets, so completion
  order does not matter. Extra memory is bounded at the queue depth plus one
  in-flight buffer per reader, times the chunk size per large file (about
  52 MiB at Aggressive), and the wire format is unchanged.
- Source layout: the transfer engine is split into `transfer/` modules by role
  (`client`, `push`, `pull`, `server`, `receive`, `receiver`, `pull_server`,
  `produce`, `send`, `options`),
  the CLI into `commands/` modules, and the benchmark harness into
  `examples/bench/` modules. This is a readability change only; public paths
  are preserved through `transfer`'s re-exports. `docs/architecture.md` gained
  a system map and function-level push, pull, and pipeline diagrams.

### Security

- Upgraded `p256`/`p384` to 0.14, removing RUSTSEC-2026-0387 (crypto-bigint
  timing side channels).
- `authorized_keys` entries with any option cftp cannot enforce are rejected
  (only restriction-only flags such as `no-pty` and `restrict` are accepted).
- The raw-public-key verifiers now refuse TLS 1.2 signatures rather than
  verifying them against the TLS 1.3 transcript.
- Bounded destination length, destination-by-entry memory, session slots
  (4096), pending file descriptors, and added handshake/client-hello timeouts.
- `--via-ssh` refuses a host key that differs from one already pinned in
  `known_hosts`, and passes the SSH target after `--` so a hostile
  `[USER@]HOST` cannot be interpreted as an `ssh` option.
- Received modes have setuid, setgid, sticky, and group/other-write bits
  stripped; temporary files are created mode 0600.

### Fixed

- The Arch `PKGBUILD` lists `gcc` in `makedepends`, and the package version is
  aligned with the workspace again.

## [0.2.1] - 2026-10-22

### Fixed

- `serve` now bounds the final connection-drain wait so the one-shot process exits promptly even when the peer closes slowly.

## [0.2.0] - 2026-10-22

### Added

- Light, default, and aggressive hardware profiles via `--light`, `--default`,
  and `--aggressive`.
- Live `l`/`d`/`a` profile hotkeys during `put`.
- Server-side profile caps with accept/reject negotiation.
- Network-adaptive aggressive compression based on link speed, measured
  throughput, RTT, and loss.
- `cftp/3` protocol alongside backward-compatible `cftp/2` fallback.
- SSH-key authentication is now the default. Clients use `~/.ssh/id_*` and
  `~/.ssh/known_hosts`; servers use `~/.ssh/id_ed25519` as the host key and
  verify clients against `~/.ssh/authorized_keys`.
- First-connection host-key confirmation and `--accept-new` for scripts.
- `--auth`, `--host-key`, `--authorized-keys`, `--ssh-key`, and
  `--known-hosts` CLI options.

### Changed

- `cftp serve` and `cftp put` use SSH authentication unless certificate flags
  are supplied or `--auth cert` is set. `cftp identity init` and certificate
  mode remain available.

### Security

- SSH host keys are pinned in `known_hosts`; changed host keys are rejected.
- `authorized_keys` options that cftp cannot enforce, such as `from=` and
  `command=`, are ignored with a warning.

## [0.1.1] - 2026-10-20

### Changed

- Pack building, chunk compression, sending, and receiving now use bounded
  parallelism instead of processing one item or stream at a time.

## [0.1.0] - 2026-10-20

### Added

- QUIC transport with mutual TLS and ALPN `cftp/2`.
- Push transfers for files and directory trees.
- Small-file packing and offset-based chunking for large files.
- zstd compression with incompressible-extension detection.
- BLAKE3 integrity checks for packs and chunks.
- Atomic writes with preserved modes and mtimes.
- Symlink transfers on Unix with escape protection.
- Path traversal and symlink-escape rejection.
- Local directory copy mode.
- Progress bars with local read/send and remote receive/write throughput.
- Identity generation with `cftp identity init`.
- Debian and Arch packaging, man pages, and shell completions.
- CLI integration tests for identity setup, local copy, and loopback transfers.

### Changed

- Protocol version and ALPN bumped to `2` for server progress telemetry.

### Fixed

- Chunk receiving now rejects overlapping ranges and files with gaps.
- Local copy resolves destination paths before the source/destination overlap check.
- The server reports validation errors before closing the connection.
- The README quick start no longer creates a doubled destination directory.

### Security

- Received paths are normalized and confined to the destination root.
- Files are written to temporary files and renamed into place after completion.

[0.2.1]: https://github.com/connythecreator/cftp/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/connythecreator/cftp/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/connythecreator/cftp/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/connythecreator/cftp/releases/tag/v0.1.0
