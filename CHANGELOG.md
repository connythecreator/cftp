# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

[Unreleased]: https://github.com/connythecreator/cftp/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/connythecreator/cftp/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/connythecreator/cftp/releases/tag/v0.1.0
