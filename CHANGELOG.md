# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/connythecreator/cftp/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/connythecreator/cftp/releases/tag/v0.1.0
