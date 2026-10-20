# Contributing to cftp

Thanks for contributing. This project is a Rust workspace with a QUIC transfer
engine and a command-line frontend. Keep protocol logic in `cftp-proto` and
`cftp-core`; keep CLI wiring in `cftp-cli`.

## Prerequisites

- Rust 1.88 or newer. `rust-toolchain.toml` selects the stable channel and
  installs rustfmt and clippy.
- Docker for Arch package builds on non-Arch hosts.
- Optional local tools: `cargo-deb`, `cargo-deny`, `typos-cli`, and
  `markdownlint-cli2`.

## Setup

```bash
git clone https://github.com/connythecreator/cftp.git
cd cftp
cargo build --locked
cargo test --workspace --locked
```

## Local checks

Run the same checks as CI before opening a pull request:

```bash
make check
make doc
make deny
make typos
```

`make check` runs formatting, clippy with warnings denied, and the test suite.
`make ci` combines the core checks. Packaging targets are `make deb`,
`make verify-deb`, and `make arch-docker`.

## Code standards

- Format with `cargo fmt --all`. CI rejects unformatted code.
- Clippy must pass with `-D warnings`.
- The crates forbid unsafe code. Do not remove `#![forbid(unsafe_code)]`.
- The minimum supported Rust version is 1.88. Do not use newer APIs without
  updating the MSRV and CI.
- Use `anyhow` at binary and I/O boundaries. Use `thiserror` for protocol
  errors.
- Keep public APIs documented with rustdoc. CI builds docs with warnings denied.
- Add tests for behavior changes. Tests must be hermetic: use `tempfile` and
  loopback ephemeral ports, never external services.
- Prefer focused tests plus the existing end-to-end transfer test for changes
  that affect both peers.

## Protocol and safety rules

- Changing a struct in `cftp-proto` changes the postcard wire format. Update
  `PROTOCOL_VERSION`, both push and receive paths in `transfer.rs`, and tests
  together.
- Use the helpers in `cftp-core/src/path.rs` for every received path. Never join
  untrusted paths directly.
- Keep `MAX_CONTROL_FRAME` and `MAX_DATA_PAYLOAD` checks in place, and validate
  chunk and pack settings in `push`.
- `serve` is intentionally one-shot. Do not add a long-running accept loop
  without discussing the lifecycle and shutdown behavior first.

## Generated files

- `packaging/generated/` contains generated man pages and shell completions.
  Regenerate with `make assets`; do not edit those files by hand.
- `packaging/arch/.SRCINFO` is generated with `make arch-srcinfo`. Regenerate it
  after changing `PKGBUILD`.
- `Cargo.lock` is committed. Update it only through Cargo commands.

## Commits and pull requests

- Write imperative commit subjects, for example `fix: reject empty manifest
  paths`. Conventional Commit prefixes such as `feat:`, `fix:`, `docs:`,
  `test:`, `build:`, and `ci:` are recommended.
- Keep pull requests focused. Include tests and documentation for user-visible
  changes.
- Update `CHANGELOG.md` for user-visible additions, changes, and fixes.
- Do not commit editor state, build output, generated archives, or local
  assistant configuration.

## Pull request checklist

- [ ] `make check` passes.
- [ ] `make doc` passes.
- [ ] `make deny` passes when dependencies or licenses change.
- [ ] New behavior has tests.
- [ ] Protocol changes bump `PROTOCOL_VERSION` and update both peers.
- [ ] User-visible changes update `CHANGELOG.md`.
- [ ] Packaging changes regenerate `.SRCINFO` and pass `make arch-docker`.

## Security

Do not report security issues in public issues or pull requests. Follow
[SECURITY.md](SECURITY.md).
