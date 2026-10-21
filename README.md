# cftp

Fast file and directory transfers over QUIC, with mutual TLS and zstd compression.

[![CI](https://github.com/connythecreator/cftp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/connythecreator/cftp/actions/workflows/ci.yml)
[![License: GPL-3.0-only](https://img.shields.io/badge/license-GPL--3.0--only-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://www.rust-lang.org)

cftp (Connys File Transfer Program) pushes files and directory trees to a remote
receiver over QUIC. It authenticates with SSH keys by default, uses zstd for
compression, and BLAKE3 for integrity checks. Small files are packed together and
large files are split into chunks.

## Features

- QUIC transport with ALPN `cftp/2`.
- SSH-key authentication by default, using `authorized_keys` and `known_hosts`.
- Optional mutual TLS with a local CA.
- zstd compression, skipping known incompressible file extensions.
- Small-file packs and offset-based chunks for large files.
- BLAKE3 integrity checks for every pack entry and chunk.
- Atomic writes, preserved mtimes, and preserved modes on Unix.
- Symlink support on Unix, with escape protection.
- Path traversal and symlink-escape rejection.
- Local directory copy when the destination is not a `cftp://` URL.
- Live read, sent, received, and written throughput in the progress bar.
- Progress bars, with `--quiet` to disable them.

## Documentation

- [Architecture and protocol](docs/architecture.md)
- [Packaging and releases](docs/packaging.md)
- [Contributing](.github/CONTRIBUTING.md)
- [Security policy](.github/SECURITY.md)
- [Changelog](CHANGELOG.md)

## Requirements

- Rust 1.88 or newer to build from source.
- An SSH key pair, and the peer's public key in `~/.ssh/authorized_keys`.
- UDP port `4433` (default) reachable between peers.
- Linux, macOS, or Windows for source builds. Packages are currently Linux-only.
- Docker if you want to build the Arch package on a non-Arch host.

## Installation

### Debian and Ubuntu

Download the `.deb` from the [latest release](https://github.com/connythecreator/cftp/releases)
and install it:

```bash
sudo apt install ./cftp_*_amd64.deb
```

### Arch Linux

Download the package from the [latest release](https://github.com/connythecreator/cftp/releases)
and install it:

```bash
sudo pacman -U cftp-*-x86_64.pkg.tar.zst
```

Packaging files live in `packaging/arch/`. The package is not published to the
AUR yet.

### From source

```bash
cargo install --path crates/cftp-cli --locked
```

Or build a release binary without installing it:

```bash
cargo build --release --locked
./target/release/cftp --help
```

## Quick start

cftp uses SSH keys by default. On the receiver, add the sender's public key to
`~/.ssh/authorized_keys` and start the listener:

```bash
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
cftp serve --root ./incoming
```

The receiver uses `~/.ssh/id_ed25519` as its host key. On the sender:

```bash
cftp put ./project/ cftp://receiver:4433/project
```

The sender uses the first existing key from `~/.ssh/id_ed25519`,
`~/.ssh/id_ecdsa`, or `~/.ssh/id_rsa`, and verifies the receiver against
`~/.ssh/known_hosts`. The first connection prints the receiver's host-key
fingerprint and asks you to confirm it, like `scp`. In scripts, pass
`--accept-new` to add an unknown host key automatically.

The receiver writes the transfer under `./incoming/project/`. `serve` is
one-shot: it accepts one connection, receives the transfer, then exits.

To copy locally instead of over the network:

```bash
cftp put ./project ./backup
```

A trailing slash on a directory source transfers its contents; without it, the
source directory name is preserved:

```bash
cftp put ./project/ cftp://receiver:4433/destination
```

### Manual certificate mode

SSH authentication is the default. The original mutual-TLS mode is still
available and is selected automatically when any certificate flag is passed, or
explicitly with `--auth cert`:

```bash
cftp identity init
cftp serve --root ./incoming --auth cert
cftp put ./project/ cftp://receiver:4433/project --auth cert
```

You can still generate an identity on each machine and exchange only the CA
certificates:

- On the server, pass the client's CA with `--client-ca`.
- On the client, pass the server's CA with `--server-ca`.

The generated leaf certificate uses `cftp.local` as its name, so the client's
default `--server-name cftp.local` continues to work.

## Command reference

### `cftp identity init`

| Flag | Description |
| --- | --- |
| `--dir <DIR>` | Write `ca.pem`, `identity.pem`, and `identity.key` to this directory instead of the platform config directory. |
| `--force` | Replace existing identity files. |

On Linux the default directory is `~/.config/cftp`.

### `cftp serve`

| Flag | Description |
| --- | --- |
| `--root <DIR>` | Required. Directory that received files are written under. |
| `--listen <ADDR>` | UDP address to listen on. Defaults to `0.0.0.0:4433`. |
| `--auth <auto\|ssh\|cert>` | Authentication backend. Defaults to `auto`. |
| `--host-key <FILE>` | SSH host key. Defaults to `~/.ssh/id_ed25519`. |
| `--authorized-keys <FILE>` | SSH keys allowed to connect. Defaults to `~/.ssh/authorized_keys`. |
| `--identity <FILE>` | Leaf certificate presented to clients (cert mode). |
| `--key <FILE>` | Private key for the leaf certificate (cert mode). |
| `--client-ca <FILE>` | CA used to verify client certificates (cert mode). |
| `--quiet` | Disable the progress bar. |

### `cftp put`

| Argument or flag | Description |
| --- | --- |
| `<SRC>` | Source file or directory. |
| `<DEST>` | A `cftp://host[:port]/path` URL, or a local destination directory. |
| `--auth <auto\|ssh\|cert>` | Authentication backend. Defaults to `auto`. |
| `--ssh-key <FILE>` | SSH private key presented to the server. Defaults to the first of `~/.ssh/id_ed25519`, `id_ecdsa`, `id_rsa`. |
| `--known-hosts <FILE>` | Host keys used to verify the server. Defaults to `~/.ssh/known_hosts`. |
| `--accept-new` | Add an unknown server host key to `known_hosts` without prompting. |
| `--identity <FILE>` | Leaf certificate presented to the server (cert mode). |
| `--key <FILE>` | Private key for the leaf certificate (cert mode). |
| `--server-ca <FILE>` | CA used to verify the server certificate (cert mode). |
| `--server-name <NAME>` | Expected server name. Defaults to `cftp.local`. |
| `--quiet` | Disable the progress bar. |
| `--chunk-size <BYTES>` | Large-file chunk size. Defaults to 1 MiB. |
| `--pack-target <BYTES>` | Target size of a small-file pack. Defaults to 4 MiB. |
| `--pack-max-files <N>` | Maximum files per pack. Defaults to 128. |
| `--small-file-max <BYTES>` | Files at or below this size are packed. Defaults to 2 MiB. |

The default port for a remote destination is `4433`. Only the `cftp://` scheme
is accepted; any other `://` destination is rejected.

## Throughput

The client progress bar shows four rates:

- `read`: source bytes read by the client.
- `sent`: payload bytes sent by the client.
- `recv`: payload bytes received by the server.
- `write`: logical bytes written to disk by the server.

Rates are displayed in bits per second (`Gbps`, `Mbps`, `Kbps`, or `bps`) and
are application-level estimates over a short window, not operating system
interface counters. Compression means `read`/`write` can differ substantially
from `sent`/`recv`.

## Security

By default, peers authenticate with SSH keys. The server checks client keys
against `authorized_keys`, and the client pins server keys in `known_hosts`.
Transfers protect data in transit with QUIC/TLS, received paths are confined to
the destination root, and files are written atomically. Certificate mode remains
available for deployments that prefer a shared CA. See
[architecture and protocol](docs/architecture.md) for the full security model and
[the security policy](.github/SECURITY.md) for reporting vulnerabilities.

## Development

```bash
cargo build --release --locked
cargo test --workspace --locked
make check
```

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for coding standards, protocol
rules, and the pull request checklist.

## Packaging

See [packaging and releases](docs/packaging.md) for Debian, Arch, and release
workflows.

## License

cftp is licensed under GPL-3.0-only. See [LICENSE](LICENSE).
