# Usage

## Requirements

- Rust 1.88 or newer to build from source. Prebuilt Linux packages are
  available for Debian/Ubuntu and Arch.
- An SSH key pair. The receiver needs the sender's public key in
  `~/.ssh/authorized_keys`; the sender pins the receiver's host key in
  `~/.ssh/known_hosts`.
- UDP reachability. The default port is `4433`, not TCP.
- Linux, macOS, or Windows source builds. Symlink transfer is Unix-only.

## Install

```bash
# Debian / Ubuntu
sudo apt install ./cftp_*_amd64.deb

# Arch
sudo pacman -U cftp-*-x86_64.pkg.tar.zst

# From source
cargo install --path crates/cftp-cli --locked
```

See [packaging](../packaging.md) for building the packages yourself.

## First transfer

On the receiver:

```bash
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
cftp serve --root ./incoming
```

On the sender:

```bash
cftp put ./project/ cftp://receiver:4433/project
```

The first connection prints the receiver's host-key fingerprint and asks for
confirmation, like `ssh`/`scp`. In scripts, pass `--accept-new` to add an unknown
key without prompting. A *changed* key always fails.

The receiver writes the tree under `./incoming/project/`. `serve` keeps running
and accepts more transfers, up to four concurrently; extra clients wait for a
slot. `--max-sessions N` changes the bound. Use `--one-shot` when a wrapper
expects a single session.

The same server also serves pulls:

```bash
cftp get cftp://receiver:4433/project/ ./downloads
```

Pull requires `cftp/4` on both peers. A server can disable either direction
with `--no-push` or `--no-pull`; a disabled request is refused with a clear
message before any data moves.

## Source and destination rules

- A destination containing `://` is remote. Only `cftp://host[:port]/path` is
  accepted; any other scheme is an error.
- Anything else is a local copy, performed without the network.
- A trailing slash on a directory source transfers its contents; without it the
  directory name is preserved. `get` applies the same rule to the remote source.

```bash
cftp put ./project/ cftp://receiver:4433/destination   # contents
cftp put ./project  cftp://receiver:4433/destination   # project/ inside
cftp put ./project  ./backup                           # local copy
cftp get cftp://receiver:4433/project/ ./downloads     # contents
cftp get cftp://receiver:4433/project ./downloads      # downloads/project/
cftp get ./backup ./restored                           # local copy
```

On the server, both directions are confined to `--root`: pushes write under it
and pulls resolve the requested path under it. Traversal and symlink escapes
are rejected.

## Server policy

`serve` is authoritative over which modes are available:

| Flags | Behaviour |
| --- | --- |
| (none) | Push and pull are both allowed. |
| `--no-push` | Pull only. Pushes fail with `push is disabled on this server`. |
| `--no-pull` | Push only. Pulls fail with `pull is disabled on this server`. |

`--no-push` and `--no-pull` conflict, because a server with neither mode would
accept sessions it can never serve.

## Hardware profiles

| Flag | Use |
| --- | --- |
| `--light` | Minimal CPU, memory, and network. |
| `--default` | Default tuning. |
| `--aggressive` | Saturate the link for the fastest transfer. |

`serve` has a fixed cap (default `default`). A client may request a higher
profile, but the server rejects anything above its cap and the transfer
continues at the current profile. During a `put`, press `l`, `d`, or `a` to
request a profile change without restarting.

Profiles are mutually exclusive with `--chunk-size`, `--pack-target`,
`--pack-max-files`, and `--small-file-max`.

Older `cftp/2` and `cftp/3` peers work for pushes, but ignore profiles and
hotkeys on `cftp/2`; pull is refused unless both peers speak `cftp/4`.

## Throughput display

The progress bar shows four rates: source bytes read, payload bytes sent,
payload bytes received by the receiver, and logical bytes written to disk.
During a pull the roles reverse, so `recv` and `write` describe the local
client. Compression means read/write can differ substantially from
sent/received. Use `--quiet` in scripts.

## Certificate mode

SSH keys are the default. Mutual TLS with a local CA remains available:

```bash
cftp identity init
cftp serve --root ./incoming --auth cert
cftp put ./project/ cftp://receiver:4433/project --auth cert
```

`identity init` writes `ca.pem`, `identity.pem`, and `identity.key` to
`~/.config/cftp` (Linux) or `--dir`. Both peers must trust the same CA. The
leaf certificate is used for both client and server authentication; the default
server name is `cftp.local`.

## Benchmarks

```bash
make bench BENCH_ARGS="--quick"
```

The harness in `crates/cftp-cli/examples/bench.rs` generates small-file, large-file,
and mixed datasets, transfers them over loopback, and compares against local copy
baselines. `scp`, FTP, and HTTP run only when their tools and targets are
available or supplied. See [the benchmark guide](../../bench/README.md).

## Next

- [Architecture](Architecture.md) for how the transfer works.
- [Library](Library.md) to embed the client or server in another program.
- [Troubleshooting](Troubleshooting.md) when a transfer fails.
