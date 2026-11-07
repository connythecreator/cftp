# Usage

## Requirements

- Rust 1.88 or newer to build from source. Prebuilt Linux packages are
  available for Debian/Ubuntu and Arch.
- An SSH key pair. `cftp keygen` creates one if `~/.ssh` has none. The receiver
  needs the sender's public key in `~/.ssh/authorized_keys`, or you can use
  `--via-ssh`, which authorizes the sender for a single session.
- The sender pins the receiver's host key in `~/.config/cftp/known_hosts`;
  `serve` generates `~/.config/cftp/host_key` on first run.
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

If you can already `ssh` from the sender to the receiver, one command starts a
temporary one-shot server there, learns its host key over the authenticated SSH
channel, and authorizes your client key for that session only:

```bash
cftp put ./project project --via-ssh you@receiver
```

The destination is relative to the SSH login's home directory (a leading slash
is ignored). UDP `4433` (or the port after the host) must still be reachable
directly; SSH is only the bootstrap.

For a permanent receiver, authorize the sender once and start the listener:

```bash
# On the sender: print the public key to authorize on the receiver.
cftp pubkey

# On the receiver: paste that line into ~/.ssh/authorized_keys, then start
# the listener (the root and host key are created on first run).
cftp serve --root ~/incoming

# On the sender:
cftp put ./project/ cftp://receiver:4433/project
```

`cftp keygen` creates a key if the sender has none.

The first connection prints the receiver's host-key fingerprint and asks for
confirmation, like `ssh`/`scp`. In scripts, pass `--accept-new` to add an unknown
key without prompting. A *changed* key always fails.

The receiver writes the tree under `~/incoming/project/`. `serve` keeps running
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
- scp-style `host:path` destinations are rejected with a hint pointing at
  `cftp://host/path` or `--via-ssh host:/path`.
- With `--via-ssh`, a plain path argument is the remote destination (relative
  to the remote home directory) instead of a local copy.
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

Separately from the profile, cftp watches each connection for packet loss. A
clean path (loopback or wired LAN) stays on CUBIC. After two consecutive
measurement windows above 0.3% loss it switches that connection to BBR, which
handles random loss far better on Wi-Fi and other wireless links. The switch
affects the QUIC congestion controller only; zstd compression and integrity
checking are unchanged.

Older `cftp/2` and `cftp/3` peers work for pushes, but ignore profiles and
hotkeys on `cftp/2`; pull is refused unless both peers speak `cftp/4`.

## Throughput display

The progress bar shows four rates: source bytes read, payload bytes sent,
payload bytes received by the receiver, and logical bytes written to disk.
During a pull the roles reverse, so `recv` and `write` describe the local
client. Compression means read/write can differ substantially from
sent/received. Use `--quiet` in scripts.

Pass `--stats` to `put` or `get` to print one line of QUIC transport counters
after the transfer: smoothed and minimum RTT, congestion window, path MTU,
packets sent and lost, congestion events, and UDP datagram and byte counts. The
line starts with `stats:` so it is easy to grep in a benchmark log.

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
make bench BENCH_ARGS="--quick"   # local smoke run
make bench-gate                   # release gate in Docker
```

The harness in `crates/cftp-cli/examples/bench/` generates small-file, large-file,
mixed, and 1 GiB `huge` datasets, transfers them over loopback, and compares
against local copy baselines. It runs cftp with `--aggressive`. `scp`, SFTP,
HTTPS, FTP, and HTTP run only when their tools and targets are available or
supplied. `make bench-gate` runs every peer in one Docker container and fails
unless cftp is within 5 % of the fastest encrypted peer (scp, SFTP, HTTPS); FTP
and HTTP are plaintext reference rows. The gate includes the 1 GiB `huge`
dataset and deletes each upload after measuring it, so it holds one generated
dataset plus one copy rather than one copy per peer. Published results are on
[the benchmark page](Benchmarks.md). See also
[the benchmark guide](../../bench/README.md).

## Next

- [Architecture](Architecture.md) for how the transfer works.
- [Library](Library.md) to embed the client or server in another program.
- [Troubleshooting](Troubleshooting.md) when a transfer fails.
