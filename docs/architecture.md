# Architecture and protocol

cftp is split into three crates:

- `cftp-proto` defines wire types and framing.
- `cftp-core` implements scanning, path safety, compression, SSH/TLS
  authentication, and the transfer engine.
- `cftp-cli` provides the `cftp` binary.

## Transfer flow: push

1. The client scans the source, assigns each entry an id, and sends a
   `ClientHello` manifest over a bidirectional control stream.
2. The server validates the destination, manifest paths, ids, and symlink
   targets, creates the directory and symlink tree off the async runtime,
   negotiates the hardware profile, then replies with `ServerHello`.
3. Data travels on many unidirectional streams. Each stream begins with a
   `DataHeader` frame and is followed by raw payload bytes.
4. The server sends periodic `Progress` messages with receive and write
   counters. The client combines them with its own read and send counters to
   render throughput in the progress bar.
5. After the client sends `Complete`, the server replies with `Done` and the
   final counters.

## Transfer flow: pull

Pull runs the same engine in the opposite direction, with the server
authoritative over the path.

1. The client sends `ClientHello { mode: Pull, destination: <source path> }`
   with no manifest entries.
2. The server checks its `ServePolicy`, resolves the source under its root with
   `path::resolve_read_path` (no traversal, no symlinks), scans it, and replies
   with `ServerHelloV4` (including `allow_push` and `allow_pull`) followed by a
   `ServerManifest`.
3. The client validates the manifest with the same path, id, symlink, and size
   rules as a push, creates the tree off the async runtime, and sends `Ready`.
4. The server encodes and sends packs and chunks exactly as a push client does,
   with periodic `Progress` telemetry.
5. The server sends `Done` after the last stream; the client verifies every file
   and sends `Complete`, which the server waits briefly to receive before
   closing.

Both directions share `scan_source`, `produce_items`, `send_item`, and the
receiver state machine, so encoding, path safety, and verification fixes apply
to both.

`serve` stays alive by default and accepts further sessions, running at most
`MAX_CONCURRENT_SESSIONS` (four) at once; additional clients wait for a slot.
`--one-shot` retains the historical behaviour of exiting after one successful
transfer. Sessions do not share transfer state, but a persistent server does
share its root, so two concurrent manifests that name the same path race on
that path, and a pull can read a tree while a push is writing it. `ServePolicy`
can disable either direction (`--no-push` / `--no-pull`); a disabled session is
rejected before any scan, path resolution, or file creation.

## Wire format

- ALPN is `cftp/4` with `cftp/3` and `cftp/2` fallback.
  `PROTOCOL_VERSION_V4` is `4`, `PROTOCOL_VERSION_V3` is `3`, and
  `PROTOCOL_VERSION_V2` is `2`.
- New peers advertise all three ALPNs and select the highest one both support.
  `cftp/2` and `cftp/3` peers continue to use their unchanged messages.
- `cftp/3` adds `HardwareProfile` to `ClientHello` and effective/cap profiles to
  `ServerHello`, plus `SetProfile` and `ProfileChanged` control messages.
- `cftp/4` adds pull and server policy in new `ClientMessageV4` and
  `ServerMessageV4` enums. Separate enums mean a v3 peer never receives a
  postcard variant it cannot decode. Pull requires `cftp/4`; push works over
  v2, v3, or v4.
- Control messages are a 4-byte big-endian length followed by a postcard payload.
- `MAX_CONTROL_FRAME` is 64 MiB, `MAX_DATA_HEADER` is 8 MiB, and
  `MAX_DATA_PAYLOAD` is 64 MiB. A frame that declares a larger body is rejected
  before any buffer is allocated.
- Data payloads are not length-framed; their lengths come from the data header.
- Manifests are bounded too: `MAX_MANIFEST_ENTRIES`, `MAX_MANIFEST_PATH_LEN`,
  `MAX_FILE_SIZE`, and `MAX_TRANSFER_BYTES` cap what a single transfer may
  declare. Packs may not list more than `MAX_PACK_ENTRIES` entries.

## Hardware profiles and hotswapping

- `light`, `default`, and `aggressive` presets control worker threads, chunk
  and pack sizes, zstd levels, in-flight work, stream limits, and QUIC windows.
- The client requests a profile. The server has a fixed cap configured at
  startup and may accept or reject each request. Rejections leave the current
  profile unchanged and do not abort the transfer.
- The server never changes its profile spontaneously; only accepted client
  requests change it.
- During `put`, `l`, `d`, and `a` hotkeys send a new request. The client applies
  the server-reported effective profile.
- Only the client adjusts tuning automatically. Aggressive mode adapts zstd
  levels from adapter link speed, measured throughput, RTT, and loss, with
  hysteresis and a cooldown. It never sends automatic profile changes.
- On `cftp/2`, profile flags and hotkeys are ignored.
- Quinn supports changing send/receive windows and stream limits after the
  handshake. `stream_receive_window` is fixed at connection setup.

## Packing and chunking

- Files at or below `--small-file-max` are packed together.
- Packs target `--pack-target` bytes and `--pack-max-files` files.
- Larger files are split into `--chunk-size` chunks with explicit offsets.
- Packs use zstd level 3; chunks use zstd level 1.
- Known incompressible extensions are sent raw.
- Every pack entry and chunk carries a BLAKE3 hash.

## Parallelism

- Source scanning is sequential.
- Pack building and chunk compression run in a bounded pool sized from
  `available_parallelism()`.
- Up to 32 data streams are sent concurrently.
- The server decodes up to 32 streams concurrently, serializes state changes
  behind a mutex, and performs disk writes outside the lock so a slow disk never
  blocks incoming stream handoff.
- Throughput can still be limited by the network, disk, or QUIC flow control
  rather than CPU count.

## Liveness and timeouts

Every wait that could otherwise hang forever is bounded by an application
timeout, so a stalled or hostile peer produces an error instead of a silent
freeze:

- Connection setup, the hello exchange, opening a data stream, and control
  writes have fixed deadlines (`HANDSHAKE_TIMEOUT`, `HELLO_TIMEOUT`,
  `CONTROL_WRITE_TIMEOUT`).
- Each stream write and read is bounded by an idle timeout
  (`STREAM_WRITE_IDLE`, `STREAM_READ_IDLE`); a peer that stops reading or
  sending aborts the transfer with a message rather than blocking.
- The server treats the absence of data-stream activity for
  `SERVER_STALL_TIMEOUT` as a dead session, and drains incomplete streams for at
  most `COMPLETE_DRAIN_TIMEOUT` after `Complete`.
- The client aborts if no server message arrives for `SESSION_STALL_TIMEOUT`,
  and waits at most `DONE_TIMEOUT` for `Done`.
- The server sends `Progress` on a timer, independent of data activity, and the
  client's progress bar ticks steadily and prints a "still working" notice when
  no update has arrived for a while. A one-shot server keeps accepting
  connections until one handshake succeeds, so a failed handshake cannot kill
  the session before the real client arrives.

## Throughput metrics

The client progress bar can show four rates:

- `read`: source bytes read by the client.
- `sent`: payload bytes sent by the client.
- `recv`: payload bytes received by the server.
- `write`: logical bytes written to disk by the server.

Rates are application-level estimates over a short window, not operating system
interface counters. Compression means `read` and `write` can differ substantially
from `sent` and `recv`.

## Authentication

SSH authentication is the default. cftp does not speak the SSH transport; it
uses SSH keys as TLS raw public keys over QUIC:

- The client loads `~/.ssh/id_ed25519`, `id_ecdsa`, or `id_rsa`.
- The server loads its host key (default `~/.ssh/id_ed25519`) and
  `~/.ssh/authorized_keys`.
- Each key is normalized to PKCS#8 for signing and to a SubjectPublicKeyInfo
  (SPKI) for identification.
- Custom rustls verifiers compare the peer's SPKI against `authorized_keys` or
  `known_hosts`. TLS proves possession of the private key, so no separate
  challenge is required.
- Unknown host keys prompt for confirmation, or are added with `--accept-new`.
  A changed host key always fails.
- `known_hosts` wildcards, port entries, hashed entries, and `@revoked` markers
  are supported. SSH certificates and `ssh-agent` are not.
- `authorized_keys` entries with security-relevant options that cftp cannot
  enforce, such as `from=` and `command=`, are ignored with a warning.

Certificate mode remains available with `--auth cert`. It uses mutual TLS with a
shared local CA, as before.

## Security model

- Transfers require mutual authentication: SSH key pinning by default, or
  mutual TLS in certificate mode.
- Received paths are normalized and rejected if they are absolute, contain
  `..`, or escape the destination through a symlink.
- Pulled sources are resolved under the server root component by component;
  `..`, absolute paths, and symlinks anywhere in the requested path are
  rejected, so a pull cannot read outside the root.
- Files are written to temporary files and renamed into place only after they
  are complete.
- QUIC protects data in transit. cftp does not add application-layer encryption
  beyond TLS.
- Restrict the UDP port to peers you trust. Protect SSH private keys and
  `identity.key` with restrictive file permissions.

To report a vulnerability, see [the security policy](../.github/SECURITY.md).

## Benchmarks

`crates/cftp-cli/examples/bench.rs` is a self-contained harness for measuring
cftp against local and optional external baselines. It generates small-file,
large-file, and mixed datasets, transfers them over loopback with mutual TLS,
verifies the received bytes, and reports wall time and throughput. Recursive
`std::fs` and `cp -a` copies are included as local baselines; `scp`, FTP, and
HTTP comparisons run only when their tools, targets, or URLs are supplied.

See [the benchmark guide](../bench/README.md) for usage, dataset sizes, and
fairness caveats:

```sh
make bench BENCH_ARGS="--quick"
```

## Known limitations

- Concurrent sessions on one persistent server share a root, so two manifests
  that name the same path race on that path.
- `cftp/3` profiles require both peers to support `cftp/3`; `cftp/2` peers use
  the legacy default tuning.
- Pull requires `cftp/4` on both peers. There is no pull fallback.
- A pull is not resumable yet; an interrupted pull restarts.
- Aggressive mode can consume hundreds of MiB because it increases in-flight
  data and QUIC windows.
- Transfers are not resumed across sessions; interrupted transfers restart.
- `ssh-agent` and SSH certificate authentication are not supported yet.
- Filenames must be valid UTF-8. On Unix, filenames containing backslashes are
  not preserved.
- Symlink creation is supported on Unix only.
- Throughput figures are per-transfer application estimates, not operating
  system interface counters.
