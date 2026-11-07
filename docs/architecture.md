# Architecture and protocol

cftp is split into three crates:

- `cftp-proto` defines wire types and framing.
- `cftp-core` implements scanning, path safety, compression, SSH/TLS
  authentication, and the transfer engine.
- `cftp-cli` provides the `cftp` binary.

## System map

```text
cftp-cli
  main.rs ................ parse argv, dispatch
  commands/serve.rs ...... listen, print URLs, stay alive
  commands/put.rs ........ build PushOptions, render progress
  commands/get.rs ........ build PullOptions, render progress
  commands/identity.rs ... keygen, pubkey, identity init
  commands/remote.rs ..... cftp:// and --via-ssh parsing, SSH bootstrap
  commands/progress.rs ... indicatif progress bar and rates
        │
        │ push / pull / serve  (cftp-core::prelude)
        ▼
cftp-core
  transfer/mod.rs ........ public API, shared constants, re-exports
  transfer/options.rs .... PushOptions, PullOptions, stats
  transfer/client.rs ..... connect_client, validate_tuning
  transfer/push.rs ....... push: scan → encode → send → report
  transfer/pull.rs ....... pull: validate manifest → fetch → write
  transfer/produce.rs .... scan → packs/chunks → encoded queue
  transfer/send.rs ....... EncodedItem → QUIC unidirectional streams
  transfer/server.rs ..... serve, accept loop, serve_connection
  transfer/receive.rs .... receive_connection, spawn_receive_task, manifest checks
  transfer/receiver.rs ... decode_stream → ReceiverState → write_at
  transfer/pull_server.rs  serve_pull: resolve → scan → manifest → send
  scan.rs path.rs codec.rs  filesystem walk, path safety, zstd/BLAKE3
  auth.rs identity.rs ssh.rs trust.rs  authentication backends
  tuning.rs link.rs congestion.rs socket.rs  profiles and transport
        │
        │ postcard control frames, raw data payloads
        ▼
cftp-proto ................ wire types, framing, ALPN, size limits
```

Each transfer is one QUIC connection: the client opens a bidirectional control
stream, then sends data on many unidirectional streams. The same
`produce_items`/`send_item` pipeline serves a push client and a pull server, and
the same `decode_stream`/`ReceiverState` pipeline serves a push server and a
pull client.

## Push: function-level flow

```text
CLIENT                                          SERVER
push()                                          serve()
 ├─ scan_source()                               └─ accept_connection()
 ├─ connect_client()                              └─ serve_connection()
 │   └─ QUIC handshake + SSH/RPK auth               ├─ read_client_hello()
 ├─ write_client_hello()                            ├─ ServePolicy check
 ├─ read_server_hello()                             ├─ validate_manifest()
 ├─ produce_items()                                 ├─ create tree
 │   ├─ build_pack()          small files           │   (spawn_blocking)
 │   ├─ enqueue_large()       large files           ├─ write_server_hello()
 │   │   ├─ reserve_chunk()                         └─ receive_connection()
 │   │   └─ read_worker() → read_at()                   ├─ spawn_receive_task() × N
 │   │       └─ bounded read-ahead queue                │   ├─ decode_stream()
 │   └─ codec::compress() + BLAKE3 (blocking)           │   └─ ReceiverState::
 ├─ send_item() × max_inflight_sends                    │       apply_pack/apply_chunk
 │   └─ open_uni() + DataHeader + payload               ├─ write_at(): temp file + rename
 ├─ read_server_message() ← Progress telemetry          ├─ write_server_progress() (timer)
 └─ write_complete() ───────────────────────────────►   ├─ drain streams, verify hashes
                                                        └─ write_server_done() → Done
```

## Pull: function-level flow

```text
CLIENT                                          SERVER
pull()                                          serve()
 ├─ connect_client()                            └─ serve_connection()
 ├─ write_client_hello(Pull, source)               ├─ read_client_hello()
 ├─ read_server_hello()                            ├─ ServePolicy check
 ├─ read_server_manifest()                         ├─ path::resolve_read_path()
 ├─ validate_manifest()                            ├─ scan_source()
 ├─ create tree (spawn_blocking)                   ├─ write_server_manifest()
 ├─ write_client_ready() ───────────────────────►  ├─ read_client_ready()
 └─ receive loop (same state machine as a server)  └─ serve_pull()
     ├─ spawn_receive_task() × N                      ├─ produce_items()
     │   ├─ decode_stream()                           ├─ send_item()
     │   └─ ReceiverState + write_at()                ├─ write_server_progress()
     └─ write_complete() ────────────────────────►    └─ write_server_done() → Done
```

## Data pipeline

```text
large file
  shared AtomicU64 offset cursor
    ├─ read_worker 0 ─┐
    ├─ read_worker 1 ─┤ positioned reads (read_at)
    └─ read_worker N ─┘      N = 1 / 3 / 5 for light / default / aggressive
          │
          ▼  bounded queue: readahead_chunks
   encode permit (encode_workers)
          │
          ▼  spawn_blocking: BLAKE3 hash + zstd
   EncodedItem::Chunk { offset, payload }
          │
          ▼  bounded encoded queue: ENCODED_QUEUE_CAPACITY
   send_item() → one QUIC unidirectional stream per chunk
          │
          ▼
   decode_stream() → verify hash → decompress
          │
          ▼
   ReceivedRanges: accept out-of-order, reject overlaps
          │
          ▼
   write_at(offset) into a temporary file; rename after verification
```

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
  and pack sizes, zstd levels, read-ahead depth, and the UDP socket buffer.
- `default` and `aggressive` share the same QUIC windows, stream limits, and
  in-flight send bounds. Allowing 256 MiB windows and 128 concurrent sends made
  the sender outrun the receiver's socket buffer on loopback; the resulting
  retransmissions and window collapses made a 1 GiB transfer about a quarter
  slower. Aggressive spends its extra capacity on read-ahead, packing, and a
  larger socket buffer instead.
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
- Each endpoint's UDP socket is bound with `SO_RCVBUF`/`SO_SNDBUF` sized from the
  profile (1 MiB light, 4 MiB default, 8 MiB aggressive), clamped by the kernel's
  `rmem_max`/`wmem_max`. The kernel default is a few hundred KiB; on a jittery
  or high-RTT path that overflows during a burst, and QUIC reads the drops as
  congestion loss. The receiver's buffer matters most, so `serve` sizes it from
  the profile it starts with; `socket_buffer` in `Tuning` cannot be changed
  after the endpoint is bound.

## Congestion control

- Every connection starts on QUIC's CUBIC controller with the profile's
  windows. `congestion.rs` measures delivered and lost bytes over 250 ms
  windows; after two consecutive windows above 0.3% loss it replaces the
  controller with BBR, seeded from the current window, and forwards every
  later signal to it.
- Clean paths (loopback, wired LAN) measure no loss and stay on CUBIC, which
  is faster there. The switch is one-way for the life of a connection: a path
  that drops packets does not become clean during the same transfer, and
  re-seeding CUBIC from BBR's larger window is not safe.
- This runs on both peers and is independent of the hardware profile. It
  changes only the QUIC congestion controller; zstd and the wire format are
  untouched.

## Datagram size and GSO

- A connection to a loopback peer starts at 16 KB QUIC datagrams (the largest
  that fits the 14-bit packet-length field) instead of quinn's 1,200-byte
  initial MTU. Path MTU discovery has no gap to probe on a saturated transfer,
  so the default would otherwise be used for the whole transfer. A wildcard or
  routable listener keeps quinn's conservative defaults, because its peers are
  unknown.
- Every endpoint advertises 16 KB as its maximum receive size, so a loopback
  client talking to a wildcard server still uses large datagrams.
- Linux rejects a UDP GSO buffer above 64 KiB. Quinn packs up to ten datagrams
  per `sendmsg`, so `socket.rs` wraps the socket in `CappedGsoSocket` and caps
  `max_transmit_segments()` at what the 64 KiB limit allows for the current
  datagram size.
- The initial congestion window scales with the datagram size. CUBIC's 12 KB
  default is smaller than one jumbo Initial packet, and a connection that
  cannot send its first packet never gets past the handshake.

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

## Large-file send pipeline

A large file moves through a reader pool, a compression stage, and the ordered
send pipeline:

- A pool of reader workers claims chunks from a shared offset cursor and fetches
  them with positioned reads, so reads for one file proceed in parallel.
- Readers fill a bounded queue that the hash/compress workers drain; encoding
  concurrency is still bounded by `encode_workers`.
- Each chunk is hashed and compressed on `spawn_blocking`, then sent on a data
  stream. Chunks carry explicit offsets, so they can complete out of order.

The reader pool scales with the profile: 1 worker on Light, 3 on Default, and 5
on Aggressive. The queue is bounded, so the disk can run ahead of encoding
without unbounded buffering. Extra memory per large file is bounded at the queue
depth plus one in-flight buffer per reader, times `--chunk-size` (about 52 MiB
at Aggressive), and the bounded send pipeline and wire format are unchanged.

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
- The server loads its host key (default `~/.config/cftp/host_key`, generated on
  first run) and verifies clients against `~/.ssh/authorized_keys`.
- Each key is normalized to PKCS#8 for signing and to a SubjectPublicKeyInfo
  (SPKI) for identification.
- Custom rustls verifiers compare the peer's SPKI against `authorized_keys` or
  `known_hosts`. TLS proves possession of the private key, so no separate
  challenge is required.
- Unknown host keys prompt for confirmation, or are added with `--accept-new`.
  A changed host key always fails.
- `known_hosts` wildcards, port entries, hashed entries, and `@revoked` markers
  are supported. Port semantics follow OpenSSH: a bare host pattern covers port
  22, and other ports use `[host]:port`. cftp writes its own entries bracketed,
  under `~/.config/cftp/known_hosts`. SSH certificates and `ssh-agent` are not
  supported.
- `authorized_keys` entries carrying any option cftp cannot enforce, such as
  `from=` and `command=`, are rejected. Only restriction-only flags such as
  `no-pty` and `restrict` are accepted.

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

`crates/cftp-cli/examples/bench/` is a self-contained harness for measuring
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
- Aggressive mode reads up to eight chunks per large file ahead of the encoders
  (about 52 MiB) and builds larger packs, so it needs more memory than the
  default profile.
- Transfers are not resumed across sessions; interrupted transfers restart.
- `ssh-agent` and SSH certificate authentication are not supported yet.
- Filenames must be valid UTF-8. On Unix, filenames containing backslashes are
  not preserved.
- Symlink creation is supported on Unix only.
- Throughput figures are per-transfer application estimates, not operating
  system interface counters.
