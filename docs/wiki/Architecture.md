# Architecture

cftp is a three-crate workspace. The CLI contains wiring only; all protocol and
transfer logic lives in the library crates.

| Crate | Responsibility |
| --- | --- |
| `cftp-proto` | Wire types, framing, protocol constants. |
| `cftp-core` | Scanning, path safety, codec, identity/TLS, SSH authentication, transfer engine. |
| `cftp-cli` | The `cftp` binary: argument parsing, progress rendering, hotkeys. |

Every crate root has `#![forbid(unsafe_code)]`.

## Transfer flow (push)

1. The client scans the source (`scan_source`) and assigns each entry a dense
   id, `1..=n`.
2. It opens one bidirectional control stream and sends a `ClientHello`
   manifest.
3. The server checks its policy, then validates the destination, each relative
   path, duplicate paths and ids, symlink targets, and the declared size limits.
   Directories and symlinks are created on `spawn_blocking`, before data flows.
4. The server negotiates a hardware profile and replies with `ServerHello` on
   the same stream.
5. Data travels on many unidirectional streams. Each stream starts with a
   `DataHeader` (a pack or a chunk) followed by raw payload bytes.
6. The server sends `Progress` telemetry on the control stream while it
   receives; the client reads it concurrently with sending.
7. The client sends `Complete`. The server drains in-flight streams, verifies
   hashes, replies with `Done`, and the client reports final counters.

## Transfer flow (pull)

1. The client sends a `ClientHello` with `mode: Pull` and a source path.
2. The server checks policy and resolves the source under the root with
   `path::resolve_read_path`, which rejects traversal and symlinks.
3. The server replies with `ServerHelloV4` (including `allow_push` and
   `allow_pull`) and then a `Manifest` scanned from the source.
4. The client validates the manifest with the same rules as a push, creates the
   tree, and sends `Ready`.
5. The server encodes and sends packs and chunks exactly as a push client does,
   with `Progress` telemetry on the control stream.
6. The server sends `Done`; the client verifies every file, sends `Complete`,
   and the server waits briefly for that acknowledgement before closing.

Both directions share `scan_source`, `produce_items`, `send_item`, and the
receiver state machine, so a fix to encoding, path safety, or verification
applies to both.

## Server lifecycle

`serve` binds the UDP endpoint first and reports the bound address through
`on_ready`, then:

- With `one_shot = true`, it accepts connections until one handshake succeeds,
  serves that session, waits up to one second for the endpoint to go idle, and
  closes.
- Otherwise it loops over `endpoint.accept()` and a `JoinSet` of session tasks.
  A failed handshake is logged and skipped so a scanner cannot kill the server.
  A semaphore with `max_sessions` permits bounds concurrency; permits are
  released when a task ends, including on panic. Extra clients wait for a slot.

`ServePolicy` is fixed for the endpoint and copied into each session. A session
whose mode is disabled is rejected before scanning or creating anything.

Session state is per-session, but the root is shared. Two concurrent manifests
that name the same path race on that path, and a pull can read a tree while a
push is writing it.

`serve_once` is a wrapper that forces `one_shot = true`.

## Receiver state and lookup cost

The receiver keeps per-file state in parallel vectors indexed by `id - 1`, not
in integer-keyed hash maps:

```text
entries Vec<ManifestEntry>   rel_paths Vec<PathBuf>
ranges Vec<ReceivedRanges>   completed Vec<bool>
in_progress Vec<bool>        in_flight Vec<usize>
pending Vec<Option<PendingFile>>
```

`slot(file_id)` is a checked subtraction and a bounds check, so every hot-path
lookup is O(1). Manifest validation enforces dense ids, which is what makes
indexing sound. `pending_count` tracks outstanding packs without scanning.

Ranges per file are coalesced intervals. A chunk that arrives twice is
ignored if its range is already covered, and overlapping ranges are merged
rather than stored individually.

## Parallelism and disks

- Pack building and chunk compression run in a bounded blocking pool sized from
  `available_parallelism()`.
- Up to 32 data streams are sent concurrently.
- The receiver decodes up to 32 streams concurrently, serializes state changes
  behind a mutex, and performs disk writes outside the lock so a slow disk does
  not stall stream handoff.
- Files are written to temporary files and renamed into place only after the
  hash matches and every byte is present.

## Liveness

Every unbounded wait has an application deadline (`HANDSHAKE_TIMEOUT`,
`HELLO_TIMEOUT`, `SESSION_STALL_TIMEOUT`, `SERVER_STALL_TIMEOUT`,
`COMPLETE_DRAIN_TIMEOUT`, `DONE_TIMEOUT`, plus per-stream idle timeouts). A
stalled or hostile peer produces an error instead of a silent freeze. Progress
is sent on a timer independent of data activity, so the client bar keeps
ticking, and the client prints a notice when updates stop arriving.

## Further reading

- [Protocol](Protocol.md) for the wire format.
- [Library](Library.md) for the embedding API.
- [Full architecture and protocol](../architecture.md).
- [Resume design](../resume.md) for the planned crash-recovery extension.
