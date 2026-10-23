# Protocol

## Versions

- ALPN `cftp/4`, with automatic fallback to `cftp/3` and `cftp/2`.
- `PROTOCOL_VERSION_V4 = 4`, `PROTOCOL_VERSION_V3 = 3`,
  `PROTOCOL_VERSION_V2 = 2`.
- Peers advertise all three ALPNs, in that order, and select the highest common
  one.
- `cftp/3` adds `HardwareProfile` to the hello exchange plus `SetProfile` and
  `ProfileChanged` control messages. The `cftp/2` messages are unchanged.
- `cftp/4` adds pull and server policy. It introduces `ClientMessageV4` and
  `ServerMessageV4` rather than adding variants to the v3 enums, so a v3 peer
  never receives a postcard variant it cannot decode.
- `cftp/2` peers ignore profile flags and hotkeys and use default tuning.
- Pull requires `cftp/4`; a push still works over v2, v3, or v4.

## Control stream

One bidirectional QUIC stream carries control messages for the whole session.

```text
+----------------+---------------------------+
| u32 length BE  | postcard-encoded message  |
+----------------+---------------------------+
```

Messages are `ClientMessageV4` (hello, complete, abort, set-profile, ready) and
`ServerMessageV4` (hello, manifest, done, error, progress, profile-changed).
Postcard serializes struct fields in declaration order, so reordering, adding,
or removing a field is a wire-format change.

Bounds are enforced before allocation:

| Bound | Value |
| --- | --- |
| `MAX_CONTROL_FRAME` | 64 MiB |
| `MAX_DATA_HEADER` | 8 MiB |
| `MAX_DATA_PAYLOAD` | 64 MiB |
| `MAX_MANIFEST_ENTRIES` | 100,000 |
| `MAX_MANIFEST_PATH_LEN` | 4096 |
| `MAX_FILE_SIZE` | 1 TiB |
| `MAX_TRANSFER_BYTES` | 16 TiB |
| `MAX_PACK_ENTRIES` | 65,536 |

A frame that declares a larger body is rejected before its buffer is
allocated. A zero-length frame is rejected.

## Manifest

`ClientHello` carries `destination` plus `ManifestEntry` values:

```text
id, path, kind (file/directory/symlink), size, mode, mtime, link_target
```

`path` is relative, uses `/`, and may not be absolute, contain `..`, or escape
the root through a symlink. The receiver requires ids to be dense and unique:
exactly `1..=entries.len()`. This is what allows O(1) `id - 1` indexing of
per-file state. Duplicate paths are rejected.

Directories and symlinks are created before `ServerHello` is sent, off the
async runtime. Declared sizes are checked against the transfer-wide limits.

## Pull

Pull reuses the push data path in the opposite direction. The server is
authoritative over the path and the enabled modes.

```text
client                          server
  |-- Hello{mode: Pull, source} -->|
  |<-- HelloV4{allow_push, allow_pull, profile} --|
  |<-- Manifest{entries, totals} --|
  |-- Ready ---------------------->|
  |<-- Progress / packs / chunks --|
  |<-- Done -----------------------|
  |-- Complete ------------------->|
```

- `HelloV4.destination` is the source path for a pull and `entries` is empty.
- The server resolves the source with `path::resolve_read_path`: traversal and
  symlinks anywhere in the path are rejected, so a pull cannot escape the root.
- The manifest carries the same `ManifestEntry` values as a push, relative to
  the requested source. The client validates it with the same dense-id, path,
  and size rules before creating any files.
- `Ready` tells the server the client has created the directory tree and is
  ready for data streams. The server sends nothing until it arrives.
- `Done` means all data has been written to streams. The client acknowledges
  with `Complete` after it verifies every file, and the server waits briefly
  for that frame before closing the connection.

`ServerHelloV4.allow_push` and `allow_pull` report the server policy. A disabled
mode is also rejected with an `Error` message before any scan or data.

## Data streams

Data is never length-framed at the transport level; the header supplies the
length.

```text
uni stream := DataHeader frame || raw payload bytes
```

- **Pack** — several small files concatenated. The header lists each
  `file_id`, size, and BLAKE3 hash, plus the codec and compressed/uncompressed
  lengths. Packs use zstd level 3.
- **Chunk** — one large file split into fixed-size pieces. The header carries
  `file_id`, `offset`, lengths, codec, and a BLAKE3 hash. Chunks use zstd level
  1.

Known-incompressible extensions are sent raw. The receiver merges chunk ranges,
so duplicate and overlapping chunks are harmless. A pack is written only when
every entry hash matches; a chunk only when its hash matches.

## Compatibility rules

When changing a protocol struct:

1. Update the types in `cftp-proto`.
2. Bump `PROTOCOL_VERSION` (and add an ALPN only if old peers must be excluded).
3. Update the push and pull paths on both peers in `transfer.rs`.
4. Update the bounds and this page.

Never serialize untrusted paths directly into filesystem joins; use the
helpers in `cftp-core/src/path.rs`.
