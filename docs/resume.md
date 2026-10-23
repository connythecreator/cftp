# Resumable transfers

This document specifies resumable `put` transfers for cftp. The design adds a
new wire version (`cftp/4`), a server-authoritative resume journal under the
served root, and dense O(1) state in the receive hot path. It deliberately does
not add a per-chunk hash to the fast path: the journal records only data that
was already BLAKE3-verified on the wire and then fsynced to disk.

The design has four invariants:

1. Manifest ids are dense (`1..=n`), so every per-file lookup is an array index.
2. A journaled coverage bit means the corresponding bytes were verified and are
   durable. The bit is never written before the data fsync.
3. The client cannot name a journal or a file. The server derives the transfer
   key and maps ids to paths from the validated manifest.
4. Any doubt about the journal, the partial store, or the client token falls
   back to a full transfer, never to partial trust.

## 1. Goals and non-goals

### Goals

- Resume an interrupted `put` without re-sending chunks that are already
  durable on the server.
- Survive client restarts. The server re-derives the transfer key from the
  manifest and destination, so the client needs no local state to find a
  journal. A cached token is only a consistency check.
- Survive server restarts. The journal and partial data live under the served
  root and are durable before the coverage bit is set.
- Never trust client-claimed state. The client's `transfer_key` can only cause
  a conservative full-transfer fallback; it never selects a journal path.
- Bound memory and disk with caps derived from the existing manifest limits.
- Keep the receive hot path O(1) per lookup: dense `Vec` indexing, bit tests,
  and running counters. No hashing of paths or ids while data is flowing.

### Non-goals

- Cross-user sharing or multi-tenant journal access. One authenticated user
  owns a destination tree.
- Deduplication across unrelated transfers. Two transfers with different
  manifests get different keys even if some files are identical.
- Resume of local copies (`put` with a local destination uses `copy_local`).
- Proving that the source file contents did not change between attempts. The
  manifest includes size and mtime as a cheap guard; a full content digest is
  explicitly deferred.
- Multiple concurrent server processes writing the same journal. A lock file
  detects this case; operators should run one `serve` process per root.

## 2. Resume identity

### 2.1 Canonical manifest encoding

The resume key must not depend on postcard, serde field order, or platform
integer width. A dedicated encoder writes fixed-width little-endian integers
with explicit length prefixes. It is stable across cftp versions.

Field order for each entry, in ascending id order:

| Order | Field | Encoding | Included |
| --- | --- | --- | --- |
| 1 | entry count | `u32` LE | yes |
| 2 | id | `u32` LE, must equal index + 1 | yes |
| 3 | kind | `u8`: 0 file, 1 directory, 2 symlink | yes |
| 4 | size | `u64` LE | yes |
| 5 | mtime | `i64` LE, reinterpreted as `u64` | yes |
| 6 | path length | `u16` LE | yes |
| 7 | path bytes | UTF-8, `/` separators, normalized | yes |
| 8 | link flag | `u8`: 0 or 1 | yes |
| 9 | link length | `u16` LE, present only when flag is 1 | yes |
| 10 | link bytes | UTF-8, present only when flag is 1 | yes |

`mode` is excluded because the server already masks remote modes to `0o777`,
and mode changes should not invalidate resumable content. `mtime` is included
even though it is not content identity: it is the cheapest guard against
editing a source file while preserving its size. A same-second edit with the
same size can still evade it; see section 6.4.

Every variable-length field is length-prefixed. There are no implicit
terminators, and no field is encoded with a variable-length integer encoding.

```rust
pub const MANIFEST_DOMAIN: &[u8] = b"cftp/resume/manifest/v1\0";

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_len16(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u16::try_from(bytes.len()).expect("manifest field fits in u16");
    put_u16(out, len);
    out.extend_from_slice(bytes);
}

pub fn manifest_hash(entries: &[ManifestEntry]) -> [u8; 32] {
    let mut out = Vec::with_capacity(64 * 1024);
    out.extend_from_slice(MANIFEST_DOMAIN);
    put_u32(&mut out, entries.len() as u32);
    for (index, entry) in entries.iter().enumerate() {
        debug_assert_eq!(entry.id, index as u64 + 1);
        put_u32(&mut out, entry.id as u32);
        put_u8(&mut out, match entry.kind {
            EntryKind::File => 0,
            EntryKind::Directory => 1,
            EntryKind::Symlink => 2,
        });
        put_u64(&mut out, entry.size);
        put_u64(&mut out, entry.mtime as u64);
        put_len16(&mut out, entry.path.as_bytes());
        match &entry.link_target {
            None => put_u8(&mut out, 0),
            Some(target) => {
                put_u8(&mut out, 1);
                put_len16(&mut out, target.as_bytes());
            }
        }
    }
    *blake3::hash(&out).as_bytes()
}
```

The server hashes the paths it validated, not the raw client bytes. A client
that sends `a//b/./c` is rejected or normalized exactly as it is today; the
hash always sees the normalized form produced by `path::validate_relative`.
Because `MAX_MANIFEST_PATH_LEN` is 4096, the `u16` prefixes are sufficient.

### 2.2 Root, destination, and key derivation

```rust
pub const ROOT_DOMAIN: &[u8] = b"cftp/resume/root/v1\0";
pub const KEY_DOMAIN: &[u8] = b"cftp/resume/key/v1\0";

pub fn root_fingerprint(canonical_root: &Path) -> [u8; 32] {
    let bytes = canonical_root.as_os_str().as_encoded_bytes();
    let mut out = Vec::with_capacity(ROOT_DOMAIN.len() + 4 + bytes.len());
    out.extend_from_slice(ROOT_DOMAIN);
    put_u32(&mut out, bytes.len() as u32);
    out.extend_from_slice(bytes);
    *blake3::hash(&out).as_bytes()
}

pub fn transfer_key(
    root_fp: &[u8; 32],
    normalized_destination: &str,
    manifest_hash: &[u8; 32],
) -> [u8; 32] {
    let dest = normalized_destination.as_bytes();
    let mut out = Vec::with_capacity(KEY_DOMAIN.len() + 32 + 4 + dest.len() + 32);
    out.extend_from_slice(KEY_DOMAIN);
    out.extend_from_slice(root_fp);
    put_u32(&mut out, dest.len() as u32);
    out.extend_from_slice(dest);
    out.extend_from_slice(manifest_hash);
    *blake3::hash(&out).as_bytes()
}
```

The root is canonicalized once at `serve_once` startup with
`fs::canonicalize`. The destination is the normalized relative path from
`path::validate_destination`; an empty destination is a zero-length field.
`root_fingerprint` is also stored in the journal header so a journal copied
into another root is rejected rather than trusted. `as_encoded_bytes` uses the
platform's `OsStr` encoding; the journal never leaves the server, so
cross-platform stability of the root bytes is not required.

The resulting 32-byte key is rendered as 64 lowercase hex characters for the
journal file name. File names never contain a client string.

### 2.3 Why the key is derived server-side

The server derives the key from data it has already validated:

- the canonical root it is serving;
- the normalized destination after `path::validate_destination`;
- the manifest after `validate_manifest` has rejected duplicate ids, duplicate
  paths, invalid kinds, bad symlink targets, and over-limit sizes.

The client's `transfer_key` in `ClientHelloV4` is an opaque token from a
previous session. The server compares it with its own derivation. Zero means
"no token". A non-zero mismatch causes a fresh journal and a full transfer.
The token is never joined to a path, never used as a map key, and never trusted
to describe which files exist.

This placement matters for security. If the client chose the journal name, it
could attempt traversal, collide with another transfer, or point at a file it
does not own. Because the key is a function of a validated manifest and a
root-local fingerprint, a client can only ever address the journal for the
exact transfer it is actually sending.

### 2.4 Why paths are not hash keys

The transfer key is computed once per session, so hashing paths there is fine.
Per-file state is different:

- Path strings are variable length. Hashing a path per chunk turns an O(1)
  array index into an O(path length) hash plus a hash-table probe.
- Paths are client-controlled and need normalization. Two spellings that map to
  the same on-disk file could otherwise address different state.
- A `HashMap<PathBuf, FileState>` would retain a second copy of every path and
  allocate per entry. The manifest already carries the path once.
- Ids are assigned densely by `scan_source` (`1..=n`) and are validated as
  dense in `cftp/4`. `id - 1` indexes `entries`, `rel_paths`, and every state
  vector directly.
- The journal is a dense on-disk array. A range map or a path map cannot be
  mmap-style indexed or bounded by a simple size formula.

Paths are used only for the manifest hash and for filesystem operations after
the id-to-path lookup has already happened.

### 2.5 Client token cache

The client may persist the last token to catch a mismatched server view:

- Locator: `BLAKE3("cftp/resume/client/v1\0" || destination URL path || manifest_hash)`.
- File: the platform cache directory, `cftp/resume/<hex locator>.token`,
  containing the 64 hex characters of the server token.
- Written with a temporary file, `fsync`, and `rename`; mode `0600`.
- Deleted after a clean `Done`.

If the file is missing, corrupt, or has the wrong length, the client sends a
zero token. The server still derives the key and can resume from its journal;
the token is not required for correctness. This keeps "survive client restarts"
true even when the cache is wiped.

## 3. Wire protocol for `cftp/4`

### 3.1 Version negotiation

- Add `ALPN_V4 = b"cftp/4"` and `PROTOCOL_VERSION_V4 = 4`.
- Advertise `[cftp/4, cftp/3, cftp/2]` in `identity::alpn_protocols` when not
  forced to v2.
- `negotiated_version` maps the selected ALPN to `WireVersion::V4`, `V3`, or
  `V2`. Unknown or absent ALPN still means `V2`, matching today.
- A `cftp/3` peer keeps the existing full-transfer behavior. It never sees a
  journal and never sends resume fields.

A version bump is mandatory, not cosmetic. Postcard serializes enum variants
and struct fields in declaration order. Adding fields to `ClientHelloV3`
changes the bytes that a `cftp/3` peer expects, and adding enum variants changes
the variant index space. `cftp/4` therefore introduces new `ClientMessageV4`
and `ServerMessageV4` enums and leaves the v2/v3 types frozen.

### 3.2 Messages

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHelloV4 {
    pub version: u16,
    pub mode: TransferMode,
    pub destination: String,
    pub entries: Vec<ManifestEntry>,
    pub profile: HardwareProfile,
    /// True only when the user passed --resume.
    pub resume: bool,
    /// Token from the previous session, or [0; 32] when unknown.
    pub transfer_key: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHelloV4 {
    pub version: u16,
    pub accepted: bool,
    pub message: String,
    pub effective_profile: HardwareProfile,
    pub server_cap: HardwareProfile,
    pub profile_accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    /// False when the server disabled resume or the client did not ask.
    pub enabled: bool,
    /// True when a valid journal was loaded.
    pub found: bool,
    /// True when a non-zero client token disagreed with the server key.
    pub token_mismatch: bool,
    pub transfer_key: [u8; 32],
    pub manifest_hash: [u8; 32],
    pub entry_count: u32,
    pub files_total: u32,
    pub files_complete: u32,
    pub bytes_total: u64,
    pub bytes_complete: u64,
    /// Dense bitmap over entry ids 1..=entry_count. Bit 0 is id 1.
    pub complete: Vec<u64>,
    /// Only files with partial chunk coverage. Capped.
    pub partial: Vec<PartialFile>,
    /// True when the partial list was capped; omitted files are resent whole.
    pub partial_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialFile {
    pub id: u32,
    /// Bit u is set when unit u (offset u * RESUME_UNIT) is durable.
    pub covered: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessageV4 {
    Hello(ClientHelloV4),
    Complete(Complete),
    Abort(Abort),
    SetProfile { request_id: u64, profile: HardwareProfile },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessageV4 {
    Hello(ServerHelloV4),
    Resume(ResumeState),
    Done(Done),
    Error(Abort),
    Progress(ServerProgress),
    ProfileChanged {
        request_id: u64,
        accepted: bool,
        effective_profile: HardwareProfile,
        server_cap: HardwareProfile,
        message: String,
    },
}
```

`ServerProgress` and `Done` keep their existing layouts. On a resumed transfer,
the server initializes `files_done` and `bytes_done` from the journal, so the
heartbeat starts at the resumed position and `Done` reports full totals.
`received_bytes` and `written_bytes` remain session-only counters.

The server sends `ServerMessageV4::Resume` immediately after `Hello` when
`hello.resume` is true. It sends no `Resume` message when `resume` is false.
The client waits for `Resume` only in the first case.

### 3.3 Per-file status encoding

A single dense `complete` bitmap covers all entries, including directories and
symlinks. Non-file bits are always zero. This costs `ceil(entry_count / 64)`
words: 1,563 words (12.5 KiB) for `MAX_MANIFEST_ENTRIES = 100_000`. It is much
smaller than a vector of 100,000 structs.

Partially covered files are listed separately. Only chunked files can be
partial, because packs are atomic. In practice the count is bounded by
`MAX_PENDING_FILES = 4096`; the server caps the list at
`MAX_RESUME_PARTIAL_FILES = 4096` and sets `partial_truncated` when it drops
entries. A dropped entry is treated as missing by the client and is resent
whole, which is safe.

The total number of coverage bits is bounded by
`MAX_TRANSFER_BYTES / RESUME_UNIT`: 16 TiB / 1 MiB = 16,777,216 bits = 2 MiB.
That bound is independent of the number of files, because sum of per-file
`ceil(size / RESUME_UNIT)` is at most `total_bytes / RESUME_UNIT + file_count`.

The client expands the message into a dense `Vec<FilePlan>` indexed by `id - 1`,
after checking that `state.manifest_hash` equals its own
`manifest_hash(entries)` and that `state.entry_count` equals the manifest
length. A mismatch is a protocol error and forces a full-transfer retry: the
server's state must describe exactly the manifest the client sent.

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PlanStatus {
    Missing,
    Complete,
    ChunkResume,
}

pub struct FilePlan {
    pub status: PlanStatus,
    /// First unit that must be sent. Cells before this are durable.
    pub first_missing_unit: u32,
    /// Missing cell runs, derived from the bitmap words in the message.
    pub missing_runs: Vec<(u32, u32)>,
}
```

### 3.4 Bounds

| Constant | Value | Purpose |
| --- | --- | --- |
| `RESUME_UNIT` | 1 MiB | Journal coverage granularity |
| `MAX_RESUME_STATE_BYTES` | 8 MiB | Cap for the `Resume` control frame |
| `MAX_RESUME_PARTIAL_FILES` | 4096 | Partial entries in one `ResumeState` |
| `MAX_JOURNAL_BYTES` | 8 MiB | Snapshot and log cap per transfer |
| `MAX_RESUME_HASH_BYTES` | 64 MiB | Optional chunk-hash sidecars per transfer |
| `JOURNAL_FLUSH_BYTES` | 4 MiB | Data fsync batch size |
| `JOURNAL_FLUSH_INTERVAL` | 250 ms | Maximum journaling delay |

`ClientHelloV4` remains bounded by `MAX_CONTROL_FRAME` (64 MiB) and the existing
manifest limits. `Resume` is read with
`read_frame_with_limit::<_, ServerMessageV4>(..., MAX_RESUME_STATE_BYTES)` so a
hostile or buggy peer cannot force an 8 MiB+ allocation. The server refuses to
send a `ResumeState` whose encoded length would exceed that cap; it truncates
the partial list and, if even the complete bitmap would not fit, it sends
`enabled: false` and performs a full transfer.

### 3.5 Handshake flow

1. The client scans, validates manifest limits, and sends `Hello` with
   `resume` and the cached token.
2. The server validates the destination and manifest, requires dense ids for
   v4, derives the key, and loads or discards the journal.
3. The server replies with `Hello`.
4. If `resume` was requested, the server sends `Resume` with the dense complete
   bitmap and partial list.
5. The client builds `Vec<FilePlan>` and starts only the missing work.
6. Data flows exactly as today, except chunked files are written to the partial
   store and journal coverage is updated.
7. `Complete`, drain, journal flush, `Done`.

## 4. Server-side journal

### 4.1 Location and lifecycle

The journal lives under the canonical served root:

```text
<root>/.cftp-resume/                       directory, mode 0700
<root>/.cftp-resume/<hex-key>.snap         compacted snapshot, mode 0600
<root>/.cftp-resume/<hex-key>.log          append-only records, mode 0600
<root>/.cftp-resume/<hex-key>.part/        partial file store, mode 0700
<root>/.cftp-resume/<hex-key>.part/<id>    sparse partial file, mode 0600
<root>/.cftp-resume/<hex-key>.<id>.chunks optional strict hash sidecar, mode 0600
<root>/.cftp-resume/<hex-key>.lock         single-writer lock, mode 0600
```

Rules:

- `<hex-key>` is exactly 64 lowercase hex characters produced by `hex::encode`.
  The server validates the decoded length before any path join.
- The `.cftp-resume` directory is created with mode `0700`. If it exists as a
  symlink or a non-directory, resume is disabled and a warning is logged; the
  transfer continues with a full send.
- Journaling is enabled only for a v4 peer with `resume: true` and a server
  that has not passed `--no-resume`. A normal transfer creates no files here.
- On `resume: true` with an existing journal, the server replays the log and
  reconciles the partial store. On `resume: false`, it removes the key's
  snapshot, log, sidecars, and partial directory before sending data, so a full
  transfer never mixes with stale state. A v4 transfer that cannot honor resume
  (the client did not ask, or the server passed `--no-resume`) performs the
  same removal: a full send can overwrite files that a journal describes, so
  leaving the journal behind would make a later resume unsafe.
- On a clean `Done`, the server fsyncs the journal, then removes the key's
  files and partial directory, and fsyncs `.cftp-resume`. A retry after a lost
  `Done` therefore re-sends; the alternative, retaining a completed marker, is
  possible but costs one dense snapshot per finished transfer.
- On an error or abort, the server makes a best-effort flush and compaction
  before returning, so a retry can resume.

### 4.2 Snapshot layout

All integers are little-endian. The snapshot is always read as a whole and
validated against the manifest before any of it is used.

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 8 | magic | `b"CFTPRSM1"` |
| 8 | 2 | format version | `1` |
| 10 | 2 | flags | bit 0: chunk-hash sidecars present |
| 12 | 4 | entry count | equals manifest length |
| 16 | 32 | transfer key | derived value |
| 48 | 32 | manifest hash | derived value |
| 80 | 32 | root fingerprint | derived value |
| 112 | 4 | file count | entries of kind file |
| 116 | 4 | bits bytes | byte length of the bits arena |
| 120 | 8 | total bytes | sum of file sizes |
| 128 | 8 | files complete | snapshot counter, revalidated on load |
| 136 | 8 | bytes complete | snapshot counter, revalidated on load |
| 144 | 4 | partial count | revalidated on load |
| 148 | 4 | reserved | zero |
| 152 | 8 | reserved | zero |
| 160 | `32 * entry_count` | file records | dense, id order |
| `160 + 32n` | `bits_bytes` | bits arena | concatenated per-file words |
| end - 32 | 32 | footer | BLAKE3 of every preceding byte |

Worst case: `160 + 3,200,000 + 2,097,152 + 32` bytes, about 5.4 MiB, below the
8 MiB cap. The reader checks `160 + 32 * entry_count + bits_bytes + 32` against
the actual file length and `MAX_JOURNAL_BYTES` before allocating.

### 4.3 File record layout

Each record is 32 bytes and is indexed by `id - 1`. Directories and symlinks
use status `NotData`; their bits are zero.

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 1 | status | 0 missing, 1 partial, 2 complete, 3 not-data |
| 1 | 1 | kind | duplicated from the manifest |
| 2 | 2 | reserved | zero |
| 4 | 4 | bits word offset | word index into the arena; `u32::MAX` if none |
| 8 | 4 | bits words | length of this file's bit vector |
| 12 | 4 | covered units | running counter, validated against the bits |
| 16 | 8 | covered bytes | running counter for progress |
| 24 | 4 | total units | `ceil(size / RESUME_UNIT)`, at most 1,048,576 |
| 28 | 4 | reserved | zero |

`total units` fits in `u32` because `MAX_FILE_SIZE` is 1 TiB and
`RESUME_UNIT` is 1 MiB. `bits word offset` fits in `u32` because the arena is
at most 2 MiB, or 262,144 words.

### 4.4 Append-only log

Coverage changes are appended as compact records. The log is never rewritten
in place.

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 4 | magic | `0x52534D31` (`RSM1`) |
| 4 | 2 | kind | 1 cell run, 2 file complete, 3 rename intent, 4 file reset |
| 6 | 2 | payload length | 0..=64 |
| 8 | 4 | file id | 1-based |
| 12 | 4 | start unit | for cell runs |
| 16 | 4 | count | number of units |
| 20 | 8 | checksum | first 8 bytes of BLAKE3 over bytes `0..20` and payload |
| 28 | `payload length` | payload | reserved, must be ignored by v1 readers |

Kinds:

- `CellRun`: units `start_unit .. start_unit + count` are durable.
- `FileComplete`: the id is complete; the final file was renamed and its parent
  directory was fsynced.
- `RenameIntent`: the partial file for the id is about to be renamed. Recovery
  uses it to finish a completion whose final record was lost.
- `FileReset`: all state for the id is cleared (used when a partial file is
  missing, truncated, or fails strict verification).

A record is applied only if its magic, payload length, id range, unit range,
and checksum are valid. Replay stops at the first invalid, truncated, or
out-of-range record. The prefix before that point remains usable; the torn tail
is ignored and the file is truncated to the last good offset before the next
append.

### 4.5 Validation rules

The loader rejects the whole journal and falls back to a full transfer if any
of these checks fails:

1. The journal directory is a real directory, each journal file is a regular
   file rather than a symlink, and each file is within `MAX_JOURNAL_BYTES`.
2. Magic, format version, transfer key, manifest hash, and root fingerprint
   equal the freshly derived values.
3. `entry_count` equals the manifest length; `file_count` and `total_bytes`
   match the manifest; `total_bytes` fits `MAX_TRANSFER_BYTES`; ids are dense.
4. The footer hash matches the snapshot bytes, and each log record checksum
   matches.
5. Every status is in range. `NotData` is allowed only for non-file entries.
   `Complete` is allowed only for files.
6. For `Partial`, `bits_words == ceil(total_units / 64)`, the bit range is
   inside the arena, and `covered units == count_ones(bits)`.
7. `covered_bytes` equals the sum of full units times `RESUME_UNIT`, plus the
   exact tail length when the final unit bit is set.
8. Snapshot counters equal the recomputed sums from the records.
9. For every `Complete` file, the final path exists, is a regular file (not a
   symlink or directory), and has the declared size.
10. For every `Partial` file, the partial file exists and has the declared
    size. If it is missing, short, or longer, that file is reset to missing and
    its bits are cleared.

Rule 9 uses `symlink_metadata`, not `metadata`, so a symlink planted at the
final path is never followed. Rule 10 does not trust the partial file contents;
it trusts the journal bits only after the ordering rules in section 7, and it
re-sends any unit whose bits were not fsynced.

### 4.6 Corruption and fallback

Any validation failure logs a warning with the reason and a key prefix, moves
the key's artifacts to `<hex-key>.bad.<unix-time>` when possible, and starts a
fresh journal. If the move fails, the artifacts are left in place but are not
read. A fresh journal for the same key overwrites the old snapshot and log.

Torn log tails are not corruption. They are the expected result of a crash
between append and fsync, and are discarded without a warning beyond a debug
line.

The footer and record checksums detect accidental corruption and torn writes,
not a local attacker with write access to the served root. Such an attacker can
already modify the destination files. Resume integrity against the network
peer is provided by TLS/SSH authentication and by the BLAKE3 checks on the wire.

## 5. O(1) data structures

### 5.1 Dense id space

`cftp/4` requires ids to be exactly `1..=n`, in ascending order in the manifest.
`scan_source` already assigns them this way. `validate_manifest_v4` rejects any
gap, duplicate, zero, or value above `u32::MAX`. The v2/v3 validation path keeps
its existing tolerance for arbitrary unique ids, because those versions are
frozen.

With dense ids, `ReceiverState` becomes:

| Today | v4 | Access |
| --- | --- | --- |
| `entries: Arc<HashMap<u64, ManifestEntry>>` | `Arc<Vec<ManifestEntry>>` | `entries[(id - 1) as usize]` |
| `rel_paths: Arc<HashMap<u64, PathBuf>>` | `Arc<Vec<PathBuf>>` | `rel_paths[(id - 1) as usize]` |
| `completed: HashSet<u64>` | `Vec<FileState>` plus `files_complete` | `files[(id - 1) as usize]` |
| `ranges: HashMap<u64, ReceivedRanges>` | `Vec<Option<ReceivedRanges>>` | index |
| `in_progress: HashSet<u64>` | `Vec<bool>` | index |
| `in_flight: HashMap<u64, usize>` | `Vec<u32>` | index |
| `pending: HashMap<u64, PendingFile>` | `Vec<Option<PendingFile>>` | index |

The duplicate-path check still uses a `HashSet<String>` during hello
validation. That is O(n) once per session, not a per-chunk cost.

### 5.2 Bit vector

No `unsafe` and no external bit-vector crate:

```rust
#[derive(Debug, Clone, Default)]
pub struct BitVec {
    words: Vec<u64>,
    len: usize,
}

impl BitVec {
    pub fn new(len: usize) -> Self {
        Self { words: vec![0; len.div_ceil(64)], len }
    }

    #[inline]
    pub fn get(&self, index: usize) -> bool {
        debug_assert!(index < self.len);
        (self.words[index >> 6] >> (index & 63)) & 1 != 0
    }

    #[inline]
    pub fn set(&mut self, index: usize) -> bool {
        debug_assert!(index < self.len);
        let word = &mut self.words[index >> 6];
        let mask = 1u64 << (index & 63);
        let was_set = *word & mask != 0;
        *word |= mask;
        !was_set
    }

    pub fn set_run(&mut self, start: usize, count: usize) -> u32 {
        let mut newly = 0;
        for index in start..start + count {
            if self.set(index) {
                newly += 1;
            }
        }
        newly
    }

    pub fn count_ones(&self) -> u32 {
        self.words.iter().map(|word| word.count_ones()).sum()
    }
}
```

`set_run` is linear in the number of units in a chunk, which is at most
`MAX_DATA_PAYLOAD / RESUME_UNIT = 64` for the largest wire chunk and 4 for the
default 4 MiB chunk. The per-unit work is one shift, one mask, and one OR.

### 5.3 File state and the resume database

The in-memory state matches the snapshot layout, including a shared word arena
instead of a `Vec<u64>` per file.

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    Missing,
    Partial,
    Complete,
    NotData,
}

#[derive(Clone, Copy)]
pub struct FileState {
    pub status: FileStatus,
    pub bits_off: u32,
    pub bits_words: u32,
    pub total_units: u32,
    pub covered_units: u32,
    pub covered_bytes: u64,
}

pub struct ResumeDb {
    pub key: [u8; 32],
    pub manifest_hash: [u8; 32],
    pub root_fp: [u8; 32],
    /// Indexed by id - 1; length is entry_count.
    pub files: Vec<FileState>,
    /// Shared arena; each FileState addresses a slice of words.
    pub bits: Vec<u64>,
    pub files_complete: u64,
    pub bytes_complete: u64,
    /// Files with new durable units not yet written to the log.
    pub dirty: Vec<u32>,
}

impl ResumeDb {
    #[inline]
    pub fn file(&self, id: u64) -> &FileState {
        &self.files[(id - 1) as usize]
    }

    #[inline]
    pub fn is_covered(&self, id: u64, unit: u32) -> bool {
        let file = self.file(id);
        if file.bits_words == 0 {
            return false;
        }
        let word = (file.bits_off + (unit >> 6)) as usize;
        (self.bits[word] >> (unit & 63)) & 1 != 0
    }
}
```

A small or packed file never needs a bit vector. Packs are atomic, so such a
file is either `Missing` or `Complete`; the journal stores one status byte and
`ResumeState` exposes it as a single bit in the dense complete bitmap. Only a
file that is streamed in chunks can carry a `bits_words` slice.

The completion check is a running counter, not a scan:

```rust
fn file_is_covered(file: &FileState) -> bool {
    file.total_units == 0 || file.covered_units == file.total_units
}

fn all_complete(db: &ResumeDb, file_count: u64) -> bool {
    db.files_complete == file_count
}
```

### 5.4 Range maps during one session

A `BTreeMap` of ranges is the wrong persistent shape but the right in-memory
shape for the duration of one connection:

- Wire chunks can arrive out of order and can be arbitrarily sized, because
  `--chunk-size` is user-controlled and the light profile uses 256 KiB while
  `RESUME_UNIT` is 1 MiB.
- `ReceivedRanges` already detects overlap, merges adjacent ranges, and answers
  "is this byte range covered" in O(log g), where `g` is the number of disjoint
  gaps. In practice `g` is bounded by in-flight chunks (at most
  `MAX_INFLIGHT_SENDS`) plus one.
- It is transient. Once a unit is fully covered, it is converted to a bit and
  the range information for that unit is discarded.

`prepare_chunk` reserves the chunk's byte range in the per-file
`ReceivedRanges`, exactly as today, so overlapping or duplicate chunks are
rejected before any write starts. Reserving is not the same as completing: a
reserved range may still be in flight.

At `finish_chunk`, units wholly contained in the finished chunk are set
immediately. Edge units wait for the per-file in-flight count to reach zero,
because only then does every reserved range correspond to a completed write.
The server then walks the merged ranges once and sets every fully covered unit:

```rust
fn absorb_chunk(
    file: &mut FileState,
    bits: &mut [u64],
    start: u64,
    len: u64,
    size: u64,
) -> u32 {
    // Called after this chunk's write has completed. Only units wholly
    // contained in this chunk are set here; the rest wait for the drain.
    let end = start + len;
    let first = start / RESUME_UNIT;
    let last = end.div_ceil(RESUME_UNIT).min(file.total_units as u64);
    let mut newly = 0;
    for unit in first..last {
        let unit_start = unit * RESUME_UNIT;
        let unit_end = (unit_start + RESUME_UNIT).min(size);
        if start > unit_start || end < unit_end {
            continue;
        }
        let word = &mut bits[(file.bits_off as usize) + (unit as usize >> 6)];
        let mask = 1u64 << (unit & 63);
        if *word & mask == 0 {
            *word |= mask;
            newly += 1;
        }
    }
    file.covered_units += newly;
    newly
}

fn drain_finished(
    file: &mut FileState,
    ranges: &mut ReceivedRanges,
    bits: &mut [u64],
    size: u64,
) -> u32 {
    // Called only when the per-file in-flight count is zero, so every
    // reserved range has been written.
    let mut newly = 0;
    for (&start, &end) in ranges.iter() {
        let first = start / RESUME_UNIT;
        let last = end.div_ceil(RESUME_UNIT).min(file.total_units as u64);
        for unit in first..last {
            let unit_start = unit * RESUME_UNIT;
            let unit_end = (unit_start + RESUME_UNIT).min(size);
            if start > unit_start || end < unit_end {
                continue;
            }
            let word = &mut bits[(file.bits_off as usize) + (unit as usize >> 6)];
            let mask = 1u64 << (unit & 63);
            if *word & mask == 0 {
                *word |= mask;
                newly += 1;
            }
        }
    }
    file.covered_units += newly;
    ranges.retain_unfinished(size, RESUME_UNIT);
    newly
}
```

The `min(size)` term is what makes a non-unit-aligned final file work: the
last unit is complete when a range covers up to the file's exact end, not when
it covers a full `RESUME_UNIT`. Both loops are bounded by the units touched by
the chunk or range, not by the file's total unit count.

`drain_finished` iterates merged ranges and touches at most the units inside
them. It then retains only sub-unit remainders, which keeps the map bounded. An
aligned chunk never needs the drain to complete its own units, but it still
participates in reservation and overlap checking.

`covers_range` is a small addition to `ReceivedRanges` implemented with the
same `range(..=start).next_back()` probe as `covers`. The journal never stores
byte ranges, so the unbounded-range shape never reaches disk.

### 5.5 Why a persisted range map is the wrong shape

- Entry size varies. A range map serializes as a length plus one
  `(start, end)` pair per gap, so its size is not a simple function of the
  manifest. It cannot be preallocated or bounded by
  `total_bytes / RESUME_UNIT`.
- Lookups during replay are O(log g) and require pointer chasing, while a bit
  test is one shift and one mask.
- A hostile or unusual peer can create many disjoint gaps. Bounding `g` would
  require a second policy, whereas the bit vector is bounded by construction.
- Compaction would have to walk and serialize every range. A snapshot can
  instead copy dense arrays.
- The bit vector supports the exact operation the resume protocol needs: "is
  unit `u` durable?" A range map answers a superset of that question at higher
  cost.

`ReceivedRanges` stays exactly where it is useful: in memory, for overlap
rejection and unaligned edges during a single transfer. Its `insert` and
`covers` methods remain; add `iter`, `covers_range`, and `retain_unfinished`
helpers so the drain can prune units that are now bits.

## 6. Verification without hashing hot paths

### 6.1 Trust chain

The fast path stores no chunk hashes. It relies on ordering:

1. `decode_stream` already decompresses a chunk and verifies its BLAKE3 hash
   before the write task is spawned.
2. The chunk is written with `write_all_at` at its declared offset.
3. The partial file is fsynced in a batch.
4. Only after the fsync does the server append `CellRun` records for the units
   covered by that batch.
5. The journal is fsynced.

Therefore a journal bit implies "these bytes were BLAKE3-verified, written, and
durable". Re-hashing them on resume would repeat work the wire protocol already
did and would turn a resume into a full read of the partial data.

Resume does validate structure cheaply: the snapshot footer, every replayed
record checksum, the bit-count arithmetic, and the size of each complete or
partial file. A missing or wrong-sized file resets that id.

### 6.2 Optional strict sidecar

For operators who want detection of disk corruption or tampering, a sidecar
stores per-unit hashes for partial files only. The journal flag `HASHES_PRESENT`
records that at least one sidecar exists.

```text
<root>/.cftp-resume/<hex-key>.<id>.chunks
offset 0   8 bytes   magic b"CFTPCHK1"
offset 8   2 bytes   version (1)
offset 10  2 bytes   reserved
offset 12  4 bytes   file id
offset 16  4 bytes   unit count
offset 20  32 * unit count bytes of BLAKE3 hashes
```

A hash slot is meaningful only when the corresponding coverage bit is set.
Hashes are written with `write_all_at` at `20 + unit * 32` before the journal
record for that unit, and are fsynced with the same batch. They are never kept
in RAM in bulk.

On resume in strict mode, the server reads the partial file in unit-sized
buffers, hashes each covered unit, and compares. A mismatch appends
`FileReset` and re-sends from that unit. The cost is proportional to the
covered bytes of partial files, so strict mode trades resume latency and IO for
detection. It is off by default.

### 6.3 Budget and alternatives

| Mode | Extra disk per TiB | Resume read cost | Detects |
| --- | --- | --- | --- |
| Journal trust (default) | 0 | None; one stat per file | Torn writes, truncation, journal corruption |
| Strict sidecar | 32 MiB | Re-read covered partial data | Silent bit rot, tampering |
| Completed-prefix hash | 32 bytes per file | Re-read the contiguous prefix | Corruption only in a contiguous prefix |

A 1 TiB file at 1 MiB units needs 1,048,576 hashes, which is exactly 32 MiB.
The worst case for a whole transfer, 16 TiB of partial files, would be 512 MiB,
above `MAX_RESUME_HASH_BYTES = 64 MiB`. When the cap is reached the server
clears `HASHES_PRESENT` for new files, stops creating sidecars, and continues in
journal-trust mode. Existing sidecars remain readable but are not extended.

A flat in-memory `Vec<[u8; 32]>` indexed by dense unit was considered and
rejected for the default path. It is 32 MiB for one 1 TiB file but 512 MiB at
the 16 TiB transfer cap, and it duplicates the durability guarantee that the
journal already provides. In strict mode a single bounded per-file allocation
can be used while a file is being verified and then dropped; the persistent
copy is always the sidecar, never the whole transfer in RAM.

The completed-prefix hash is cheap and can detect corruption of the earliest
durable data, but it cannot validate holes or a file whose coverage starts in
the middle. It is not a substitute for the flat per-unit sidecar in strict mode.

### 6.4 Source mutation

Resume assumes the source did not change between attempts. The manifest hash
includes size and mtime, so the usual edit changes the key and produces a full
transfer. Two cases remain:

- An editor that preserves both size and mtime within the filesystem's mtime
  resolution can produce a mixed file: old durable units from the previous
  content, new units from the current content.
- A complete file is skipped without consulting the current source at all.

The default design accepts this and documents it. Stronger protection would
require a client-computed content digest per file in the manifest. That would
make `scan_source` read every byte before the transfer, which defeats the
scan-then-stream design and is a non-goal here. `--resume-strict` can be added
later as an opt-in that stores sidecar hashes and verifies partial files; it
still cannot validate skipped complete files without a manifest digest.

## 7. Write protocol and crash consistency

### 7.1 Partial store

Chunked files no longer use an anonymous temp file in the destination
directory. They use a stable sparse file in the journal:

```text
<root>/.cftp-resume/<hex-key>.part/<id>
```

- Created with `create_new(true)` and mode `0600`, then `set_len(size)`. The
  file is sparse; unset regions read as zeros but are never trusted because
  only journaled units are skipped.
- On resume, an existing file is opened read-write without truncation after a
  `symlink_metadata` check that it is a regular file of the declared size.
- The `Drop` implementation for a partial file must not delete it. This is the
  opposite of today's `PendingFile::drop`, which removes its temp file. Resume
  data survives an aborted session; cleanup is explicit.
- The path contains only the server-derived hex key and a decimal `u32` id.
  No client path, link target, or token appears in it.
- The final destination is still protected by
  `path::prepare_file_path`, which rejects symlinks, directories, and missing
  ancestors, and by `ensure_ancestors`, which refuses symlink traversal.

### 7.2 Packed files

Packs remain atomic. Each entry is written with `write_atomic`: a temp file in
the destination parent, `sync_all`, metadata, `rename`, then the `FileComplete`
record and fsync. Because a pack either finishes or is re-sent, a packed file
is never `Partial`.

The destination-parent temp file can survive a process kill as an orphan under
the existing `.cftp.tmp.<pid>.<counter>` name. Two options are acceptable:

- Leave it and let it be ignored. It is hidden, bounded by one pack, and never
  read by resume.
- Route pack temp files through `<root>/.cftp-resume/<hex-key>.tmp/` with a
  `write_atomic_in` helper, and fall back to the destination parent on
  `EXDEV`. This keeps every resume artifact in one directory.

The second option is recommended but is not on the critical path for the first
implementation.

### 7.3 Chunked files

For a chunk at `offset` with length `len`:

1. `prepare_chunk` validates the id, kind, size, and overlap. In v4 it also
   rejects any unit in `[offset / RESUME_UNIT, (offset + len) / RESUME_UNIT)`
   that is already set. A set unit is immutable for the session. The byte range
   is reserved in the per-file `ReceivedRanges` before the write starts, so a
   second chunk cannot be prepared over the same bytes.
2. The chunk payload was already hash-verified and decompressed.
3. The blocking writer calls `write_all_at(&partial_file, offset, &raw)`.
4. `finish_chunk` sets the units wholly contained in this chunk. When the
   per-file in-flight count reaches zero, it drains the merged ranges and sets
   the edge units; if the file is now fully covered, it enters the completion
   path.

Completion path:

1. `partial.sync_all()`.
2. Apply mode and mtime to the partial file.
3. Append `RenameIntent { id, size }` to the log and fsync it.
4. `path::prepare_file_path(root, relative)` and `fs::rename(partial, final)`.
5. `sync_dir(final.parent())`.
6. Append `FileComplete { id }` and fsync the log.
7. Update `files_complete` and `bytes_complete`, remove the sidecar, and
   publish progress.

A crash between steps 3 and 6 is recoverable: on load, an intent without a
complete record and without a partial file is resolved by checking that the
final path exists as a regular file of the declared size. If it does, the id is
marked complete; otherwise the id is reset and resent.

### 7.4 Flush policy

A dedicated journal flusher task owns all journal writes and fsyncs. It never
runs on the data-write path.

- A file becomes dirty when a unit is newly covered. Newly covered units are
  accumulated as runs; adjacent runs merge, so a 4 MiB chunk usually produces
  one run.
- Flush when `JOURNAL_FLUSH_BYTES` of newly covered bytes accumulate or when
  `JOURNAL_FLUSH_INTERVAL` elapses, whichever comes first.
- Per flush, process at most 64 dirty files. For each, `sync_data()` the
  partial file, append its runs, then `sync_data()` the log once for the whole
  batch.
- Keep a file dirty if its sync or append fails. A persistent error aborts the
  session; the journal keeps whatever was already durable.
- Flush before `Complete` handling and before `Done`. The server does not
  report completion for a file whose data or journal record is not durable.
- Bound the log: if an append would exceed `MAX_JOURNAL_BYTES`, compact first.
  If compaction fails, abort the session rather than exceed the cap.
- Compaction runs only after a full flush. The snapshot therefore never
  records a bit whose data fsync has not completed.

`sync_data` is used for batches because only file contents matter; `sync_all`
is used at file completion where metadata (mode and mtime) must also be
durable.

### 7.5 Crash matrix

| Crash point | Partial data | Journal | Resume action |
| --- | --- | --- | --- |
| During write, before data fsync | Some bytes possibly present | No units | Resend affected units |
| After data fsync, before append | Durable units | No units | Resend and overwrite |
| After append, before log fsync | Durable units | Record possibly torn | Replay stops; resend affected units |
| After log fsync | Durable units | Unit bits durable | Skip units |
| After rename, before `FileComplete` | Final file durable | `RenameIntent` | Verify size, mark complete |
| After `FileComplete` | Final file durable | Complete | Verify size, skip file |

The invariant is one-directional: data is durable before its bit is durable.
The reverse (bit durable, data not durable) cannot happen through the normal
path. A kernel that lies about fsync is outside the threat model.

### 7.6 Compaction

The snapshot plus log is compacted:

- at session start if the replayed log exceeds `max(1 MiB, snapshot_bytes / 2)`;
- before an append would exceed `MAX_JOURNAL_BYTES`;
- after `Complete`, before `Done`, and on a best-effort basis on error.

Compaction writes `<key>.snap.tmp`, fsyncs it, renames it over `<key>.snap`,
fsyncs `.cftp-resume`, then truncates or removes `<key>.log`. A crash during
compaction leaves the old snapshot and log intact; `.snap.tmp` is ignored and
removed on the next load.

## 8. Security

### 8.1 Journal paths

- The key is 32 bytes derived by the server, rendered with `hex::encode`, and
  length-checked before use. It cannot contain `/`, `..`, NUL, or a drive
  prefix.
- The partial file name is the decimal representation of a validated `u32` id.
- The `.cftp-resume` directory is created with `create_dir`/`DirBuilderExt`
  mode `0700` and is rejected if it is a symlink or not a directory.
- Journal files are created with `create_new` and mode `0600`; existing files
  are opened only after `symlink_metadata` confirms they are regular files.
- The manifest reserves the first path component `.cftp-resume` at the
  destination root. A client cannot write into the journal directory by naming
  it in the manifest, a destination, or a symlink target. The check belongs in
  `path.rs` and applies to v4 manifests and destinations.
- There is a small TOCTOU window between `symlink_metadata` and `open` without
  `O_NOFOLLOW`. The directory is mode `0700` and owned by the serving user, so
  exploiting it requires local write access to the server's root. If this needs
  to be closed later, add an `openat2`/`O_NOFOLLOW` helper behind a Unix
  extension; do not use `unsafe` in this crate.

### 8.2 Permissions

Directories are `0700` (traversal requires execute permission, so a `0600`
directory would be unusable) and files are `0600`. This keeps partial data and
the manifest hash private to the serving user, even when the served root is
readable by others. On Windows, rely on the default ACL and document that the
guarantee is best-effort.

### 8.3 Root binding

The key and the journal header both include `root_fingerprint`. A journal
copied or moved into a different root fails validation and is discarded.
Renaming the served root changes the fingerprint, so the old journal is
orphaned rather than silently reused. `fs::canonicalize` is evaluated once at
startup; two paths that resolve to the same root produce the same fingerprint.

### 8.4 The client cannot name files

A client supplies only:

- a destination string, validated by `validate_destination`;
- a manifest, validated for duplicate ids, duplicate paths, absolute paths,
  traversal, symlink escapes, size limits, and path length;
- a `resume` flag;
- an opaque 32-byte token.

The server maps ids to paths from its own validated vectors. The token is
compared, not used. The journal name comes from the derived key. There is no
client-controlled string in any journal or partial-store path.

### 8.5 Denial-of-service bounds

- Manifest limits are unchanged: `MAX_MANIFEST_ENTRIES`, `MAX_MANIFEST_PATH_LEN`,
  `MAX_FILE_SIZE`, `MAX_TRANSFER_BYTES`, and `MAX_PACK_ENTRIES`.
- Journal size is bounded analytically at about 5.4 MiB and enforced at
  `MAX_JOURNAL_BYTES = 8 MiB`.
- The complete bitmap is 12.5 KiB at the entry cap; coverage bits are at most
  2 MiB; the partial list is capped at 4096 entries.
- `ResumeState` is capped at 8 MiB; overflow truncates partial statuses, which
  forces a resend rather than a large allocation.
- Partial files are created only for ids that are actually being written, and
  at most `MAX_PENDING_FILES = 4096` are open at once.
- A journal directory can still grow across many authenticated transfers. Prune
  it at startup: remove keys and `.part` directories older than a configurable
  TTL (default 7 days) and keep the directory under a configurable size (for
  example 1 GiB), oldest first. Pruning only considers names matching the
  key/extension patterns.
- An authenticated client can delete another transfer's journal only by
  presenting the same destination and manifest (the same derived key). Sharing
  a destination is already a trust relationship; cross-user isolation is a
  non-goal.

### 8.6 Hostile manifests

The v4 validation path is strict:

- ids must be dense and within `u32`;
- paths must pass `validate_relative`, must not contain NUL, must be UTF-8,
  and must not start with `.cftp-resume`;
- duplicate normalized paths are rejected;
- symlink targets must be present, relative, and must not traverse an existing
  symlink;
- sizes must respect `MAX_FILE_SIZE` and `MAX_TRANSFER_BYTES`;
- directories and symlinks must declare size zero;
- a pack or chunk may reference only a file id in the manifest.

### 8.7 Concurrency and locking

`serve` is one-shot per process, but an operator can start two processes on
different ports against the same root. The journal format is append-only, but
two writers would interleave records and corrupt counters. Take
`<key>.lock` with `create_new` for the duration of a resumable session. The
lock contains the pid and start time, and its mtime is refreshed by the
progress heartbeat. A lock whose mtime has not changed for `RESUME_LOCK_STALE`
(5 minutes) is treated as stale and removed; otherwise the second server
disables resume and performs a full transfer. This is a best-effort guard, not
a distributed lock.

## 9. Performance budget

### 9.1 Session setup

| Work | Typical | Worst case |
| --- | --- | --- |
| Canonical manifest hash | Path and metadata bytes, one pass | Manifest bounded by `MAX_CONTROL_FRAME` |
| Key derivation | Three BLAKE3 passes over small buffers | Constant |
| Journal read | Header plus 5.4 MiB | `MAX_JOURNAL_BYTES` |
| Journal validation | O(entry_count) | 100,000 records |
| Partial store reconciliation | One stat per complete/partial file | 100,000 stats |
| `ResumeState` serialization | 12.5 KiB plus partial bits | 8 MiB cap |

There is no full-file hashing and no re-read of complete data. The stat pass is
the only per-file filesystem cost, and it is needed to reject missing or
wrong-sized files.

### 9.2 Hot path

Per data unit, the receive path performs:

- one array index for the entry, one for the path, and one for the state;
- `set` or `set_run` on a `u64` word;
- an integer increment on `covered_units`;
- a comparison for file completion.

There is no `HashMap` lookup, no `HashSet` insert, and no path hashing. The
journal flusher amortizes one `sync_data` per file per 4 MiB or 250 ms, plus
one log fsync per batch. At 1 Gbps, a 4 MiB batch is about 34 ms of data, so
the 250 ms interval usually wins and the re-send window is bounded by a quarter
second of throughput (or 4 MiB, whichever is smaller) per active file.

The unaligned edge path is at most two `ReceivedRanges` probes per chunk.
Aligned chunks still reserve a range so overlap is rejected, but they need no
range probe to complete their own units. The range map is therefore touched
once per chunk for reservation and only probed when a chunk edge falls inside
a unit.

### 9.3 Pack and chunk interaction

- The client's producer receives `Vec<FilePlan>` before it starts reading.
  Complete ids are skipped entirely: no read, no compression, no send.
- Missing small files are packed as usual. Skipping complete files changes pack
  composition but not pack semantics; a pack contains only files that still
  need data.
- A partial file is always resumed by chunk, even if its current size is below
  `small_file_max`. Sending it in a pack would resend the whole file. If the
  client chooses to pack it anyway, the server treats a pack as authoritative:
  it clears the id's partial bits, removes the partial file, writes the packed
  file atomically, and marks it complete.
- Resumed chunk sends start at `first_missing_unit * RESUME_UNIT`, so the
  client re-sends at most one partially filled unit (under 1 MiB) per file. The
  client may use any chunk size for the missing region; the server's unit
  conversion handles unaligned edges.
- `chunk_size` may change between attempts because it is part of the tuning
  profile. The fixed `RESUME_UNIT` keeps the journal stable across profiles.
  Users are not required to pass the same `--chunk-size` twice.
- A complete id is never sent data for. Existing duplicate-chunk and
  duplicate-pack checks remain, now backed by the bit vector.

### 9.4 Progress heartbeat

The heartbeat task is unchanged and keeps ticking every second. The server
initializes `files_done`, `bytes_done`, and the progress publication from the
journal, so the first heartbeat already shows resumed progress. `received_bytes`
and `written_bytes` start at zero and measure this session only, which matches
their current meaning. The client's `SendState.files_done` and `bytes_done` are
initialized from `ResumeState` for the same reason; `stats.files` and
`stats.bytes` are replaced by the server's `Done` totals at the end.

No control reader is blocked while data is sent or journaled. The flusher is a
separate task; journal fsyncs never run under the `ReceiverState` mutex.

### 9.5 Memory budget

| Structure | Bound |
| --- | --- |
| Dense manifest vectors | `MAX_MANIFEST_ENTRIES` times entry and path size |
| `Vec<FileState>` | 32 bytes times 100,000, about 3.2 MiB |
| Coverage arena | 2 MiB across the whole transfer |
| Complete bitmap in `ResumeState` | 12.5 KiB |
| Partial status list | 4096 entries plus 2 MiB of bits |
| Pending partial files | `MAX_PENDING_FILES` open handles |
| Journal buffers | One snapshot at a time, at most 8 MiB |
| Optional sidecars | Written to disk; at most one hash per unit in flight |

## 10. Rollout plan

### 10.1 Negotiation and feature flags

- Advertise `cftp/4` first, then `cftp/3`, then `cftp/2`.
- A `cftp/3` server selects `cftp/3`; the client sees `WireVersion::V3` and
  never sends resume fields. Behavior is byte-for-byte the current transfer.
- A `cftp/4` server with `--no-resume` still accepts v4 but replies
  `ResumeState { enabled: false, .. }`. Before the full send it removes any
  existing journal for the derived key, because the full send can overwrite
  files that the journal describes.
- The client enables resume only with `--resume`. Without it, it sends
  `resume: false`, and the server removes any existing journal for the derived
  key and does not create one. This makes the feature opt-in and keeps the
  default path free of journaling overhead.
- After the first release, `--resume` can become the default. The wire format
  does not change.
- A v3 peer does not send resume fields and does not derive a v4 key, so it
  cannot invalidate an existing journal. Operators mixing v3 clients with
  resume-enabled v4 clients on the same destination and manifest should treat
  the v3 full send as invalidating the resume state manually.

### 10.2 Test plan

Unit tests in `crates/cftp-core/src/resume.rs`:

- Canonical encoding golden vectors: a fixed manifest produces a pinned hex
  `manifest_hash`, `root_fingerprint`, and `transfer_key`.
- Field-order stability: changing mode or mtime changes only the expected
  fields; changing mode does not change the hash; changing mtime does.
- `BitVec` get, set, set_run overlap, count_ones, and boundary words.
- Snapshot encode/decode round trip, footer mismatch, truncated file, bad
  magic, bad version, wrong key, wrong root, wrong entry count.
- Log replay: valid prefix plus torn tail, bad checksum, out-of-range id,
  out-of-range unit, `FileReset`.
- Counter reconstruction and validation of `covered_bytes` including a
  non-unit-aligned final file.
- `absorb_chunk` with aligned chunks, unaligned chunks, out-of-order chunks,
  and gaps.
- Partial-store reconciliation with missing, short, and oversized files.
- Dense-id validation rejects gaps, duplicates, and zero.

End-to-end tests in `crates/cftp-core/tests/resume.rs`:

- Interrupt a large `put` after N bytes, restart the server and client, assert
  that the second session sends less than the full byte count and that the
  destination tree matches byte for byte.
- Interrupt after some packs and some chunks, including a file larger than
  `RESUME_UNIT` and a file smaller than `small_file_max`.
- Server restart between attempts, proving the journal is durable.
- Journal corruption (flip a footer byte, truncate the snapshot, corrupt a log
  record) causes a full transfer that still succeeds.
- Hostile manifest: duplicate ids, non-dense ids, `.cftp-resume` path, path
  traversal, symlink escape. Each is rejected before any file is created.
- Cross-root: copy a journal into a different root and assert it is discarded.
- Partial file truncated or deleted between attempts: only that id is resent.
- Pack-vs-chunk transition: run the second attempt with a different
  `--small-file-max` and `--chunk-size`.
- `--resume` with a server `--no-resume`: full transfer, no journal files.
- Version matrix: a v3 client against a v4 server and a v4 client against a v3
  server both complete with a full send.

CLI tests in `crates/cftp-cli/tests/cli.rs`:

- `--resume` is accepted and shown in `--help`.
- `serve --no-resume` is accepted.
- A resumed transfer prints a notice with resumed file and byte counts.

### 10.3 Metrics and logging

Use structured `tracing` fields so tests and operators can assert behavior:

- `resume.key_prefix`: first 16 hex characters of the key.
- `resume.requested`, `resume.enabled`, `resume.found`,
  `resume.token_mismatch`, `resume.fallback_reason`.
- `resume.files_complete`, `resume.bytes_complete`,
  `resume.partial_files`, `resume.resent_bytes`.
- `journal.records_replayed`, `journal.torn_tail`,
  `journal.compactions`, `journal.fsyncs`, `journal.bytes`.
- `partial.files_open`, `partial.reset_missing`, `partial.reset_size`.

Log a single info-level notice on resume, and a warning with a reason on every
fallback. Never log full paths at info level; existing notices already print
relative source paths on the client.

## 11. Change inventory

| File | Change | Rough lines |
| --- | --- | --- |
| `crates/cftp-proto/src/lib.rs` | `ALPN_V4`, `PROTOCOL_VERSION_V4`, `RESUME_UNIT`, resume caps, `ClientHelloV4`, `ServerHelloV4`, `ResumeState`, `PartialFile`, `ClientMessageV4`, `ServerMessageV4`, tests | 220 |
| `crates/cftp-core/src/resume.rs` (new) | Key derivation, canonical manifest encoder, `BitVec`, `FileState`, `ResumeDb`, snapshot and log codecs, validation, compaction, partial store, optional hash sidecars, tests | 1,300 |
| `crates/cftp-core/src/wire.rs` | `WireVersion::V4`, negotiation, v4 read/write for hello, resume, progress, done, error, profile, tests | 180 |
| `crates/cftp-core/src/transfer.rs` | Dense `ReceiverState`, v4 manifest validation, journal load and flush task, partial-store chunk path, resume planner in the producer, progress initialization, cleanup | 600 |
| `crates/cftp-core/src/path.rs` | Journal directory and file helpers, partial-path helpers, `finish_partial`, `sync_dir`, reserved `.cftp-resume` check, tests | 140 |
| `crates/cftp-core/src/identity.rs` | ALPN list includes `cftp/4` | 5 |
| `crates/cftp-core/src/scan.rs` | Skip a top-level `.cftp-resume` when scanning a directory source | 10 |
| `crates/cftp-core/src/lib.rs` | `pub mod resume;` | 1 |
| `crates/cftp-cli/src/cli.rs` | `put --resume`, `serve --no-resume` | 15 |
| `crates/cftp-cli/src/main.rs` | Pass flags, load and store the optional client token, resume notices | 45 |
| `crates/cftp-core/tests/resume.rs` (new) | Interrupted, corrupt, hostile, cross-version, cross-root e2e tests | 600 |
| `crates/cftp-cli/tests/cli.rs` | Flag and notice tests | 60 |
| `docs/architecture.md` | Wire format, transfer flow, known limitations | 25 |
| `README.md` | `--resume` documentation | 15 |

Total implementation is roughly 2,700 lines of production code and 1,000 lines
of tests. Most of the production code is the journal codec and validation,
which are isolated in `resume.rs` and can be reviewed independently.

Key function signatures:

```rust
pub fn manifest_hash(entries: &[ManifestEntry]) -> Result<[u8; 32]>;
pub fn root_fingerprint(canonical_root: &Path) -> [u8; 32];
pub fn transfer_key(root_fp: &[u8; 32], destination: &str, manifest: &[u8; 32]) -> [u8; 32];

pub fn load(root: &Path, key: [u8; 32], manifest_hash: [u8; 32], entries: &[ManifestEntry])
    -> Result<ResumeDb>;
pub fn open_journal(root: &Path, key: [u8; 32]) -> Result<Journal>;
pub fn replay(journal: &mut Journal, db: &mut ResumeDb) -> Result<()>;
pub fn append(journal: &mut Journal, record: &LogRecord) -> Result<()>;
pub fn compact(journal: &mut Journal, db: &ResumeDb) -> Result<()>;
pub fn to_resume_state(db: &ResumeDb, entries: &[ManifestEntry]) -> Result<ResumeState>;

pub fn create_partial(root: &Path, key: [u8; 32], id: u32, size: u64)
    -> Result<File>;
pub fn finish_partial(root: &Path, key: [u8; 32], id: u32, file: &mut File,
    relative: &Path, mode: u32, mtime: i64) -> Result<()>;
```

## 12. Risks and mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| fsync ordering bug marks data durable before it is | Resumed file contains holes or stale bytes | Single flusher, bits only after `sync_data`, crash-matrix tests with fault injection |
| Source changed with the same size and mtime | Mixed old and new units | mtime in the key, documented assumption, opt-in strict mode, future manifest digest |
| Journal directory symlink or TOCTOU | Writes outside the journal | 0700 directory, `create_new`, regular-file checks, reserved path component; add `O_NOFOLLOW` later if needed |
| Two servers on one root | Corrupted counters and interleaved logs | `<key>.lock` with stale detection; document one server per root |
| Journal grows without bound | Disk exhaustion | 8 MiB per transfer, TTL and total-size pruning at startup |
| `ResumeState` too large | Control frame pressure or failure | 8 MiB cap, truncate partial list, fall back to full |
| Crash between rename and `FileComplete` | A large file is resent | `RenameIntent` plus size check on recovery |
| Pack temp file orphaned | Hidden disk usage | Ignore, or route pack temps through the journal directory |
| Different chunk size or profile on retry | Cells misaligned | Fixed `RESUME_UNIT`, unaligned edge handling, any chunk size accepted |
| A partial file is truncated externally | Silent corruption | Size check resets the id; strict sidecar detects content corruption |
| Windows rename or directory fsync semantics differ | Completion error or no durable directory entry | `sync_dir` is best-effort on non-Unix; test on Windows before enabling resume there |
| `Done` lost after a clean transfer | Full resend on retry | Optional completed-marker retention; default deletes the journal |
| Scanning a root that contains `.cftp-resume` | Journal is transferred as user data | `scan_source` skips a top-level `.cftp-resume` directory |
| v3 peers see v4 fields | Decode failure | ALPN-selected v4 enums only; v3 types frozen |
| A v3 full send overwrites a destination with a live journal | A later resume mixes old and new data | v4 non-resume sends delete the journal; document that v3 clients do not invalidate it |
| Local attacker edits journal bits | Wrong files skipped | Out of scope for a root-writer attacker; checksums catch accidental corruption |

The riskiest part is not the bit vector or the wire format; it is the durability
ordering. The implementation should land the journal codec and its fault tests
before the transfer-engine changes, so the ordering invariant can be tested in
isolation.
