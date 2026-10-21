# Architecture and protocol

cftp is split into three crates:

- `cftp-proto` defines wire types and framing.
- `cftp-core` implements scanning, path safety, compression, SSH/TLS
  authentication, and the transfer engine.
- `cftp-cli` provides the `cftp` binary.

## Transfer flow

1. The client scans the source, assigns each entry an id, and sends a
   `ClientHello` manifest over a bidirectional control stream.
2. The server validates the destination, manifest paths, ids, and symlink
   targets, then replies with `ServerHello`.
3. Data travels on many unidirectional streams. Each stream begins with a
   `DataHeader` frame and is followed by raw payload bytes.
4. The server sends periodic `Progress` messages with receive and write
   counters. The client combines them with its own read and send counters to
   render throughput in the progress bar.
5. After the client sends `Complete`, the server replies with `Done` and the
   final counters.

## Wire format

- ALPN is `cftp/2`; `PROTOCOL_VERSION` is `2`.
- Control messages are a 4-byte big-endian length followed by a postcard payload.
- `MAX_CONTROL_FRAME` is 256 MiB and `MAX_DATA_PAYLOAD` is 64 MiB.
- Data payloads are not length-framed; their lengths come from the data header.

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
- The server decodes up to 32 streams concurrently and serializes state changes
  and disk writes behind a mutex.
- Throughput can still be limited by the network, disk, or QUIC flow control
  rather than CPU count.

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
- Files are written to temporary files and renamed into place only after they
  are complete.
- QUIC protects data in transit. cftp does not add application-layer encryption
  beyond TLS.
- Restrict the UDP port to peers you trust. Protect SSH private keys and
  `identity.key` with restrictive file permissions.

To report a vulnerability, see [the security policy](../.github/SECURITY.md).

## Known limitations

- `serve` handles one connection per process.
- Transfers are not resumed across sessions; interrupted transfers restart.
- `ssh-agent` and SSH certificate authentication are not supported yet.
- Filenames must be valid UTF-8. On Unix, filenames containing backslashes are
  not preserved.
- Symlink creation is supported on Unix only.
- Throughput figures are per-transfer application estimates, not operating
  system interface counters.
