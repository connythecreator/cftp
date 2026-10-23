# Security

## Authentication

- SSH keys are used as TLS raw public keys over QUIC. cftp does not implement
  the SSH transport.
- The server loads a host key (default `~/.ssh/id_ed25519`) and checks client
  keys against `authorized_keys`.
- The client loads `~/.ssh/id_ed25519`, `id_ecdsa`, or `id_rsa`, and pins the
  server in `known_hosts`. Wildcards, port entries, hashed entries, and
  `@revoked` markers are supported.
- Unknown host keys prompt for confirmation or are added with `--accept-new`.
  A changed key always fails.
- `authorized_keys` options cftp cannot enforce (such as `from=` and
  `command=`) are ignored with a warning.
- SSH certificates and `ssh-agent` are not supported.

Certificate mode (`--auth cert`) uses mutual TLS with a shared local CA. Both
peers must trust the same `ca.pem`; the leaf certificate is used for client and
server authentication.

## Path safety

Received paths are security-sensitive and are never joined onto the root
directly:

- Absolute paths, `..` components, and empty or non-UTF-8 paths are rejected.
- Symlink targets that escape the root are rejected.
- The normalized path is checked against the destination root before use.
- Files are written to a temporary file in the destination directory and
  renamed into place only after every hash matches and all bytes are present.
- Symlink creation is Unix-only.

Pulls use the same confinement in the other direction. The server resolves the
client-supplied source with `path::resolve_read_path`, which walks components
one at a time and rejects symlinks anywhere in the path, so a pull cannot read
outside the root or be redirected through a link. Directory contents are then
scanned by the normal `scan_source` path, which also refuses escaping symlinks.

The helpers in `cftp-core/src/path.rs` implement these rules. Any integration
that reads a requested path or writes received paths must use them.

## Wire safety

- Manifests, frames, headers, and payloads are bounded (`MAX_MANIFEST_ENTRIES`,
  `MAX_CONTROL_FRAME`, `MAX_DATA_HEADER`, `MAX_DATA_PAYLOAD`, file and transfer
  size limits). Declared lengths are rejected before allocation.
- Ids must be dense `1..=n`; out-of-range and duplicate ids are rejected.
- Every pack entry and chunk carries a BLAKE3 hash that is verified before the
  data is committed.
- A malformed session fails with an error; it does not abort the persistent
  server.

## Deployment

- Restrict the UDP port (`4433` by default) to peers you trust.
- Protect SSH private keys and `identity.key` with restrictive permissions.
- The server root is shared by concurrent sessions, so a client with access can
  overwrite any path it names and pull any readable path under it. Run separate
  servers for different trust domains.
- Use `serve --no-push` or `--no-pull` when a deployment should be one-way. The
  policy is enforced before any scan or path resolution, and a disabled mode is
  reported in the server hello.
- A pull exposes the same files that are readable by the server process under
  `--root`. Mount only the tree you intend to publish.
- Use `--one-shot` in wrappers that expect the process to exit after one
  transfer; the default server stays alive.
- Certificate identity files are written by `identity init`; protect `ca.pem`
  if peers must not be able to mint identities.

## Reporting

Report vulnerabilities through the process in the
[security policy](../../.github/SECURITY.md). Do not open a public issue for an
unfixed vulnerability.
