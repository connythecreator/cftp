# Troubleshooting

## Nothing connects

- Confirm both sides use the same UDP port (`4433` by default). cftp does not
  use TCP.
- Open the UDP port in the host firewall and any cloud security group. TCP
  reachability tells you nothing about QUIC.
- Bind to an address the peer can reach. `--listen 0.0.0.0:4433` is the
  default; `127.0.0.1` is loopback-only.
- A middlebox that drops long-lived UDP flows can stall a transfer. The client
  reports a stalled session instead of hanging; check `RUST_LOG=cftp=debug` on
  both sides.

Useful checks:

```bash
ss -ulpn | grep 4433
RUST_LOG=cftp=debug cftp serve --root ./incoming
RUST_LOG=cftp=debug cftp put ./src cftp://host:4433/dest
```

## Authentication failures

**`the authenticity of host ... can't be established`** — the server key is not
in `known_hosts`. Confirm the fingerprint out of band, then add it with
`--accept-new` (or answer the prompt). A changed key for a known host is always
rejected; remove the stale entry only after verifying the change.

**`client SSH key is not listed in authorized_keys`** — append the sender's
public key to the receiver's `authorized_keys`:

```bash
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
```

**Encrypted key / `failed to decrypt`** — the CLI prompts, but the library
examples and many embeddings do not. Decrypt the key, use an agent-backed
unencrypted key file, or supply a passphrase callback to
`load_private_key` / `load_client_auth` / `load_server_auth`.

**Certificate mode** — both peers need the same `ca.pem`, and the client's
default `--server-name` is `cftp.local`. A certificate for a different name
needs `--server-name`.

## The server exits or does not exit when expected

- `serve` stays alive and accepts repeated transfers, up to four at a time.
  This is the default. Pass `--one-shot` when a wrapper expects one session and
  then exit.
- Extra clients wait for a free slot; they do not fail. Raise `--max-sessions`
  if that wait is unwanted, but it increases memory and file-descriptor use.
- `listener closed before a client connected` means the endpoint was closed
  without a successful handshake. Check authentication and UDP reachability.

## Transfer errors

| Message | Meaning |
| --- | --- |
| `frame too large` | A declared control frame or header exceeded its bound. |
| `frame length is zero` | The peer sent an empty control frame. |
| `hash mismatch` | A pack entry or chunk failed BLAKE3 verification; the file was not committed. Retry the transfer. |
| `manifest id ...` / `duplicate manifest id` | A custom caller built a manifest whose ids are not dense `1..=n`. `push` always generates dense ids. |
| `path escapes the destination` | A path or symlink target traverses outside the root, on either a push or a pull. |
| `push is disabled on this server` / `pull is disabled on this server` | The server was started with `--no-push` or `--no-pull`. Use the other direction or change the server policy. |
| `server does not support pull (cftp/4 required; ...)` | One peer is older than `cftp/4`. Pull has no v2/v3 equivalent; upgrade the server or the client. |
| stalled-session timeout | No data or control traffic arrived for the idle window. Treat it as a network or peer failure, not a hang. |

Files are written through temporary files and renamed after verification, so a
failed transfer does not leave a partial file at the final path. Temporary files
may remain if the process is killed.

## Missing or duplicate content

- Without a trailing slash, a directory source is transferred as a directory
  inside the destination; with a trailing slash, its contents are transferred.
- Empty directories are recreated from the manifest.
- Symlinks are recreated on Unix only. On Windows, symlink entries are not
  preserved.
- Non-UTF-8 filenames on Unix are rejected; backslashes in names are not
  preserved.

## Profiles

`cftp/2` peers ignore profile flags and hotkeys and use default tuning. A
server caps client requests at its `--light`/`--default`/`--aggressive` setting;
a rejected request leaves the current profile unchanged and the transfer
continues.

## Packaging and completions

- Completions are generated at build time (`make assets`) into
  `packaging/generated/` and installed by the packages. Start a new shell after
  installing, or re-source the completion file, before expecting them.
- Debian installs the zsh completion under `vendor-completions`; Arch uses
  `site-functions` and `vendor_completions.d`. Both are standard directories;
  no extra configuration is needed.
- If `make deb` cannot find assets, run it from the repository root.

## Benchmarks

`make bench` needs a release build and runs loopback transfers. `scp`, FTP, and
HTTP baselines are skipped when their tools or targets are missing; see
`bench/README.md` for the environment variables and flags.
