<!-- markdownlint-disable MD033 MD041 -->
<p align="center">
  <img src="../../assets/cftp-logo.png" alt="cftp logo" width="180">
</p>
<!-- markdownlint-enable MD033 -->

# cftp wiki

cftp (Connys File Transfer Program) pushes and pulls files and directory trees
over QUIC. It authenticates peers with SSH keys by default, compresses with zstd,
verifies with BLAKE3, packs small files, and chunks large ones. The server is
authoritative: it decides which directions it offers and which paths clients may
read or write.

This wiki is the guided tour. The [README](../../README.md) is the command
reference, and the source is the specification.

## Contents

| Page | What it covers |
| --- | --- |
| [Usage](Usage.md) | Installing, first transfer, profiles, local copies, certificate mode. |
| [Architecture](Architecture.md) | Crate boundaries, concurrency, liveness, where state lives. |
| [Protocol](Protocol.md) | ALPN, framing, manifests, packs and chunks, compatibility rules. |
| [Library](Library.md) | Embedding `cftp-core` as a client or server without the CLI. |
| [Security](Security.md) | Authentication, path confinement, atomic writes, deployment. |
| [Troubleshooting](Troubleshooting.md) | Common failures and what the error messages mean. |

## In one paragraph

A transfer starts with a manifest over a QUIC bidirectional stream, followed by
many unidirectional data streams carrying packs (small files) or chunks (large
files). On a push the sender scans, the receiver validates and creates the tree,
and the receiver reports progress on the control stream. On a pull the server
does the scanning and sending, and the client validates the manifest before it
creates anything locally. Every BLAKE3 hash is verified, and files appear at
their final path only after a temp-file rename. `serve` stays alive by default;
`--one-shot` restores the single-session behaviour, and `--no-push` / `--no-pull`
restrict which directions the server accepts.

## Publishing this wiki

GitHub keeps a wiki in a separate repository. These pages are plain Markdown in
`docs/wiki/`, so they can be copied into a `cftp.wiki.git` checkout as the
starting point. Repository-relative links (`../architecture.md`,
`../../README.md`) work in the repository; adjust them to wiki page links after
copying.
