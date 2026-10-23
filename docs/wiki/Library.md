# Using cftp-core as a library

The `cftp` binary is only a front end. `cftp-core` exposes both halves of a
transfer as ordinary async functions, so another Rust program can send or
receive files without spawning the CLI.

`cftp-cli` depends on `cftp-core`; the dependency does not go the other way.
Add the path dependency:

```toml
[dependencies]
cftp-core = { path = "crates/cftp-core" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time"] }
anyhow = "1"
```

Import the common types through the prelude:

```rust
use cftp_core::prelude::*;
```

## Receiving (server half)

```rust
use std::net::SocketAddr;
use std::path::Path;

use cftp_core::prelude::*;
use cftp_core::auth::load_server_auth;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let auth = load_server_auth(
        Path::new("/home/me/.ssh/id_ed25519"),
        Path::new("/home/me/.ssh/authorized_keys"),
        |_| Ok(String::new()), // passphrase for encrypted host keys
    )?;

    let (progress, mut events) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            println!("{event:?}");
        }
    });
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (session_tx, mut sessions) = tokio::sync::mpsc::unbounded_channel::<ServeStats>();
    tokio::spawn(async move {
        while let Some(stats) = sessions.recv().await {
            println!("session: {} files, {} bytes", stats.files, stats.bytes);
        }
    });

    let mut options = ServeOptions::new("./incoming", "0.0.0.0:4433".parse::<SocketAddr>()?);
    options.one_shot = false;          // stay alive for repeated transfers
    options.progress = Some(progress);
    options.on_ready = Some(ready_tx);
    options.on_session = Some(session_tx);

    let server = tokio::spawn(async move { serve(&auth, options).await });
    println!("listening on {}", ready_rx.await?);
    server.await??;
    Ok(())
}
```

`ServeOptions::new` defaults to `one_shot = true`, matching the historical
library behaviour. The CLI opts into `one_shot = false`. `serve_once` forces the
single-session mode explicitly.

Key points:

- `on_ready` yields the actual bound `SocketAddr`, which matters when binding
  port `0`.
- `on_session` yields one `ServeStats` per completed session in persistent mode.
- `max_sessions` bounds concurrency; extra clients wait for a permit.
- Drop or abort the `serve` task to stop the endpoint.

## Sending (client half)

```rust
use std::path::Path;

use cftp_core::prelude::*;
use cftp_core::auth::load_client_auth;
use cftp_core::trust::KnownHostsStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let auth = load_client_auth(
        Path::new("/home/me/.ssh/id_ed25519"),
        KnownHostsStore::load("/home/me/.ssh/known_hosts")?,
        "receiver",
        4433,
        HostKeyPolicy::Ask, // or AcceptNew / Strict
        |_| Ok(String::new()),
    )?;

    let (progress, mut events) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            println!("{event:?}");
        }
    });

    let options = PushOptions {
        destination: "project".into(),
        progress: Some(progress),
        ..PushOptions::default()
    };
    let stats = push(Path::new("./project"), "127.0.0.1:4433".parse()?, &auth, options).await?;
    println!("sent {} files, {} bytes", stats.files, stats.bytes);
    Ok(())
}
```

`push` scans the source itself. The destination is relative to the receiver's
root and follows the same trailing-slash rules as the CLI.

## Pulling (client half, server authoritative)

`pull` asks the server for a path under its root and writes the returned tree
locally. The server is authoritative: it decides the path, the enabled modes,
and the hardware profile.

```rust
use cftp_core::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let auth = cftp_core::auth::load_client_auth(/* as above */)?;

    let options = PullOptions {
        source: "project".into(),   // path under the server's --root
        destination: "./downloads".into(),
        ..PullOptions::default()
    };
    let stats = pull("127.0.0.1:4433".parse()?, &auth, options).await?;
    println!("received {} files, {} bytes", stats.files, stats.bytes);
    Ok(())
}
```

- `source` is relative to the server root. Trailing-slash rules match the CLI:
  `project/` transfers the contents, `project` preserves the directory name.
- `destination` is created if missing. Every path in the returned manifest is
  validated with the same rules as a push before anything is written.
- `pull` requires `cftp/4`; it fails with an explicit error against an older
  server or one whose `ServePolicy` disables pull.
- `PullStats` carries `files`, `bytes` (logical bytes written), `wire_bytes`
  (payload bytes received), and `effective_profile`.

## Server policy

`ServeOptions::policy` is a `ServePolicy` fixed for the endpoint:

```rust
let mut options = ServeOptions::new("./shared", addr);
options.policy = ServePolicy::pull_only();   // or push_only()
```

- `ServePolicy::default()` allows both directions.
- `ServePolicy::push_only()` refuses `pull` with `pull is disabled on this server`.
- `ServePolicy::pull_only()` refuses `push` with `push is disabled on this server`.
- A policy that disables both is rejected when `serve` starts.

The policy is reported to the client in `ServerHelloV4` as `allow_push` and
`allow_pull`, and a disabled mode is rejected before any scan or data.

Progress arrives as `ProgressEvent` values (`Scan`, `Sending`, `Receiving`,
`Complete`, `Notice`). The channel is unbounded; keep the receiver alive until
`push`, `pull`, or `serve` returns, otherwise sends become no-ops (they never
block the transfer). Drop the `PullOptions`/`PushOptions` value (or the
`ProgressSender` it holds) before awaiting a task that drains the channel, or
the drain will wait forever.

## Authentication backends

SSH raw public keys are the default:

- Server: `load_server_auth(host_key, authorized_keys, passphrase)`.
- Client: `load_client_auth(key, known_hosts, host, port, policy, passphrase)`.
- `HostKeyPolicy::Ask` records an unknown key and fails the handshake;
  `take_unknown_host_key()` returns it so your UI can prompt, and
  `accept_host_key()` persists it. `AcceptNew` trusts and stores an unknown key;
  `Strict` rejects it.

Certificate mode uses the same `Identity` files as the CLI:

```rust
use cftp_core::auth::{ClientAuth, ServerAuth};
use cftp_core::identity::Identity;

let identity = Identity::load("identity.pem", "identity.key", "ca.pem")?;
let server_auth = ServerAuth::Cert(identity.clone());
let client_auth = ClientAuth::Cert(identity);
```

## Examples in the repository

Two runnable examples exercise the API end to end:

```bash
cargo run -p cftp-core --example server -- ./incoming 127.0.0.1:4433
cargo run -p cftp-core --example client -- ./project cftp://127.0.0.1:4433/project
```

The client example also accepts `get`, which pulls a path from the server into a
local directory:

```bash
cargo run -p cftp-core --example client -- get cftp://127.0.0.1:4433/project ./downloads
```

They use `~/.ssh` for keys and reject encrypted keys, because prompting would
add a dependency the example does not have.

## Web backends and downloads

A browser still cannot download files from cftp directly: browsers do not speak
QUIC with this ALPN or postcard framing. `pull` is for another cftp peer, not
for HTTP clients.

To expose cftp-transferred files to browsers, put an HTTP front end in front of
the server's root: receive with `serve` (or `push` into a storage service), then
let a normal HTTP file server handle `GET`, `HEAD`, and `Range` requests. The
core library deliberately does not bundle an HTTP server, which keeps its
dependency set small.
