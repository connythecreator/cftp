use super::*;

#[derive(Debug)]
pub(super) enum Destination {
    Remote {
        host: String,
        port: u16,
        destination: String,
    },
    Local(PathBuf),
}

/// A remote endpoint reached through an SSH bootstrap session.
#[derive(Debug, Clone)]
pub(super) struct ViaSsh {
    /// Value handed to `ssh` (`user@host`, or just `host`).
    pub(super) target: String,
    /// Bare host used for DNS, known_hosts, and the QUIC connection.
    pub(super) host: String,
    pub(super) port: u16,
}

pub(super) fn parse_via_ssh(value: &str) -> Result<ViaSsh> {
    let value = value.trim();
    if value.is_empty() {
        bail!("--via-ssh needs [user@]host[:port]");
    }
    let (user, rest) = match value.split_once('@') {
        Some(("", _)) => bail!("--via-ssh is missing a user before '@': {value}"),
        Some((user, rest)) => (Some(user), rest),
        None => (None, value),
    };
    if rest.is_empty() {
        bail!("--via-ssh is missing a host: {value}");
    }
    if rest.contains('@') {
        bail!("--via-ssh accepts only one '@': {value}");
    }
    let (host, port) = if let Some(rest) = rest.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .with_context(|| format!("invalid bracketed IPv6 address in --via-ssh: {value}"))?;
        let port = match after {
            "" => DEFAULT_PORT,
            _ => after
                .strip_prefix(':')
                .with_context(|| format!("invalid --via-ssh port: {value}"))?
                .parse::<u16>()
                .with_context(|| format!("invalid --via-ssh port: {value}"))?,
        };
        (host.to_string(), port)
    } else {
        match rest.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') && !port.is_empty() => (
                host.to_string(),
                port.parse::<u16>()
                    .with_context(|| format!("invalid --via-ssh port: {value}"))?,
            ),
            _ => (rest.to_string(), DEFAULT_PORT),
        }
    };
    if host.is_empty() {
        bail!("--via-ssh is missing a host: {value}");
    }
    if host.starts_with('-') || user.is_some_and(|user| user.starts_with('-')) {
        bail!("--via-ssh does not accept a host or user beginning with '-': {value}");
    }
    let target = match user {
        Some(user) => format!("{user}@{host}"),
        None => host.clone(),
    };
    Ok(ViaSsh { target, host, port })
}

/// A remote one-shot server started over SSH.
///
/// Dropping the guard kills the SSH session, so a failed local transfer does not
/// leave a server waiting for a connection.
pub(super) struct SshBootstrap {
    pub(super) child: tokio::process::Child,
    pub(super) port: u16,
}

impl SshBootstrap {
    pub(super) async fn finish(mut self) -> Result<()> {
        match tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await {
            Ok(Ok(status)) if status.success() => Ok(()),
            Ok(Ok(status)) => bail!("remote cftp exited with {status}"),
            Ok(Err(err)) => Err(err).with_context(|| "failed to wait for ssh"),
            Err(_) => {
                let _ = self.child.kill().await;
                Ok(())
            }
        }
    }
}

impl Drop for SshBootstrap {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Starts `cftp serve --one-shot --bootstrap` on the remote host over SSH.
///
/// The client's public key goes to the remote process's stdin (so it never shows
/// up in a process list) and the remote server answers with
/// `CFTP-READY <port> <host public key>`. That key is appended to the local
/// known_hosts file: the authenticated SSH channel is the trust root.
pub(super) async fn start_ssh_bootstrap(
    via: &ViaSsh,
    client_public: &str,
    known_hosts: &Path,
) -> Result<SshBootstrap> {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    let listen = format!(
        "{}:{}",
        if via.host.contains(':') {
            "[::]"
        } else {
            "0.0.0.0"
        },
        via.port
    );
    let mut command = tokio::process::Command::new("ssh");
    command
        // `--` stops ssh from interpreting a hostile target such as
        // `-oProxyCommand=...` as an option; it is a destination, not a flag.
        .arg("--")
        .arg(&via.target)
        .args([
            "cftp",
            "serve",
            "--one-shot",
            "--bootstrap",
            "--quiet",
            "--root",
            ".",
        ])
        .arg("--listen")
        // ssh joins remote arguments through the remote shell, so quote the
        // address: IPv6 brackets would otherwise be a glob pattern in zsh.
        .arg(format!("'{listen}'"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // If anything below fails, dropping the child must not leave the remote
    // one-shot server waiting for a connection that will never come.
    command.kill_on_drop(true);
    let mut child = command
        .spawn()
        .with_context(|| "failed to run `ssh`; OpenSSH is required for --via-ssh")?;

    {
        let mut stdin = child.stdin.take().context("failed to open ssh stdin")?;
        stdin
            .write_all(client_public.as_bytes())
            .await
            .context("failed to send the client key over ssh")?;
        stdin.write_all(b"\n").await?;
        let _ = stdin.shutdown().await;
    }

    let stdout = child
        .stdout
        .take()
        .context("failed to capture ssh stdout")?;
    let mut reader = BufReader::new(stdout);
    let ready = tokio::time::timeout(Duration::from_secs(120), async {
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line).await?;
            if read == 0 {
                bail!(
                    "the remote cftp exited before announcing an endpoint (is cftp installed on {}?)",
                    via.target
                );
            }
            if let Some(rest) = line.trim_end().strip_prefix("CFTP-READY ") {
                return Ok::<String, anyhow::Error>(rest.to_string());
            }
        }
    })
    .await;
    let rest = match ready {
        Ok(Ok(rest)) => rest,
        Ok(Err(err)) => return Err(err),
        Err(_) => {
            let _ = child.start_kill();
            bail!(
                "timed out waiting for {} to start cftp; check that ssh works and cftp is installed there",
                via.target
            );
        }
    };
    let (port_text, key_text) = rest
        .split_once(' ')
        .context("the remote cftp sent a malformed ready line")?;
    let port: u16 = port_text
        .parse()
        .context("the remote cftp sent an invalid port")?;
    let public =
        SshPublicKey::from_openssh(key_text).context("the remote cftp sent an invalid host key")?;

    let mut store = KnownHostsStore::load(known_hosts)?;
    match store.status(&via.host, port, public.spki()) {
        HostKeyStatus::Trusted => {}
        HostKeyStatus::Unknown => store.append(&via.host, port, &public).with_context(|| {
            format!("failed to record the host key in {}", known_hosts.display())
        })?,
        HostKeyStatus::Mismatch { fingerprints } => bail!(
            "{} announced a host key that differs from the one pinned in {} ({}); if the host \
             key really changed, remove the stale entry and retry",
            via.target,
            known_hosts.display(),
            fingerprints.join(", ")
        ),
        HostKeyStatus::Revoked { fingerprints } => bail!(
            "{} announced a host key that is revoked in {} ({})",
            via.target,
            known_hosts.display(),
            fingerprints.join(", ")
        ),
    }

    // Keep draining stdout so the remote process does not die on SIGPIPE when
    // it prints its closing summary.
    tokio::spawn(async move {
        let mut sink = [0u8; 4096];
        while matches!(reader.read(&mut sink).await, Ok(read) if read > 0) {}
    });

    Ok(SshBootstrap { child, port })
}

pub(super) const DEFAULT_PORT: u16 = 4433;

pub(super) fn parse_destination(dest: &str, via: Option<&ViaSsh>) -> Result<Destination> {
    if dest.contains("://") {
        let url = Url::parse(dest).with_context(|| format!("invalid destination URL: {dest}"))?;
        if url.scheme() != "cftp" {
            bail!("destination must use the cftp:// scheme");
        }
        let host = url
            .host_str()
            .with_context(|| format!("destination has no host: {dest}"))?
            .to_string();
        if let Some(via) = via
            && !host.eq_ignore_ascii_case(&via.host)
        {
            bail!(
                "--via-ssh connects to {} but the URL names {host}; use the same host or a plain path",
                via.host
            );
        }
        if let Some(via) = via
            && let Some(url_port) = url.port()
            && url_port != via.port
        {
            bail!(
                "--via-ssh uses port {} but the URL names {url_port}; put the port on --via-ssh only",
                via.port
            );
        }
        let port = url
            .port()
            .or_else(|| via.map(|via| via.port))
            .unwrap_or(DEFAULT_PORT);
        let path = url.path().trim_start_matches('/').to_string();
        return Ok(Destination::Remote {
            host,
            port,
            destination: path,
        });
    }

    if let Some(via) = via {
        // With --via-ssh a bare path is the remote path under the SSH user's
        // home directory, which is what `scp` users will type.
        return Ok(Destination::Remote {
            host: via.host.clone(),
            port: via.port,
            destination: dest.trim_start_matches('/').to_string(),
        });
    }

    if dest.starts_with("cftp:") {
        bail!("invalid cftp URL: {dest}");
    }
    if let Some((host, path)) = scp_like(dest) {
        bail!(
            "`{dest}` looks like scp syntax, which cftp does not accept; use `cftp://{host}/{}` (or pass `--via-ssh {host}` and give `{}` as the destination to start a server over SSH)",
            path.trim_start_matches('/'),
            path.trim_start_matches('/')
        );
    }
    Ok(Destination::Local(PathBuf::from(dest)))
}

/// Recognizes `[user@]host:path` so the user gets a useful error instead of a
/// silent local copy. Windows drive prefixes (`C:\...`) are not scp syntax.
pub(super) fn scp_like(dest: &str) -> Option<(&str, &str)> {
    let (host, path) = dest.split_once(':')?;
    if host.is_empty() || path.is_empty() {
        return None;
    }
    if host.contains('/') || host.contains('\\') {
        return None;
    }
    if host.len() == 1 && host.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    let host = host.rsplit_once('@').map(|(_, host)| host).unwrap_or(host);
    (!host.is_empty()).then_some((host, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_via_ssh_targets() {
        let via = parse_via_ssh("me@example.com").unwrap();
        assert_eq!(via.target, "me@example.com");
        assert_eq!(via.host, "example.com");
        assert_eq!(via.port, DEFAULT_PORT);

        let via = parse_via_ssh("me@example.com:5555").unwrap();
        assert_eq!(via.target, "me@example.com");
        assert_eq!((via.host.as_str(), via.port), ("example.com", 5555));

        let via = parse_via_ssh("server").unwrap();
        assert_eq!(via.target, "server");
        assert_eq!(via.host, "server");

        let via = parse_via_ssh("[::1]:5555").unwrap();
        assert_eq!((via.host.as_str(), via.port), ("::1", 5555));

        assert!(parse_via_ssh("").is_err());
        assert!(parse_via_ssh("@example.com").is_err());
        assert!(parse_via_ssh("me@example.com:notaport").is_err());
        assert!(parse_via_ssh("-oProxyCommand=evil").is_err());
        assert!(parse_via_ssh("-o@example.com").is_err());
    }

    #[test]
    fn scp_like_ignores_local_looking_paths() {
        assert_eq!(scp_like("host:/path"), Some(("host", "/path")));
        assert_eq!(scp_like("me@host:path"), Some(("host", "path")));
        assert_eq!(scp_like("./host:path"), None);
        assert_eq!(scp_like("C:\\Users\\me"), None);
        assert_eq!(scp_like("plain"), None);
        assert_eq!(scp_like(":path"), None);
    }

    #[test]
    fn a_bare_path_is_remote_only_with_via_ssh() {
        assert!(matches!(
            parse_destination("docs", None).unwrap(),
            Destination::Local(_)
        ));
        let via = parse_via_ssh("me@server").unwrap();
        match parse_destination("/docs", Some(&via)).unwrap() {
            Destination::Remote {
                host,
                port,
                destination,
            } => {
                assert_eq!(host, "server");
                assert_eq!(port, DEFAULT_PORT);
                assert_eq!(destination, "docs");
            }
            Destination::Local(_) => panic!("expected a remote destination"),
        }
    }

    #[test]
    fn a_scp_style_destination_is_rejected_with_a_url_hint() {
        let err = parse_destination("host:/srv/files", None).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("scp"), "{message}");
        assert!(message.contains("cftp://host/srv/files"), "{message}");
    }
}
