//! Shared client plumbing: QUIC connection setup and option validation.

use super::*;

pub(super) async fn connect_client(
    addr: SocketAddr,
    auth: &ClientAuth,
    tuning: &Tuning,
    force_v2: bool,
    server_name: &str,
) -> Result<(Endpoint, Connection)> {
    let bind: SocketAddr = if addr.is_ipv6() {
        "[::]:0".parse().expect("valid ipv6 bind address")
    } else {
        "0.0.0.0:0".parse().expect("valid ipv4 bind address")
    };
    let mtu = Mtu::for_peer(addr);
    let mut endpoint: Endpoint = bind_endpoint(bind, None, tuning, mtu)?;
    let client_config = auth.client_config(tuning, force_v2, mtu)?;
    endpoint.set_default_client_config(client_config);
    let connecting = endpoint
        .connect(addr, server_name)
        .with_context(|| format!("failed to start connection to {addr}"))?;
    let connection: Connection = match tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting).await {
        Err(_) => bail!(
            "timed out after {}s establishing a connection to {addr}",
            HANDSHAKE_TIMEOUT.as_secs()
        ),
        Ok(Ok(connection)) => connection,
        Ok(Err(err)) => {
            if let Some(unknown) = auth.take_unknown_host_key() {
                return Err(anyhow::Error::new(unknown));
            }
            return Err(err).with_context(|| format!("failed to connect to {addr}"));
        }
    };
    Ok((endpoint, connection))
}

pub(super) fn validate_tuning(tuning: &Tuning) -> Result<()> {
    if tuning.chunk_size == 0 || tuning.chunk_size > cftp_proto::MAX_DATA_PAYLOAD {
        bail!(
            "chunk size must be between 1 and {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if tuning.pack_target == 0 || tuning.pack_target > cftp_proto::MAX_DATA_PAYLOAD {
        bail!(
            "pack target must be between 1 and {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if tuning.small_file_max > cftp_proto::MAX_DATA_PAYLOAD as u64 {
        bail!(
            "small-file threshold cannot exceed {} bytes",
            cftp_proto::MAX_DATA_PAYLOAD
        );
    }
    if tuning.pack_max_files == 0 {
        bail!("pack max files must be greater than zero");
    }
    if tuning.max_inflight_sends == 0 || tuning.max_inflight_streams == 0 {
        bail!("in-flight limits must be greater than zero");
    }
    Ok(())
}
