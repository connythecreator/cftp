//! UDP socket setup for QUIC endpoints.
//!
//! `quinn` binds its sockets with the kernel defaults, which are sized for
//! general-purpose traffic and overflow under a burst of QUIC datagrams on a
//! lossy or high-latency path. An overflow drops datagrams before quinn can
//! acknowledge them, which the congestion controller then reads as loss. We
//! bind the socket ourselves so the buffers can be sized from [`Tuning`].

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, Endpoint, EndpointConfig, ServerConfig, UdpPoller, default_runtime};
use socket2::{Domain, Protocol, Socket, Type};

use crate::identity::Mtu;
use crate::tuning::Tuning;

/// Binds a UDP socket for `bind` and wraps it in a QUIC endpoint.
///
/// `server_config` is `None` for a client endpoint. `mtu` selects how datagrams
/// are batched for sending; the endpoint always advertises the largest payload
/// it can receive, so a peer on a jumbo path can use it. Buffer sizing is
/// best-effort: a kernel may clamp or refuse it, and that must not stop a
/// transfer that would otherwise work.
pub(crate) fn bind_endpoint(
    bind: SocketAddr,
    server_config: Option<ServerConfig>,
    tuning: &Tuning,
    mtu: Mtu,
) -> Result<Endpoint> {
    let socket: UdpSocket = bind_socket(bind, tuning.socket_buffer)?.into();
    let runtime = default_runtime().context("a QUIC endpoint needs an active Tokio runtime")?;
    let mut endpoint_config = EndpointConfig::default();
    endpoint_config
        .max_udp_payload_size(Mtu::MAX_DATAGRAM)
        .expect("valid maximum UDP payload size");
    let socket: Arc<dyn AsyncUdpSocket> = runtime
        .wrap_udp_socket(socket)
        .context("failed to wrap the UDP socket")?;
    let socket: Arc<dyn AsyncUdpSocket> = match mtu.gso_segments() {
        Some(segments) => Arc::new(CappedGsoSocket {
            inner: socket,
            segments,
        }),
        None => socket,
    };
    Endpoint::new_with_abstract_socket(endpoint_config, server_config, socket, runtime)
        .with_context(|| format!("failed to start a QUIC endpoint on {bind}"))
}

/// Caps the number of datagrams quinn packs into one GSO `sendmsg`.
///
/// quinn always batches up to ten datagrams per send. On a path using jumbo
/// datagrams that becomes a buffer above the 64 KiB Linux allows for UDP GSO,
/// and the send fails, stalling the connection. Reporting the smaller limit
/// keeps each buffer inside the kernel's cap so larger datagrams can be used.
#[derive(Debug)]
struct CappedGsoSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    segments: usize,
}

impl AsyncUdpSocket for CappedGsoSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.inner.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        self.inner.try_send(transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.segments
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

fn bind_socket(bind: SocketAddr, buffer: usize) -> Result<Socket> {
    let domain: Domain = if bind.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket: Socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))
        .with_context(|| format!("failed to create a UDP socket for {bind}"))?;
    if bind.is_ipv6() {
        // Match std's wildcard behaviour: accept IPv4-mapped peers too.
        let _ = socket.set_only_v6(false);
    }
    socket
        .bind(&bind.into())
        .with_context(|| format!("failed to bind UDP {bind}"))?;

    if buffer > 0 {
        if let Err(err) = socket.set_recv_buffer_size(buffer) {
            tracing::warn!("could not size the UDP receive buffer to {buffer} bytes: {err}");
        }
        if let Err(err) = socket.set_send_buffer_size(buffer) {
            tracing::warn!("could not size the UDP send buffer to {buffer} bytes: {err}");
        }
    }

    Ok(socket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_a_udp_socket_with_sized_buffers() {
        let socket: Socket = bind_socket("127.0.0.1:0".parse().unwrap(), 1024 * 1024).unwrap();
        assert_ne!(
            socket
                .local_addr()
                .unwrap()
                .as_socket()
                .expect("ip socket")
                .port(),
            0
        );
        // The kernel clamps to rmem_max/wmem_max, so require a usable buffer
        // rather than exactly what we asked for.
        assert!(socket.recv_buffer_size().unwrap() > 0);
        assert!(socket.send_buffer_size().unwrap() > 0);
    }

    #[tokio::test]
    async fn endpoint_binds_to_an_ephemeral_port() {
        let endpoint: Endpoint = bind_endpoint(
            "127.0.0.1:0".parse().unwrap(),
            None,
            &Tuning::default(),
            Mtu::Ethernet,
        )
        .unwrap();
        assert_ne!(endpoint.local_addr().unwrap().port(), 0);
        endpoint.close(0u32.into(), b"done");
    }
}
