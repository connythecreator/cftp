use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use directories::BaseDirs;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};

use crate::congestion::AdaptiveCongestionConfig;
use crate::tuning::Tuning;

pub const DEFAULT_SERVER_NAME: &str = "cftp.local";

/// Largest datagram used on a path known to carry jumbo frames.
///
/// A QUIC packet is capped at just under 16 KiB by its 14-bit length field,
/// and 16,000 carries ten Ethernet-sized packets in one datagram.
const LOOPBACK_DATAGRAM: u16 = 16_000;

/// Bytes of UDP payload Linux accepts in one GSO buffer.
const GSO_BUFFER_LIMIT: u32 = 64 * 1024;

/// Datagram sizing for a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mtu {
    /// Conservative assumption for arbitrary internet peers: quinn's defaults
    /// (1,200-byte initial MTU, discovery up to Ethernet).
    Ethernet,
    /// The peer is known to be local (loopback), where the path MTU is 65,536
    /// bytes. The connection starts at the jumbo datagram size instead of
    /// waiting for path MTU discovery: a saturating transfer never leaves a
    /// gap for the discovery probes, so a 1,200-byte MTU would otherwise be
    /// used for the whole transfer.
    Loopback,
}

impl Mtu {
    /// Largest datagram this implementation accepts or sends.
    pub(crate) const MAX_DATAGRAM: u16 = LOOPBACK_DATAGRAM;

    pub(crate) fn max_udp_payload_size(self) -> u16 {
        match self {
            Self::Ethernet => 1500 - 28,
            Self::Loopback => LOOPBACK_DATAGRAM,
        }
    }

    fn initial_mtu(self) -> u16 {
        match self {
            Self::Ethernet => 1_200,
            Self::Loopback => LOOPBACK_DATAGRAM,
        }
    }

    /// Initial congestion window for this path.
    ///
    /// RFC 6928 starts at ten packets. With jumbo datagrams a ten-packet
    /// window is larger than the 12 KiB default, and a window smaller than one
    /// datagram would deadlock the handshake.
    fn initial_window(self) -> u64 {
        match self {
            // quinn's default window; ten 1,200-byte packets.
            Self::Ethernet => 10 * 1_200,
            Self::Loopback => 10 * u64::from(self.max_udp_payload_size()),
        }
    }

    /// How many datagrams may share one GSO send for this path.
    ///
    /// `None` leaves quinn's default in place. Jumbo datagrams need the batch
    /// capped so a single `sendmsg` stays within the kernel's limit.
    pub(crate) fn gso_segments(self) -> Option<usize> {
        match self {
            Self::Ethernet => None,
            Self::Loopback => Some((GSO_BUFFER_LIMIT / u32::from(LOOPBACK_DATAGRAM)) as usize),
        }
    }

    /// Datagram sizing for a connection to `peer`.
    pub(crate) fn for_peer(peer: SocketAddr) -> Self {
        if peer.ip().is_loopback() {
            Self::Loopback
        } else {
            Self::Ethernet
        }
    }

    /// Datagram sizing for an endpoint listening on `listen`.
    ///
    /// A loopback listener can only be reached from this host, where large
    /// datagrams are safe. A wildcard or routable listener keeps the
    /// conservative default because its peers are unknown.
    pub(crate) fn for_listen(listen: SocketAddr) -> Self {
        Self::for_peer(listen)
    }
}

#[derive(Debug, Clone)]
pub struct IdentityPaths {
    pub dir: PathBuf,
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl IdentityPaths {
    pub fn user() -> Result<Self> {
        let base = BaseDirs::new().context("could not determine the user config directory")?;
        Ok(Self::in_dir(base.config_dir().join("cftp")))
    }

    pub fn in_dir(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            ca: dir.join("ca.pem"),
            cert: dir.join("identity.pem"),
            key: dir.join("identity.key"),
            dir,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdentityInfo {
    pub paths: IdentityPaths,
    pub ca_fingerprint: String,
    pub cert_fingerprint: String,
}

#[derive(Debug)]
pub struct Identity {
    pub certs: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub ca: CertificateDer<'static>,
    pub ca_fingerprint: String,
    pub cert_fingerprint: String,
}

impl Identity {
    pub fn load(cert_path: &Path, key_path: &Path, ca_path: &Path) -> Result<Self> {
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        let ca = load_first_cert(ca_path)?;
        let ca_fingerprint = fingerprint(ca.as_ref());
        let cert_fingerprint = fingerprint(certs[0].as_ref());
        Ok(Self {
            certs,
            key,
            ca,
            ca_fingerprint,
            cert_fingerprint,
        })
    }

    pub fn server_config(
        &self,
        tuning: &Tuning,
        force_v2: bool,
        mtu: Mtu,
    ) -> Result<quinn::ServerConfig> {
        let mut roots = RootCertStore::empty();
        roots
            .add(self.ca.clone())
            .context("failed to add client CA certificate")?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("failed to build client certificate verifier")?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_single_cert(self.certs.clone(), self.key.clone_key())?;
        config.alpn_protocols = alpn_protocols(force_v2);

        let quic = quinn::crypto::rustls::QuicServerConfig::try_from(config)?;
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(quic));
        server.transport_config(Arc::new(transport_config(tuning, mtu)));
        Ok(server)
    }

    pub fn client_config(
        &self,
        tuning: &Tuning,
        force_v2: bool,
        mtu: Mtu,
    ) -> Result<quinn::ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots
            .add(self.ca.clone())
            .context("failed to add server CA certificate")?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_client_auth_cert(self.certs.clone(), self.key.clone_key())?;
        config.alpn_protocols = alpn_protocols(force_v2);

        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(config)?;
        let mut client = quinn::ClientConfig::new(Arc::new(quic));
        client.transport_config(Arc::new(transport_config(tuning, mtu)));
        Ok(client)
    }
}

pub(crate) fn transport_config(tuning: &Tuning, mtu: Mtu) -> quinn::TransportConfig {
    let mut transport = quinn::TransportConfig::default();
    if mtu == Mtu::Loopback {
        transport.initial_mtu(mtu.initial_mtu());
        transport.min_mtu(mtu.initial_mtu());
        let mut discovery = quinn::MtuDiscoveryConfig::default();
        discovery.upper_bound(mtu.initial_mtu());
        transport.mtu_discovery_config(Some(discovery));
    }
    transport
        .max_concurrent_bidi_streams(quinn::VarInt::from_u32(tuning.max_concurrent_bi_streams));
    transport
        .max_concurrent_uni_streams(quinn::VarInt::from_u32(tuning.max_concurrent_uni_streams));
    transport.receive_window(
        quinn::VarInt::from_u64(tuning.receive_window).expect("valid receive window"),
    );
    transport.stream_receive_window(
        quinn::VarInt::from_u64(tuning.stream_receive_window).expect("valid stream receive window"),
    );
    transport.send_window(tuning.send_window);
    // Every path starts on CUBIC; the controller switches itself to BBR only
    // after measuring sustained loss (see `congestion.rs`).
    transport.congestion_controller_factory(Arc::new(AdaptiveCongestionConfig {
        initial_window: mtu.initial_window(),
    }));
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    transport.max_idle_timeout(Some(
        Duration::from_secs(60)
            .try_into()
            .expect("valid idle timeout"),
    ));
    transport
}

pub(crate) fn alpn_protocols(force_v2: bool) -> Vec<Vec<u8>> {
    if force_v2 {
        vec![cftp_proto::ALPN_V2.to_vec()]
    } else {
        vec![
            cftp_proto::ALPN_V4.to_vec(),
            cftp_proto::ALPN_V3.to_vec(),
            cftp_proto::ALPN_V2.to_vec(),
        ]
    }
}

pub fn init_identity(paths: &IdentityPaths, force: bool) -> Result<IdentityInfo> {
    if !force {
        for path in [&paths.ca, &paths.cert, &paths.key] {
            if path.exists() {
                bail!(
                    "identity file already exists: {} (use --force to replace it)",
                    path.display()
                );
            }
        }
    }

    fs::create_dir_all(&paths.dir)
        .with_context(|| format!("failed to create {}", paths.dir.display()))?;
    restrict_dir(&paths.dir)?;

    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "cftp local CA");
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];

    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca_params, ca_key);

    let mut params = CertificateParams::new(vec![DEFAULT_SERVER_NAME.to_string()])?;
    params
        .distinguished_name
        .push(DnType::CommonName, DEFAULT_SERVER_NAME);
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];

    let leaf_key = KeyPair::generate()?;
    let leaf_cert = params.signed_by(&leaf_key, &issuer)?;

    fs::write(&paths.ca, ca_cert.pem())
        .with_context(|| format!("failed to write {}", paths.ca.display()))?;
    fs::write(&paths.cert, leaf_cert.pem())
        .with_context(|| format!("failed to write {}", paths.cert.display()))?;
    fs::write(&paths.key, leaf_key.serialize_pem())
        .with_context(|| format!("failed to write {}", paths.key.display()))?;
    restrict_file(&paths.key)?;

    Ok(IdentityInfo {
        paths: paths.clone(),
        ca_fingerprint: fingerprint(ca_cert.der()),
        cert_fingerprint: fingerprint(leaf_cert.der()),
    })
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .with_context(|| format!("failed to open certificate {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse certificate {}", path.display()))?;
    if certs.is_empty() {
        bail!("no certificates found in {}", path.display());
    }
    Ok(certs)
}

fn load_first_cert(path: &Path) -> Result<CertificateDer<'static>> {
    let mut certs = load_certs(path)?;
    Ok(certs.remove(0))
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)
            .with_context(|| format!("failed to inspect private key {}", path.display()))?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            bail!(
                "private key {} is readable by other users (mode {mode:04o}); run `chmod 600 {}`",
                path.display(),
                path.display()
            );
        }
    }
    PrivateKeyDer::from_pem_file(path)
        .with_context(|| format!("failed to parse private key {}", path.display()))
}

pub fn fingerprint(der: &[u8]) -> String {
    let digest = Sha256::digest(der);
    hex::encode(digest)
}

#[cfg(unix)]
fn restrict_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_peers_use_large_datagrams() {
        assert_eq!(
            Mtu::for_peer("127.0.0.1:4433".parse().unwrap()),
            Mtu::Loopback
        );
        assert_eq!(Mtu::for_peer("[::1]:4433".parse().unwrap()), Mtu::Loopback);
        assert_eq!(
            Mtu::for_peer("192.168.1.10:4433".parse().unwrap()),
            Mtu::Ethernet
        );
        assert_eq!(
            Mtu::for_listen("0.0.0.0:4433".parse().unwrap()),
            Mtu::Ethernet
        );
        assert_eq!(
            Mtu::for_listen("127.0.0.1:4433".parse().unwrap()),
            Mtu::Loopback
        );
    }

    #[test]
    fn large_datagrams_stay_inside_the_gso_limit() {
        // Linux rejects a UDP GSO buffer above 64 KiB, so the batch size has
        // to shrink as the datagram grows.
        let segments = Mtu::Loopback
            .gso_segments()
            .expect("loopback caps its batch");
        assert!(segments > 1);
        assert!(
            u32::from(Mtu::Loopback.max_udp_payload_size()) * segments as u32 <= GSO_BUFFER_LIMIT
        );
        assert!(Mtu::Loopback.max_udp_payload_size() >= 1_200);
    }
}
