use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::verify_tls13_signature_with_raw_key;
use rustls::pki_types::{CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::CertifiedKey;
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};

use crate::identity::{Identity, transport_config};
use crate::ssh::{SshPrivateKey, SshPublicKey};
use crate::trust::{AuthorizedKeys, HostKeyStatus, KnownHostsStore, public_key_from_spki_checked};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKeyPolicy {
    Ask,
    AcceptNew,
    Strict,
}

#[derive(Debug, Clone)]
pub struct UnknownHostKey {
    pub host: String,
    pub port: u16,
    pub fingerprint: String,
    pub public_key: SshPublicKey,
}

impl std::fmt::Display for UnknownHostKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the authenticity of host '{}:{}' can't be established; key fingerprint is {}",
            self.host, self.port, self.fingerprint
        )
    }
}

impl std::error::Error for UnknownHostKey {}

pub struct SshServerAuth {
    host_key: SshPrivateKey,
    authorized_keys: Arc<AuthorizedKeys>,
}

impl SshServerAuth {
    pub fn new(host_key: SshPrivateKey, authorized_keys: AuthorizedKeys) -> Self {
        Self {
            host_key,
            authorized_keys: Arc::new(authorized_keys),
        }
    }

    pub fn host_key(&self) -> &SshPrivateKey {
        &self.host_key
    }

    pub fn authorized_keys(&self) -> &AuthorizedKeys {
        &self.authorized_keys
    }
}

pub struct SshClientAuth {
    key: SshPrivateKey,
    known_hosts: Arc<Mutex<KnownHostsStore>>,
    host: String,
    port: u16,
    policy: HostKeyPolicy,
    unknown: Arc<Mutex<Option<UnknownHostKey>>>,
}

impl SshClientAuth {
    pub fn new(
        key: SshPrivateKey,
        known_hosts: KnownHostsStore,
        host: impl Into<String>,
        port: u16,
        policy: HostKeyPolicy,
    ) -> Self {
        Self {
            key,
            known_hosts: Arc::new(Mutex::new(known_hosts)),
            host: host.into(),
            port,
            policy,
            unknown: Arc::new(Mutex::new(None)),
        }
    }

    pub fn key(&self) -> &SshPrivateKey {
        &self.key
    }

    pub fn take_unknown_host_key(&self) -> Option<UnknownHostKey> {
        self.unknown
            .lock()
            .expect("unknown host key lock poisoned")
            .take()
    }

    pub fn accept_host_key(&self, public_key: &SshPublicKey) -> Result<()> {
        self.known_hosts
            .lock()
            .expect("known hosts lock poisoned")
            .append(&self.host, self.port, public_key)
    }

    pub fn known_hosts_path(&self) -> std::path::PathBuf {
        self.known_hosts
            .lock()
            .expect("known hosts lock poisoned")
            .path()
            .to_path_buf()
    }
}

pub enum ServerAuth {
    Cert(Identity),
    Ssh(SshServerAuth),
}

impl ServerAuth {
    pub fn server_config(&self) -> Result<quinn::ServerConfig> {
        match self {
            Self::Cert(identity) => identity.server_config(),
            Self::Ssh(auth) => ssh_server_config(auth),
        }
    }
}

pub enum ClientAuth {
    Cert(Identity),
    Ssh(SshClientAuth),
}

impl ClientAuth {
    pub fn client_config(&self) -> Result<quinn::ClientConfig> {
        match self {
            Self::Cert(identity) => identity.client_config(),
            Self::Ssh(auth) => ssh_client_config(auth),
        }
    }

    pub fn take_unknown_host_key(&self) -> Option<UnknownHostKey> {
        match self {
            Self::Ssh(auth) => auth.take_unknown_host_key(),
            Self::Cert(_) => None,
        }
    }
}

fn raw_public_key(key: &SshPrivateKey) -> Result<Arc<CertifiedKey>> {
    let provider = rustls::crypto::ring::default_provider();
    let signing = provider
        .key_provider
        .load_private_key(key.pkcs8())
        .context("failed to load SSH private key for TLS")?;
    Ok(Arc::new(CertifiedKey::new(
        vec![CertificateDer::from(key.spki().to_vec())],
        signing,
    )))
}

fn ssh_server_config(auth: &SshServerAuth) -> Result<quinn::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = Arc::new(SshClientCertVerifier {
        authorized_keys: auth.authorized_keys.clone(),
        provider: provider.clone(),
    });
    let certified = raw_public_key(&auth.host_key)?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(Arc::new(
            rustls::server::AlwaysResolvesServerRawPublicKeys::new(certified),
        ));
    config.alpn_protocols = vec![cftp_proto::ALPN.to_vec()];

    let quic = quinn::crypto::rustls::QuicServerConfig::try_from(config)?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(quic));
    server.transport_config(Arc::new(transport_config()));
    Ok(server)
}

fn ssh_client_config(auth: &SshClientAuth) -> Result<quinn::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = Arc::new(SshServerCertVerifier {
        known_hosts: auth.known_hosts.clone(),
        host: auth.host.clone(),
        port: auth.port,
        policy: auth.policy,
        unknown: auth.unknown.clone(),
        provider: provider.clone(),
    });
    let certified = raw_public_key(&auth.key)?;
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(Arc::new(
            rustls::client::AlwaysResolvesClientRawPublicKeys::new(certified),
        ));
    config.alpn_protocols = vec![cftp_proto::ALPN.to_vec()];

    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(config)?;
    let mut client = quinn::ClientConfig::new(Arc::new(quic));
    client.transport_config(Arc::new(transport_config()));
    Ok(client)
}

struct SshClientCertVerifier {
    authorized_keys: Arc<AuthorizedKeys>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl std::fmt::Debug for SshClientCertVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshClientCertVerifier")
            .field(
                "authorized_keys",
                &self.authorized_keys.fingerprints().count(),
            )
            .finish_non_exhaustive()
    }
}

impl ClientCertVerifier for SshClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        if self.authorized_keys.accepts(end_entity.as_ref()) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "client SSH key is not listed in authorized_keys".to_string(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

struct SshServerCertVerifier {
    known_hosts: Arc<Mutex<KnownHostsStore>>,
    host: String,
    port: u16,
    policy: HostKeyPolicy,
    unknown: Arc<Mutex<Option<UnknownHostKey>>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl std::fmt::Debug for SshServerCertVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshServerCertVerifier")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for SshServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let spki = end_entity.as_ref();
        let status = self
            .known_hosts
            .lock()
            .expect("known hosts lock poisoned")
            .status(&self.host, self.port, spki);

        match status {
            HostKeyStatus::Trusted => Ok(ServerCertVerified::assertion()),
            HostKeyStatus::Revoked { fingerprints } => Err(rustls::Error::General(format!(
                "SSH host key for {}:{} is revoked in known_hosts ({})",
                self.host,
                self.port,
                fingerprints.join(", ")
            ))),
            HostKeyStatus::Mismatch { fingerprints } => Err(rustls::Error::General(format!(
                "REMOTE HOST IDENTIFICATION HAS CHANGED for {}:{}; known_hosts has {}",
                self.host,
                self.port,
                fingerprints.join(", ")
            ))),
            HostKeyStatus::Unknown => match self.policy {
                HostKeyPolicy::AcceptNew => {
                    let public = public_key_from_spki_checked(spki)
                        .map_err(|err| rustls::Error::General(err.to_string()))?;
                    self.known_hosts
                        .lock()
                        .expect("known hosts lock poisoned")
                        .append(&self.host, self.port, &public)
                        .map_err(|err| rustls::Error::General(err.to_string()))?;
                    Ok(ServerCertVerified::assertion())
                }
                HostKeyPolicy::Strict => Err(rustls::Error::General(format!(
                    "SSH host key for {}:{} is not present in known_hosts",
                    self.host, self.port
                ))),
                HostKeyPolicy::Ask => {
                    let public = public_key_from_spki_checked(spki)
                        .map_err(|err| rustls::Error::General(err.to_string()))?;
                    let fingerprint = public.fingerprint().to_string();
                    *self.unknown.lock().expect("unknown host key lock poisoned") =
                        Some(UnknownHostKey {
                            host: self.host.clone(),
                            port: self.port,
                            fingerprint,
                            public_key: public,
                        });
                    Err(rustls::Error::General(format!(
                        "unknown SSH host key for {}:{}; confirm it before connecting",
                        self.host, self.port
                    )))
                }
            },
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

pub fn load_server_auth(
    host_key: &std::path::Path,
    authorized_keys: &std::path::Path,
    passphrase: impl FnMut(&std::path::Path) -> Result<String>,
) -> Result<ServerAuth> {
    if !host_key.is_file() {
        bail!(
            "no SSH host key found at {}; create one with `ssh-keygen -t ed25519` or pass --host-key",
            host_key.display()
        );
    }
    let key = crate::ssh::load_private_key(host_key, passphrase)?;
    let authorized = AuthorizedKeys::load(authorized_keys)?;
    Ok(ServerAuth::Ssh(SshServerAuth::new(key, authorized)))
}

pub fn load_client_auth(
    key_path: &std::path::Path,
    known_hosts: KnownHostsStore,
    host: impl Into<String>,
    port: u16,
    policy: HostKeyPolicy,
    passphrase: impl FnMut(&std::path::Path) -> Result<String>,
) -> Result<ClientAuth> {
    let key = crate::ssh::load_private_key(key_path, passphrase)?;
    Ok(ClientAuth::Ssh(SshClientAuth::new(
        key,
        known_hosts,
        host,
        port,
        policy,
    )))
}
