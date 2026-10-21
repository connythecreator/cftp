use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use directories::BaseDirs;
use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey};
use rsa::traits::PublicKeyParts;
use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use ssh_key::HashAlg;
use ssh_key::private::{EcdsaKeypair, KeypairData};
use ssh_key::public::{EcdsaPublicKey, KeyData};

#[derive(Debug, Clone)]
pub struct SshPaths {
    pub dir: PathBuf,
    pub host_key: PathBuf,
    pub authorized_keys: PathBuf,
    pub known_hosts: PathBuf,
}

impl SshPaths {
    pub fn user() -> Result<Self> {
        let base = BaseDirs::new().context("could not determine the home directory")?;
        Ok(Self::in_dir(base.home_dir().join(".ssh")))
    }

    pub fn in_dir(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            host_key: dir.join("id_ed25519"),
            authorized_keys: dir.join("authorized_keys"),
            known_hosts: dir.join("known_hosts"),
            dir,
        }
    }

    pub fn client_key_candidates(&self) -> [PathBuf; 3] {
        [
            self.dir.join("id_ed25519"),
            self.dir.join("id_ecdsa"),
            self.dir.join("id_rsa"),
        ]
    }

    pub fn existing_client_key(&self) -> Option<PathBuf> {
        self.client_key_candidates()
            .into_iter()
            .find(|path| path.is_file())
    }
}

pub struct SshPrivateKey {
    pub(crate) public: ssh_key::PublicKey,
    pub(crate) spki: Vec<u8>,
    pub(crate) pkcs8: PrivateKeyDer<'static>,
    pub(crate) fingerprint: String,
}

impl SshPrivateKey {
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn public_openssh(&self) -> Result<String> {
        self.public
            .to_openssh()
            .context("failed to encode SSH public key")
    }

    pub fn spki(&self) -> &[u8] {
        &self.spki
    }

    pub fn pkcs8(&self) -> PrivateKeyDer<'static> {
        self.pkcs8.clone_key()
    }
}

impl std::fmt::Debug for SshPrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshPrivateKey")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct SshPublicKey {
    pub(crate) public: ssh_key::PublicKey,
    pub(crate) spki: Vec<u8>,
    pub(crate) fingerprint: String,
}

impl SshPublicKey {
    pub fn from_ssh(public: ssh_key::PublicKey) -> Result<Self> {
        let spki = public_key_to_spki(&public)?;
        let fingerprint = public.fingerprint(HashAlg::Sha256).to_string();
        Ok(Self {
            public,
            spki,
            fingerprint,
        })
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn spki(&self) -> &[u8] {
        &self.spki
    }

    pub fn public_openssh(&self) -> Result<String> {
        self.public
            .to_openssh()
            .context("failed to encode SSH public key")
    }
}

impl std::fmt::Debug for SshPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshPublicKey")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

pub fn read_private_key(path: &Path) -> Result<ssh_key::PrivateKey> {
    check_private_key_permissions(path)?;
    ssh_key::PrivateKey::read_openssh_file(path)
        .with_context(|| format!("failed to read SSH private key {}", path.display()))
}

pub fn load_private_key<F>(path: &Path, mut passphrase: F) -> Result<SshPrivateKey>
where
    F: FnMut(&Path) -> Result<String>,
{
    let mut private = read_private_key(path)?;
    if private.is_encrypted() {
        let password = passphrase(path)?;
        private = private.decrypt(password.as_bytes()).with_context(|| {
            format!(
                "failed to decrypt SSH private key {} (wrong passphrase?)",
                path.display()
            )
        })?;
    }
    from_private_key(private)
}

pub fn from_private_key(private: ssh_key::PrivateKey) -> Result<SshPrivateKey> {
    let public = private.public_key().clone();
    let spki = public_key_to_spki(&public)?;
    let fingerprint = public.fingerprint(HashAlg::Sha256).to_string();
    let pkcs8 = private_key_to_pkcs8(private.key_data())?;
    Ok(SshPrivateKey {
        public,
        spki,
        pkcs8,
        fingerprint,
    })
}

fn private_key_to_pkcs8(data: &KeypairData) -> Result<PrivateKeyDer<'static>> {
    let der = match data {
        KeypairData::Ed25519(key) => {
            let signing = ed25519_dalek::SigningKey::from(&key.private);
            signing
                .to_pkcs8_der()
                .context("failed to encode Ed25519 key as PKCS#8")?
                .as_bytes()
                .to_vec()
        }
        KeypairData::Ecdsa(key) => ecdsa_private_to_pkcs8(key)?,
        KeypairData::Rsa(key) => {
            let n =
                rsa::BigUint::try_from(&key.public.n).context("failed to convert RSA modulus")?;
            let e =
                rsa::BigUint::try_from(&key.public.e).context("failed to convert RSA exponent")?;
            let d = rsa::BigUint::try_from(&key.private.d)
                .context("failed to convert RSA private exponent")?;
            let p =
                rsa::BigUint::try_from(&key.private.p).context("failed to convert RSA prime p")?;
            let q =
                rsa::BigUint::try_from(&key.private.q).context("failed to convert RSA prime q")?;
            let mut rsa = rsa::RsaPrivateKey::from_components(n, e, d, vec![p, q])
                .context("failed to convert RSA private key")?;
            if rsa.size().saturating_mul(8) < 2048 {
                bail!("RSA keys smaller than 2048 bits are not supported");
            }
            rsa.precompute()
                .context("failed to precompute RSA private key")?;
            rsa.to_pkcs8_der()
                .context("failed to encode RSA key as PKCS#8")?
                .as_bytes()
                .to_vec()
        }
        _ => bail!(
            "unsupported SSH key type {}; supported types are Ed25519, ECDSA P-256/P-384, and RSA",
            data.algorithm()
                .map(|algorithm| algorithm.to_string())
                .unwrap_or_else(|_| "unknown".to_string())
        ),
    };
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der)))
}

fn ecdsa_private_to_pkcs8(key: &EcdsaKeypair) -> Result<Vec<u8>> {
    match key {
        EcdsaKeypair::NistP256 { .. } => {
            let secret = p256::SecretKey::from_slice(key.private_key_bytes())
                .context("failed to convert ECDSA P-256 private key")?;
            Ok(secret
                .to_pkcs8_der()
                .context("failed to encode ECDSA P-256 key as PKCS#8")?
                .as_bytes()
                .to_vec())
        }
        EcdsaKeypair::NistP384 { .. } => {
            let secret = p384::SecretKey::from_slice(key.private_key_bytes())
                .context("failed to convert ECDSA P-384 private key")?;
            Ok(secret
                .to_pkcs8_der()
                .context("failed to encode ECDSA P-384 key as PKCS#8")?
                .as_bytes()
                .to_vec())
        }
        EcdsaKeypair::NistP521 { .. } => {
            bail!("ECDSA P-521 keys are not supported; use P-256, P-384, Ed25519, or RSA")
        }
    }
}

pub fn public_key_to_spki(public: &ssh_key::PublicKey) -> Result<Vec<u8>> {
    let der = match public.key_data() {
        KeyData::Ed25519(key) => {
            let verifying = ed25519_dalek::VerifyingKey::from_bytes(&key.0)
                .context("failed to convert Ed25519 public key")?;
            verifying
                .to_public_key_der()
                .context("failed to encode Ed25519 public key as SPKI")?
                .as_bytes()
                .to_vec()
        }
        KeyData::Ecdsa(key) => ecdsa_public_to_spki(key)?,
        KeyData::Rsa(key) => {
            let rsa =
                rsa::RsaPublicKey::try_from(key).context("failed to convert RSA public key")?;
            if rsa.size().saturating_mul(8) < 2048 {
                bail!("RSA keys smaller than 2048 bits are not supported");
            }
            rsa.to_public_key_der()
                .context("failed to encode RSA public key as SPKI")?
                .as_bytes()
                .to_vec()
        }
        _ => bail!(
            "unsupported SSH key type {}; supported types are Ed25519, ECDSA P-256/P-384, and RSA",
            public.algorithm()
        ),
    };
    Ok(der)
}

fn ecdsa_public_to_spki(key: &EcdsaPublicKey) -> Result<Vec<u8>> {
    match key {
        EcdsaPublicKey::NistP256(_) => {
            let public = p256::PublicKey::from_sec1_bytes(key.as_sec1_bytes())
                .context("failed to convert ECDSA P-256 public key")?;
            Ok(public
                .to_public_key_der()
                .context("failed to encode ECDSA P-256 public key as SPKI")?
                .as_bytes()
                .to_vec())
        }
        EcdsaPublicKey::NistP384(_) => {
            let public = p384::PublicKey::from_sec1_bytes(key.as_sec1_bytes())
                .context("failed to convert ECDSA P-384 public key")?;
            Ok(public
                .to_public_key_der()
                .context("failed to encode ECDSA P-384 public key as SPKI")?
                .as_bytes()
                .to_vec())
        }
        EcdsaPublicKey::NistP521(_) => {
            bail!("ECDSA P-521 keys are not supported; use P-256, P-384, Ed25519, or RSA")
        }
    }
}

#[cfg(unix)]
fn check_private_key_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .with_context(|| format!("failed to inspect SSH private key {}", path.display()))?
        .permissions()
        .mode()
        & 0o777;
    if mode & 0o077 != 0 {
        bail!(
            "SSH private key {} has too-open permissions ({mode:04o}); run `chmod 600 {}`",
            path.display(),
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private_key_permissions(_path: &Path) -> Result<()> {
    Ok(())
}
