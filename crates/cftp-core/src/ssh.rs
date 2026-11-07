use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use directories::BaseDirs;
use rsa::traits::PublicKeyParts;
use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use ssh_key::HashAlg;
use ssh_key::private::{EcdsaKeypair, KeypairData};
use ssh_key::public::{EcdsaPublicKey, KeyData};

#[derive(Debug, Clone)]
pub struct SshPaths {
    /// cftp's own configuration directory (`~/.config/cftp`): host key and
    /// known_hosts live here so they never collide with OpenSSH's files.
    pub dir: PathBuf,
    /// The user's OpenSSH directory (`~/.ssh`): client keys and authorized_keys.
    pub ssh_dir: PathBuf,
    pub host_key: PathBuf,
    pub authorized_keys: PathBuf,
    pub known_hosts: PathBuf,
}

impl SshPaths {
    pub fn user() -> Result<Self> {
        let base = BaseDirs::new().context("could not determine the home directory")?;
        let dir: PathBuf = base.config_dir().join("cftp");
        let ssh_dir: PathBuf = base.home_dir().join(".ssh");
        Ok(Self {
            host_key: dir.join("host_key"),
            authorized_keys: ssh_dir.join("authorized_keys"),
            known_hosts: dir.join("known_hosts"),
            dir,
            ssh_dir,
        })
    }

    /// Places every file under one directory. Useful for tests and embedded use.
    pub fn in_dir(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            host_key: dir.join("host_key"),
            authorized_keys: dir.join("authorized_keys"),
            known_hosts: dir.join("known_hosts"),
            ssh_dir: dir.clone(),
            dir,
        }
    }

    pub fn client_key_candidates(&self) -> [PathBuf; 3] {
        [
            self.ssh_dir.join("id_ed25519"),
            self.ssh_dir.join("id_ecdsa"),
            self.ssh_dir.join("id_rsa"),
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

    /// Parses an OpenSSH public key line (`ssh-ed25519 AAAA... comment`).
    pub fn from_openssh(line: &str) -> Result<Self> {
        let public = ssh_key::PublicKey::from_openssh(line.trim())
            .context("failed to parse an OpenSSH public key")?;
        Self::from_ssh(public)
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

/// Generates a fresh, unencrypted Ed25519 key.
pub fn generate_keypair() -> Result<ssh_key::PrivateKey> {
    ssh_key::PrivateKey::random(&mut ssh_key::rand_core::OsRng, ssh_key::Algorithm::Ed25519)
        .context("failed to generate an Ed25519 key")
}

/// Writes an OpenSSH private key with owner-only permissions.
///
/// Refuses to replace an existing file and creates a missing parent directory
/// (with mode 0700 on Unix) so a first run can populate `~/.config/cftp`.
pub fn write_private_key(path: &Path, private: &ssh_key::PrivateKey) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        restrict_dir(parent)?;
    }
    let pem = private
        .to_openssh(ssh_key::LineEnding::LF)
        .context("failed to encode SSH private key")?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("failed to create SSH private key {}", path.display()))?;
    use std::io::Write as _;
    file.write_all(pem.as_bytes())
        .with_context(|| format!("failed to write SSH private key {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to flush SSH private key {}", path.display()))?;
    Ok(())
}

/// Loads the host key at `path`, creating a new Ed25519 key on first use.
///
/// Returns the key and whether it was generated, so callers can tell the user
/// where the new identity lives.
pub fn load_or_create_host_key<F>(path: &Path, passphrase: F) -> Result<(SshPrivateKey, bool)>
where
    F: FnMut(&Path) -> Result<String>,
{
    if path.is_file() {
        return Ok((load_private_key(path, passphrase)?, false));
    }
    let private = generate_keypair()?;
    write_private_key(path, &private)?;
    Ok((from_private_key(private)?, true))
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
            ed25519_dalek::pkcs8::EncodePrivateKey::to_pkcs8_der(&signing)
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
            rsa::pkcs8::EncodePrivateKey::to_pkcs8_der(&rsa)
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
            Ok(p256::pkcs8::EncodePrivateKey::to_pkcs8_der(&secret)
                .context("failed to encode ECDSA P-256 key as PKCS#8")?
                .as_bytes()
                .to_vec())
        }
        EcdsaKeypair::NistP384 { .. } => {
            let secret = p384::SecretKey::from_slice(key.private_key_bytes())
                .context("failed to convert ECDSA P-384 private key")?;
            Ok(p384::pkcs8::EncodePrivateKey::to_pkcs8_der(&secret)
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
            ed25519_dalek::pkcs8::EncodePublicKey::to_public_key_der(&verifying)
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
            rsa::pkcs8::EncodePublicKey::to_public_key_der(&rsa)
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
            Ok(p256::pkcs8::EncodePublicKey::to_public_key_der(&public)
                .context("failed to encode ECDSA P-256 public key as SPKI")?
                .as_bytes()
                .to_vec())
        }
        EcdsaPublicKey::NistP384(_) => {
            let public = p384::PublicKey::from_sec1_bytes(key.as_sec1_bytes())
                .context("failed to convert ECDSA P-384 public key")?;
            Ok(p384::pkcs8::EncodePublicKey::to_public_key_der(&public)
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

#[cfg(unix)]
fn restrict_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to restrict permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_dir(_path: &Path) -> Result<()> {
    Ok(())
}
