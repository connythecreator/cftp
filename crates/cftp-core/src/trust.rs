use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hmac::{Hmac, Mac};
use rsa::pkcs8::DecodePublicKey;
use rsa::traits::PublicKeyParts;
use sha1::Sha1;
use ssh_key::known_hosts::{HostPatterns, KnownHosts, Marker};
use ssh_key::public::{EcdsaPublicKey, Ed25519PublicKey, KeyData, RsaPublicKey};
use ssh_key::{Mpint, PublicKey};

use crate::ssh::{SshPublicKey, public_key_to_spki};

type HmacSha1 = Hmac<Sha1>;

pub const DEFAULT_KH_PORT: u16 = 4433;

#[derive(Debug)]
pub struct AuthorizedKeys {
    keys: Vec<SshPublicKey>,
}

impl AuthorizedKeys {
    pub fn load(path: &Path) -> Result<Self> {
        let input = fs::read_to_string(path).with_context(|| {
            format!(
                "failed to read authorized keys {}; add the client's public key to this file",
                path.display()
            )
        })?;

        let mut keys = Vec::new();
        for entry in ssh_key::authorized_keys::AuthorizedKeys::new(&input) {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "skipping invalid authorized_keys entry");
                    continue;
                }
            };

            let options = entry.config_opts();
            if let Some(option) = unsupported_authorized_key_option(options.iter()) {
                tracing::warn!(
                    path = %path.display(),
                    fingerprint = %entry.public_key().fingerprint(ssh_key::HashAlg::Sha256),
                    option,
                    "ignoring authorized_keys entry with an option cftp cannot enforce"
                );
                continue;
            }

            match SshPublicKey::from_ssh(entry.public_key().clone()) {
                Ok(key) => keys.push(key),
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        fingerprint = %entry.public_key().fingerprint(ssh_key::HashAlg::Sha256),
                        error = %err,
                        "ignoring unsupported authorized_keys entry"
                    );
                }
            }
        }

        if keys.is_empty() {
            bail!(
                "no usable SSH public keys found in {}; add the client's public key to this file",
                path.display()
            );
        }

        Ok(Self { keys })
    }

    pub fn accepts(&self, spki: &[u8]) -> bool {
        self.keys.iter().any(|key| key.spki() == spki)
    }

    pub fn fingerprints(&self) -> impl Iterator<Item = &str> {
        self.keys.iter().map(SshPublicKey::fingerprint)
    }
}

fn unsupported_authorized_key_option<'a>(
    options: impl Iterator<Item = &'a str>,
) -> Option<&'a str> {
    options.into_iter().find(|option| {
        let name = option
            .split_once('=')
            .map(|(name, _)| name)
            .unwrap_or(option)
            .trim()
            .to_ascii_lowercase();
        matches!(
            name.as_str(),
            "command"
                | "from"
                | "environment"
                | "expiry-time"
                | "permitopen"
                | "permitlisten"
                | "principals"
                | "tunnel"
        )
    })
}

#[derive(Clone, Debug)]
struct KnownHostEntry {
    marker: Option<Marker>,
    patterns: HostPatterns,
    public: SshPublicKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostKeyStatus {
    Trusted,
    Revoked { fingerprints: Vec<String> },
    Mismatch { fingerprints: Vec<String> },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct KnownHostsStore {
    path: PathBuf,
    entries: Vec<KnownHostEntry>,
}

impl KnownHostsStore {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.exists() {
            return Ok(Self {
                path,
                entries: Vec::new(),
            });
        }

        let input = fs::read_to_string(&path)
            .with_context(|| format!("failed to read known hosts {}", path.display()))?;
        let mut entries = Vec::new();
        for entry in KnownHosts::new(&input) {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "skipping invalid known_hosts entry");
                    continue;
                }
            };

            if matches!(entry.marker(), Some(Marker::CertAuthority)) {
                tracing::warn!(
                    path = %path.display(),
                    "ignoring @cert-authority known_hosts entry; SSH certificate authentication is not supported"
                );
                continue;
            }

            match SshPublicKey::from_ssh(entry.public_key().clone()) {
                Ok(public) => entries.push(KnownHostEntry {
                    marker: entry.marker().cloned(),
                    patterns: entry.host_patterns().clone(),
                    public,
                }),
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        fingerprint = %entry.public_key().fingerprint(ssh_key::HashAlg::Sha256),
                        error = %err,
                        "ignoring unsupported known_hosts entry"
                    );
                }
            }
        }

        Ok(Self { path, entries })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn status(&self, host: &str, port: u16, presented_spki: &[u8]) -> HostKeyStatus {
        let mut known_fingerprints = Vec::new();
        let mut revoked_fingerprints = Vec::new();
        let mut trusted = false;
        let mut matched = false;

        for entry in &self.entries {
            if !host_matches(&entry.patterns, host, port) {
                continue;
            }
            matched = true;
            let fingerprint = entry.public.fingerprint().to_string();
            if entry.public.spki() == presented_spki {
                if matches!(entry.marker, Some(Marker::Revoked)) {
                    revoked_fingerprints.push(fingerprint);
                } else {
                    trusted = true;
                }
            } else {
                known_fingerprints.push(fingerprint);
            }
        }

        if !revoked_fingerprints.is_empty() {
            return HostKeyStatus::Revoked {
                fingerprints: revoked_fingerprints,
            };
        }
        if trusted {
            return HostKeyStatus::Trusted;
        }
        if matched && !known_fingerprints.is_empty() {
            return HostKeyStatus::Mismatch {
                fingerprints: known_fingerprints,
            };
        }
        HostKeyStatus::Unknown
    }

    pub fn append(&mut self, host: &str, port: u16, public: &SshPublicKey) -> Result<()> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
            restrict_dir(parent)?;
        }

        let hostname = canonical_host(host, port);
        let line = format!("{hostname} {}\n", public.public_openssh()?);
        let existed = self.path.exists();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("failed to open known hosts {}", self.path.display()))?;
        file.write_all(line.as_bytes())
            .with_context(|| format!("failed to append to {}", self.path.display()))?;
        if !existed {
            restrict_file(&self.path)?;
        }

        self.entries.push(KnownHostEntry {
            marker: None,
            patterns: HostPatterns::Patterns(vec![hostname]),
            public: public.clone(),
        });
        Ok(())
    }
}

pub fn canonical_host(host: &str, port: u16) -> String {
    if port == DEFAULT_KH_PORT {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    }
}

fn host_matches(patterns: &HostPatterns, host: &str, port: u16) -> bool {
    match patterns {
        HostPatterns::Patterns(patterns) => {
            let mut positive = false;
            for pattern in patterns {
                if let Some(negated) = pattern.strip_prefix('!') {
                    if pattern_matches_host(negated, host, port) {
                        return false;
                    }
                } else if pattern_matches_host(pattern, host, port) {
                    positive = true;
                }
            }
            positive
        }
        HostPatterns::HashedName { salt, hash } => {
            let mut mac = match HmacSha1::new_from_slice(salt) {
                Ok(mac) => mac,
                Err(_) => return false,
            };
            mac.update(canonical_host(host, port).as_bytes());
            if mac.verify_slice(hash).is_ok() {
                return true;
            }
            let mut mac = HmacSha1::new_from_slice(salt).expect("HMAC accepts any key size");
            mac.update(host.as_bytes());
            mac.verify_slice(hash).is_ok()
        }
    }
}

fn pattern_matches_host(pattern: &str, host: &str, port: u16) -> bool {
    if let Some(rest) = pattern.strip_prefix('[') {
        let Some((pattern_host, pattern_port)) = rest.split_once("]:") else {
            return false;
        };
        let Ok(pattern_port) = pattern_port.parse::<u16>() else {
            return false;
        };
        return pattern_port == port && glob_match(pattern_host, host);
    }

    port == DEFAULT_KH_PORT && glob_match(pattern, host)
}

fn glob_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let mut p = 0;
    let mut v = 0;
    let mut star = None;
    let mut star_value = 0;

    while v < value.len() {
        if p < pattern.len() && (pattern[p] == b'?' || eq_ignore_ascii_case(pattern[p], value[v])) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            star_value = v;
            p += 1;
        } else if let Some(star_index) = star {
            p = star_index + 1;
            star_value += 1;
            v = star_value;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

fn eq_ignore_ascii_case(a: u8, b: u8) -> bool {
    a.eq_ignore_ascii_case(&b)
}

pub fn public_key_from_spki(spki: &[u8]) -> Result<ssh_key::PublicKey> {
    let key_data = if let Ok(key) = ed25519_dalek::VerifyingKey::from_public_key_der(spki) {
        KeyData::Ed25519(Ed25519PublicKey(key.to_bytes()))
    } else if let Ok(key) = p256::PublicKey::from_public_key_der(spki) {
        let key = EcdsaPublicKey::from_sec1_bytes(key.to_sec1_bytes().as_ref())
            .context("failed to convert P-256 SPKI to an SSH key")?;
        KeyData::Ecdsa(key)
    } else if let Ok(key) = p384::PublicKey::from_public_key_der(spki) {
        let key = EcdsaPublicKey::from_sec1_bytes(key.to_sec1_bytes().as_ref())
            .context("failed to convert P-384 SPKI to an SSH key")?;
        KeyData::Ecdsa(key)
    } else if let Ok(key) = rsa::RsaPublicKey::from_public_key_der(spki) {
        let n = Mpint::from_positive_bytes(&key.n().to_bytes_be())
            .context("failed to convert RSA modulus")?;
        let e = Mpint::from_positive_bytes(&key.e().to_bytes_be())
            .context("failed to convert RSA exponent")?;
        KeyData::Rsa(RsaPublicKey { e, n })
    } else {
        bail!("unsupported peer public key");
    };

    Ok(PublicKey::new(key_data, ""))
}

pub fn public_key_from_spki_checked(spki: &[u8]) -> Result<SshPublicKey> {
    let public = public_key_from_spki(spki)?;
    let expected = public_key_to_spki(&public)?;
    if expected != spki {
        bail!("peer public key does not match its SubjectPublicKeyInfo encoding");
    }
    SshPublicKey::from_ssh(public)
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
