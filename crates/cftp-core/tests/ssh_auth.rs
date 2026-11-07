use std::fs;

use cftp_core::ssh::{SshPublicKey, from_private_key, public_key_to_spki};
use cftp_core::trust::{
    AuthorizedKeys, HostKeyStatus, KnownHostsStore, canonical_host, public_key_from_spki_checked,
};
use ssh_key::rand_core::OsRng;
use tempfile::tempdir;

fn generate(algorithm: ssh_key::Algorithm) -> ssh_key::PrivateKey {
    ssh_key::PrivateKey::random(&mut OsRng, algorithm).unwrap()
}

/// A throwaway ECDSA P-256 key embedded only so tests can exercise parsing.
/// `ssh-key` cannot generate ECDSA keys without the `p256` feature, which pulls
/// the advisory-affected `crypto-bigint 0.5`; parsing works with `ecdsa` alone.
const ECDSA_FIXTURE: &str = "\
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS
1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQTsc7f7GpqAFGlVJKRScg4q6XPwQrkB
CL/WWn+S4eJkczp4pL18hJC1+7uOaVl0/jnDRC48JiM7UzWjGkJt0pF4AAAAsLu0LEW7tC
xFAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBOxzt/samoAUaVUk
pFJyDirpc/BCuQEIv9Zaf5Lh4mRzOnikvXyEkLX7u45pWXT+OcNELjwmIztTNaMaQm3SkX
gAAAAgKMPgW30dX7weZHjAig6EPbHYGXha0FhaaX8c//OD4GEAAAARY2Z0cC10ZXN0LWZp
eHR1cmUBAgMEBQYH
-----END OPENSSH PRIVATE KEY-----
";

fn fixture_ecdsa() -> ssh_key::PrivateKey {
    ssh_key::PrivateKey::from_openssh(ECDSA_FIXTURE).unwrap()
}

#[test]
fn converts_common_ssh_key_types_to_spki() {
    let ed25519 = generate(ssh_key::Algorithm::Ed25519);
    let ecdsa = fixture_ecdsa();
    let rsa = generate(ssh_key::Algorithm::Rsa { hash: None });

    for private in [ed25519, ecdsa, rsa] {
        let key = from_private_key(private).unwrap();
        let public = public_key_from_spki_checked(key.spki()).unwrap();
        assert_eq!(public.spki(), key.spki());
        assert!(key.fingerprint().starts_with("SHA256:"));
    }
}

#[test]
fn authorized_keys_rejects_unenforceable_options() {
    let temp = tempdir().unwrap();
    let private = generate(ssh_key::Algorithm::Ed25519);
    let public = private.public_key().to_openssh().unwrap();
    let path = temp.path().join("authorized_keys");
    fs::write(&path, format!("from=\"10.0.0.1\" {public}\n")).unwrap();

    let err = AuthorizedKeys::load(&path).unwrap_err();
    assert!(err.to_string().contains("no usable SSH public keys"));
}

#[test]
fn known_hosts_pins_and_append_round_trip() {
    let temp = tempdir().unwrap();
    let private = generate(ssh_key::Algorithm::Ed25519);
    let public = SshPublicKey::from_ssh(private.public_key().clone()).unwrap();
    let path = temp.path().join("known_hosts");

    let mut store = KnownHostsStore::load(&path).unwrap();
    assert_eq!(
        store.status("example.com", 4433, public.spki()),
        HostKeyStatus::Unknown
    );
    store.append("example.com", 4433, &public).unwrap();

    let store = KnownHostsStore::load(&path).unwrap();
    assert_eq!(
        store.status("example.com", 4433, public.spki()),
        HostKeyStatus::Trusted
    );

    let other = generate(ssh_key::Algorithm::Ed25519);
    let other = SshPublicKey::from_ssh(other.public_key().clone()).unwrap();
    assert!(matches!(
        store.status("example.com", 4433, other.spki()),
        HostKeyStatus::Mismatch { .. }
    ));
}

#[test]
fn known_hosts_matches_wildcards_and_ports() {
    let temp = tempdir().unwrap();
    let private = generate(ssh_key::Algorithm::Ed25519);
    let public = SshPublicKey::from_ssh(private.public_key().clone()).unwrap();
    let path = temp.path().join("known_hosts");
    fs::write(
        &path,
        format!(
            "[*.example.com]:4433 {}\n",
            public.public_openssh().unwrap()
        ),
    )
    .unwrap();

    let store = KnownHostsStore::load(&path).unwrap();
    assert_eq!(
        store.status("host.example.com", 4433, public.spki()),
        HostKeyStatus::Trusted
    );
    assert_eq!(
        store.status("host.example.com", 4444, public.spki()),
        HostKeyStatus::Unknown
    );

    let port_path = temp.path().join("known_hosts_port");
    fs::write(
        &port_path,
        format!(
            "{} {}\n",
            canonical_host("example.com", 4444),
            public.public_openssh().unwrap()
        ),
    )
    .unwrap();
    let store = KnownHostsStore::load(&port_path).unwrap();
    assert_eq!(
        store.status("example.com", 4444, public.spki()),
        HostKeyStatus::Trusted
    );
}

#[test]
fn a_bare_known_hosts_entry_only_covers_port_22() {
    // OpenSSH semantics: `host` in known_hosts means `host:22`, not cftp's
    // default port. Sharing a file with OpenSSH must not make an sshd host key
    // count as a cftp pin on another port.
    let temp = tempdir().unwrap();
    let private = generate(ssh_key::Algorithm::Ed25519);
    let public = SshPublicKey::from_ssh(private.public_key().clone()).unwrap();
    let path = temp.path().join("known_hosts");
    fs::write(
        &path,
        format!("example.com {}\n", public.public_openssh().unwrap()),
    )
    .unwrap();

    let store = KnownHostsStore::load(&path).unwrap();
    assert_eq!(
        store.status("example.com", 22, public.spki()),
        HostKeyStatus::Trusted
    );
    assert_eq!(
        store.status("example.com", 4433, public.spki()),
        HostKeyStatus::Unknown
    );
}

#[test]
fn public_key_encoding_round_trips() {
    let private = generate(ssh_key::Algorithm::Ed25519);
    let public = private.public_key();
    let spki = public_key_to_spki(public).unwrap();
    let restored = public_key_from_spki_checked(&spki).unwrap();
    assert_eq!(restored.spki(), spki);
}
