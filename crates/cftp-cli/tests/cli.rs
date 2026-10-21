use std::fs;
use std::net::UdpSocket;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::Command as AssertCommand;
use predicates::prelude::*;
use tempfile::tempdir;

fn cftp() -> AssertCommand {
    AssertCommand::cargo_bin("cftp").expect("cftp binary")
}

#[test]
fn reports_version_and_help() {
    cftp()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "cftp {}",
            env!("CARGO_PKG_VERSION")
        )));

    cftp()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("identity"))
        .stdout(predicate::str::contains("serve"))
        .stdout(predicate::str::contains("put"));
}

#[test]
fn identity_init_refuses_overwrite_and_force_replaces() {
    let temp = tempdir().unwrap();
    let dir = temp.path().join("identity");

    cftp()
        .args(["identity", "init", "--dir"])
        .arg(&dir)
        .assert()
        .success();

    for name in ["ca.pem", "identity.pem", "identity.key"] {
        assert!(dir.join(name).is_file(), "missing {name}");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(dir.join("identity.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    cftp()
        .args(["identity", "init", "--dir"])
        .arg(&dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("already exists"));

    cftp()
        .args(["identity", "init", "--dir"])
        .arg(&dir)
        .arg("--force")
        .assert()
        .success();
}

#[test]
fn local_copy_preserves_the_source_directory_name() {
    let temp = tempdir().unwrap();
    let source = temp.path().join("sample-project");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/main.js"), b"hello\n").unwrap();

    let destination = temp.path().join("sample-copy");
    cftp()
        .arg("put")
        .arg(&source)
        .arg(&destination)
        .assert()
        .success();

    assert_eq!(
        fs::read(destination.join("sample-project/src/main.js")).unwrap(),
        b"hello\n"
    );
}

#[test]
fn trailing_slash_copies_directory_contents() {
    let temp = tempdir().unwrap();
    let source = temp.path().join("sample-project");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/main.js"), b"hello\n").unwrap();

    let source_with_slash = format!("{}{}", source.display(), std::path::MAIN_SEPARATOR);
    let destination = temp.path().join("sample-copy");
    cftp()
        .arg("put")
        .arg(&source_with_slash)
        .arg(&destination)
        .assert()
        .success();

    assert_eq!(
        fs::read(destination.join("src/main.js")).unwrap(),
        b"hello\n"
    );
}

#[test]
fn rejects_non_cftp_urls() {
    let temp = tempdir().unwrap();
    let source = temp.path().join("sample.txt");
    fs::write(&source, b"hello\n").unwrap();

    cftp()
        .arg("put")
        .arg(&source)
        .arg("http://example.com/destination")
        .assert()
        .failure()
        .stderr(predicate::str::contains("cftp://"));
}

#[test]
fn loopback_serve_and_put() {
    let temp = tempdir().unwrap();
    let identity_dir = temp.path().join("identity");
    cftp()
        .args(["identity", "init", "--dir"])
        .arg(&identity_dir)
        .assert()
        .success();

    let source = temp.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("file.txt"), b"hello\n").unwrap();

    let destination = temp.path().join("dst");
    fs::create_dir_all(&destination).unwrap();

    let port = free_udp_port();
    let mut server = Command::new(env!("CARGO_BIN_EXE_cftp"))
        .args(["serve", "--root"])
        .arg(&destination)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .arg("--identity")
        .arg(identity_dir.join("identity.pem"))
        .arg("--key")
        .arg(identity_dir.join("identity.key"))
        .arg("--client-ca")
        .arg(identity_dir.join("ca.pem"))
        .arg("--quiet")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let url = format!("cftp://127.0.0.1:{port}/incoming");
    let mut last_failure = None;
    let mut sent = false;
    for _ in 0..50 {
        if let Some(status) = server.try_wait().unwrap() {
            panic!("server exited before receiving a transfer: {status}");
        }

        let output = Command::new(env!("CARGO_BIN_EXE_cftp"))
            .arg("put")
            .arg(&source)
            .arg(&url)
            .arg("--identity")
            .arg(identity_dir.join("identity.pem"))
            .arg("--key")
            .arg(identity_dir.join("identity.key"))
            .arg("--server-ca")
            .arg(identity_dir.join("ca.pem"))
            .arg("--quiet")
            .output()
            .unwrap();

        if output.status.success() {
            sent = true;
            break;
        }
        last_failure = Some(output);
        thread::sleep(Duration::from_millis(100));
    }

    assert!(
        sent,
        "put never succeeded: {}",
        last_failure
            .map(|output| String::from_utf8_lossy(&output.stderr).into_owned())
            .unwrap_or_default()
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if server.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() > deadline {
            let _ = server.kill();
            panic!("one-shot server did not exit");
        }
        thread::sleep(Duration::from_millis(50));
    }

    assert_eq!(
        fs::read(destination.join("incoming/source/file.txt")).unwrap(),
        b"hello\n"
    );
}

fn free_udp_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

#[test]
fn loopback_serve_and_put_with_ssh_keys() {
    use ssh_key::LineEnding;
    use ssh_key::rand_core::OsRng;

    let temp = tempdir().unwrap();
    let server_key = temp.path().join("server_ed25519");
    let client_key = temp.path().join("client_ed25519");
    let server_private =
        ssh_key::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap();
    let client_private =
        ssh_key::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap();
    server_private
        .write_openssh_file(&server_key, LineEnding::LF)
        .unwrap();
    client_private
        .write_openssh_file(&client_key, LineEnding::LF)
        .unwrap();

    let authorized_keys = temp.path().join("authorized_keys");
    fs::write(
        &authorized_keys,
        format!("{}\n", client_private.public_key().to_openssh().unwrap()),
    )
    .unwrap();
    let known_hosts = temp.path().join("known_hosts");

    let source = temp.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("file.txt"), b"hello ssh\n").unwrap();

    let destination = temp.path().join("dst");
    fs::create_dir_all(&destination).unwrap();

    let port = free_udp_port();
    let mut server = Command::new(env!("CARGO_BIN_EXE_cftp"))
        .args(["serve", "--root"])
        .arg(&destination)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .arg("--host-key")
        .arg(&server_key)
        .arg("--authorized-keys")
        .arg(&authorized_keys)
        .arg("--quiet")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let url = format!("cftp://127.0.0.1:{port}/incoming");
    let mut last_failure = None;
    let mut sent = false;
    for _ in 0..50 {
        if let Some(status) = server.try_wait().unwrap() {
            panic!("server exited before receiving a transfer: {status}");
        }

        let output = Command::new(env!("CARGO_BIN_EXE_cftp"))
            .arg("put")
            .arg(&source)
            .arg(&url)
            .arg("--ssh-key")
            .arg(&client_key)
            .arg("--known-hosts")
            .arg(&known_hosts)
            .arg("--accept-new")
            .arg("--quiet")
            .output()
            .unwrap();

        if output.status.success() {
            sent = true;
            break;
        }
        last_failure = Some(output);
        thread::sleep(Duration::from_millis(100));
    }

    assert!(
        sent,
        "put never succeeded: {}",
        last_failure
            .map(|output| String::from_utf8_lossy(&output.stderr).into_owned())
            .unwrap_or_default()
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if server.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() > deadline {
            let _ = server.kill();
            panic!("one-shot server did not exit");
        }
        thread::sleep(Duration::from_millis(50));
    }

    assert_eq!(
        fs::read(destination.join("incoming/source/file.txt")).unwrap(),
        b"hello ssh\n"
    );
    let known_hosts_contents = fs::read_to_string(&known_hosts).unwrap();
    assert!(
        known_hosts_contents.contains(
            &server_private
                .public_key()
                .to_openssh()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
                .to_string()
        ),
        "known_hosts did not receive the server key: {known_hosts_contents}"
    );
}

#[test]
fn hardware_profile_flags_are_mutually_exclusive() {
    let temp = tempdir().unwrap();
    let source = temp.path().join("source.txt");
    fs::write(&source, b"hello\n").unwrap();

    cftp()
        .arg("put")
        .arg(&source)
        .arg(temp.path().join("dst"))
        .arg("--light")
        .arg("--aggressive")
        .assert()
        .failure();
}

#[test]
fn hardware_profiles_conflict_with_explicit_tuning() {
    let temp = tempdir().unwrap();
    let source = temp.path().join("source.txt");
    fs::write(&source, b"hello\n").unwrap();

    cftp()
        .arg("put")
        .arg(&source)
        .arg(temp.path().join("dst"))
        .arg("--aggressive")
        .arg("--chunk-size")
        .arg("1024")
        .assert()
        .failure();
}

#[test]
fn help_lists_hardware_profiles() {
    cftp()
        .arg("put")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--light"))
        .stdout(predicate::str::contains("--default"))
        .stdout(predicate::str::contains("--aggressive"));

    cftp()
        .arg("serve")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--light"))
        .stdout(predicate::str::contains("--default"))
        .stdout(predicate::str::contains("--aggressive"));
}
