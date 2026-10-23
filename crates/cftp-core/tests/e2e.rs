use std::fs;
use std::time::Duration;

use cftp_core::auth::{ClientAuth, ServerAuth};
use cftp_core::identity::{DEFAULT_SERVER_NAME, Identity, IdentityPaths, init_identity};
use cftp_core::progress::ProgressEvent;
use cftp_core::transfer::{
    PullOptions, PushOptions, ServeOptions, ServePolicy, pull, push, serve_once,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_a_tree_over_mutual_tls() {
    let temp = tempfile::tempdir().unwrap();
    let identity_dir = temp.path().join("identity");
    let identity_paths = IdentityPaths::in_dir(&identity_dir);
    init_identity(&identity_paths, false).unwrap();

    let server_identity = Identity::load(
        &identity_paths.cert,
        &identity_paths.key,
        &identity_paths.ca,
    )
    .unwrap();
    let client_identity = Identity::load(
        &identity_paths.cert,
        &identity_paths.key,
        &identity_paths.ca,
    )
    .unwrap();

    let project = temp.path().join("src/project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::create_dir_all(project.join("node_modules/pkg")).unwrap();
    fs::create_dir_all(project.join("empty-dir")).unwrap();
    fs::write(project.join("src/main.js"), b"console.log('hello')\n").unwrap();
    fs::write(project.join("package.json"), b"{\"name\":\"demo\"}\n").unwrap();
    fs::write(project.join("empty.txt"), b"").unwrap();
    fs::write(
        project.join("node_modules/pkg/large.bin"),
        vec![0x5a; 3 * 1024 * 1024],
    )
    .unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("src/main.js", project.join("link.js")).unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            project.join("package.json"),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
    }

    let dest = temp.path().join("dst");
    fs::create_dir_all(&dest).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_opts = ServeOptions {
        root: dest.clone(),
        listen: "127.0.0.1:0".parse().unwrap(),
        progress: None,
        on_ready: Some(ready_tx),
        max_profile: cftp_core::tuning::HardwareProfile::Default,
        force_v2: false,
        policy: cftp_core::transfer::ServePolicy::default(),
        one_shot: true,
        max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
        on_session: None,
    };
    let server_identity = ServerAuth::Cert(server_identity);
    let server = tokio::spawn(async move { serve_once(&server_identity, server_opts).await });
    let addr = ready_rx.await.unwrap();

    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let push_result = push(
        &project,
        addr,
        &ClientAuth::Cert(client_identity),
        PushOptions {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            progress: Some(progress_tx),
            ..Default::default()
        },
    )
    .await;
    if let Err(err) = &push_result {
        let server_result = tokio::time::timeout(std::time::Duration::from_secs(2), server).await;
        panic!("push failed: {err:#}; server result: {server_result:?}");
    }
    let stats = push_result.unwrap();
    server.await.unwrap().unwrap();

    let mut saw_local = false;
    let mut saw_remote = false;
    while let Ok(event) = progress_rx.try_recv() {
        if let ProgressEvent::Sending(stats) = event {
            saw_local |= stats.read_bytes > 0 && stats.sent_bytes > 0;
            saw_remote |= stats.received_bytes > 0 && stats.written_bytes > 0;
        }
    }
    assert!(saw_local, "expected local read/send progress");
    assert!(saw_remote, "expected server receive/write progress");

    let expected_bytes = [
        "src/main.js",
        "package.json",
        "empty.txt",
        "node_modules/pkg/large.bin",
    ]
    .iter()
    .map(|path| fs::metadata(project.join(path)).unwrap().len())
    .sum::<u64>();
    assert_eq!(stats.files, 4);
    assert_eq!(stats.bytes, expected_bytes);
    assert_eq!(
        fs::read(dest.join("project/src/main.js")).unwrap(),
        b"console.log('hello')\n"
    );
    assert_eq!(
        fs::read(dest.join("project/package.json")).unwrap(),
        b"{\"name\":\"demo\"}\n"
    );
    assert!(dest.join("project/empty.txt").exists());
    assert!(dest.join("project/empty-dir").is_dir());
    assert_eq!(
        fs::read(dest.join("project/node_modules/pkg/large.bin")).unwrap(),
        vec![0x5a; 3 * 1024 * 1024]
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            fs::symlink_metadata(dest.join("project/link.js"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read_link(dest.join("project/link.js")).unwrap(),
            std::path::PathBuf::from("src/main.js")
        );
        assert_eq!(
            fs::metadata(dest.join("project/package.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }
}

#[cfg(unix)]
#[test]
fn symlink_targets_can_move_within_the_root() {
    use std::path::Path;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir_all(root.join("project/src")).unwrap();
    cftp_core::path::create_symlink(
        &root,
        Path::new("project/link.js"),
        "../project/src/main.js",
    )
    .unwrap();
    assert_eq!(
        fs::read_link(root.join("project/link.js")).unwrap(),
        std::path::PathBuf::from("../project/src/main.js")
    );
}

#[tokio::test]
async fn local_copy_preserves_tree_and_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("sample-project");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::create_dir_all(source.join("empty-dir")).unwrap();
    fs::write(source.join("src/main.js"), b"hello\n").unwrap();
    fs::write(source.join("package.json"), b"{}").unwrap();
    fs::write(source.join("empty.txt"), b"").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::os::unix::fs::symlink("src/main.js", source.join("link.js")).unwrap();
        fs::set_permissions(
            source.join("package.json"),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
    }

    let destination = temp.path().join("sample-copy");
    let stats = cftp_core::local::copy_local(&source, &destination, None)
        .await
        .unwrap();
    assert_eq!(stats.files, 3);
    assert_eq!(
        fs::read(destination.join("sample-project/src/main.js")).unwrap(),
        b"hello\n"
    );
    assert!(destination.join("sample-project/empty-dir").is_dir());
    assert!(destination.join("sample-project/empty.txt").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(
            fs::symlink_metadata(destination.join("sample-project/link.js"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::metadata(destination.join("sample-project/package.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }
}

#[tokio::test]
async fn local_copy_rejects_destination_inside_source() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("sample-project");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("nested/file.txt"), b"hello\n").unwrap();

    let destination = source.join("nested");
    let result = cftp_core::local::copy_local(&source, &destination, None).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_invalid_destination_without_hanging() {
    let temp = tempfile::tempdir().unwrap();
    let identity_dir = temp.path().join("identity");
    let identity_paths = IdentityPaths::in_dir(&identity_dir);
    init_identity(&identity_paths, false).unwrap();

    let server_identity = Identity::load(
        &identity_paths.cert,
        &identity_paths.key,
        &identity_paths.ca,
    )
    .unwrap();
    let client_identity = Identity::load(
        &identity_paths.cert,
        &identity_paths.key,
        &identity_paths.ca,
    )
    .unwrap();

    let source = temp.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("file.txt"), b"hello\n").unwrap();
    let dest = temp.path().join("dst");
    fs::create_dir_all(&dest).unwrap();

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_identity = ServerAuth::Cert(server_identity);
    let server = tokio::spawn(async move {
        serve_once(
            &server_identity,
            ServeOptions {
                root: dest,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: cftp_core::tuning::HardwareProfile::Default,
                force_v2: false,
                policy: cftp_core::transfer::ServePolicy::default(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        push(
            &source,
            addr,
            &ClientAuth::Cert(client_identity),
            PushOptions {
                destination: "../escape".to_string(),
                server_name: DEFAULT_SERVER_NAME.to_string(),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("push timed out waiting for the server error");
    let err = result.unwrap_err();
    assert!(
        format!("{err:#}").contains("escapes"),
        "unexpected error: {err:#}"
    );

    let server_result = server.await.unwrap();
    assert!(server_result.is_err());
}

#[tokio::test]
async fn local_copy_rejects_source_inside_destination() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("sample-project");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("file.txt"), b"hello\n").unwrap();

    let result = cftp_core::local::copy_local(&source, temp.path(), None).await;
    assert!(result.is_err());

    let result = cftp_core::local::copy_local(&source, &source, None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn local_copy_normalizes_parent_components() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("sample-project");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("file.txt"), b"hello\n").unwrap();

    let inside = source.join("nested/../nested");
    let result = cftp_core::local::copy_local(&source, &inside, None).await;
    assert!(result.is_err());

    let destination = temp.path().join("unused/../sample-copy");
    cftp_core::local::copy_local(&source, &destination, None)
        .await
        .unwrap();
    assert!(
        temp.path()
            .join("sample-copy/sample-project/file.txt")
            .exists()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_a_tree_over_ssh_keys() {
    use cftp_core::auth::{HostKeyPolicy, SshClientAuth, SshServerAuth};
    use cftp_core::ssh::{SshPublicKey, from_private_key};
    use cftp_core::trust::{AuthorizedKeys, KnownHostsStore, canonical_host};
    use ssh_key::rand_core::OsRng;

    let temp = tempfile::tempdir().unwrap();
    let server_private =
        ssh_key::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap();
    let client_private =
        ssh_key::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap();

    let server_public = SshPublicKey::from_ssh(server_private.public_key().clone()).unwrap();
    let client_public = SshPublicKey::from_ssh(client_private.public_key().clone()).unwrap();

    let authorized_keys_path = temp.path().join("authorized_keys");
    fs::write(
        &authorized_keys_path,
        format!("{}\n", client_public.public_openssh().unwrap()),
    )
    .unwrap();

    let server_auth = ServerAuth::Ssh(SshServerAuth::new(
        from_private_key(server_private).unwrap(),
        AuthorizedKeys::load(&authorized_keys_path).unwrap(),
    ));

    let project = temp.path().join("src/project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/main.js"), b"console.log('ssh')\n").unwrap();

    let dest = temp.path().join("dst");
    fs::create_dir_all(&dest).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_dest = dest.clone();
    let server = tokio::spawn(async move {
        serve_once(
            &server_auth,
            ServeOptions {
                root: server_dest,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: cftp_core::tuning::HardwareProfile::Default,
                force_v2: false,
                policy: cftp_core::transfer::ServePolicy::default(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();

    let known_hosts_path = temp.path().join("known_hosts");
    fs::write(
        &known_hosts_path,
        format!(
            "{} {}\n",
            canonical_host("127.0.0.1", addr.port()),
            server_public.public_openssh().unwrap()
        ),
    )
    .unwrap();

    let client_auth = ClientAuth::Ssh(SshClientAuth::new(
        from_private_key(client_private).unwrap(),
        KnownHostsStore::load(&known_hosts_path).unwrap(),
        "127.0.0.1",
        addr.port(),
        HostKeyPolicy::Strict,
    ));

    let stats = push(
        &project,
        addr,
        &client_auth,
        PushOptions {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    server.await.unwrap().unwrap();

    assert_eq!(stats.files, 1);
    assert_eq!(
        fs::read(dest.join("project/src/main.js")).unwrap(),
        b"console.log('ssh')\n"
    );
}

async fn run_profile_transfer(
    requested: cftp_core::tuning::HardwareProfile,
    server_cap: cftp_core::tuning::HardwareProfile,
    force_v2: bool,
    hotkeys: Option<tokio::sync::watch::Receiver<cftp_core::tuning::HardwareProfile>>,
    size: usize,
) -> (cftp_core::transfer::PushStats, Vec<u8>) {
    use cftp_core::auth::{HostKeyPolicy, SshClientAuth, SshServerAuth};
    use cftp_core::ssh::{SshPublicKey, from_private_key};
    use cftp_core::trust::AuthorizedKeys;
    use cftp_core::tuning::Tuning;
    use ssh_key::rand_core::OsRng;

    let temp = tempfile::tempdir().unwrap();
    let server_private =
        ssh_key::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap();
    let client_private =
        ssh_key::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519).unwrap();
    let client_public = SshPublicKey::from_ssh(client_private.public_key().clone()).unwrap();
    let authorized_keys_path = temp.path().join("authorized_keys");
    fs::write(
        &authorized_keys_path,
        format!("{}\n", client_public.public_openssh().unwrap()),
    )
    .unwrap();

    let server_auth = ServerAuth::Ssh(SshServerAuth::new(
        from_private_key(server_private).unwrap(),
        AuthorizedKeys::load(&authorized_keys_path).unwrap(),
    ));
    let client_auth = ClientAuth::Ssh(SshClientAuth::new(
        from_private_key(client_private).unwrap(),
        cftp_core::trust::KnownHostsStore::load(temp.path().join("known_hosts")).unwrap(),
        "127.0.0.1",
        4433,
        HostKeyPolicy::AcceptNew,
    ));

    let project = temp.path().join("src/project");
    fs::create_dir_all(&project).unwrap();
    let payload = vec![0x5a; size];
    fs::write(project.join("large.bin"), &payload).unwrap();

    let dest = temp.path().join("dst");
    fs::create_dir_all(&dest).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_dest = dest.clone();
    let server = tokio::spawn(async move {
        serve_once(
            &server_auth,
            ServeOptions {
                root: server_dest,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: server_cap,
                force_v2,
                policy: cftp_core::transfer::ServePolicy::default(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();

    let stats = push(
        &project,
        addr,
        &client_auth,
        PushOptions {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            profile: requested,
            tuning: Tuning::for_profile(requested, None),
            force_v2,
            profile_requests: hotkeys,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    server.await.unwrap().unwrap();

    let received = fs::read(dest.join("project/large.bin")).unwrap();
    assert_eq!(received, payload);
    (stats, received)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profiles_negotiate_and_server_caps_requests() {
    use cftp_core::tuning::HardwareProfile;

    let (stats, _) = run_profile_transfer(
        HardwareProfile::Light,
        HardwareProfile::Default,
        false,
        None,
        1024 * 1024,
    )
    .await;
    assert_eq!(stats.effective_profile, HardwareProfile::Light);

    let (stats, _) = run_profile_transfer(
        HardwareProfile::Aggressive,
        HardwareProfile::Default,
        false,
        None,
        1024 * 1024,
    )
    .await;
    assert_eq!(stats.effective_profile, HardwareProfile::Default);

    let (stats, _) = run_profile_transfer(
        HardwareProfile::Aggressive,
        HardwareProfile::Aggressive,
        false,
        None,
        1024 * 1024,
    )
    .await;
    assert_eq!(stats.effective_profile, HardwareProfile::Aggressive);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_v2_ignores_profiles() {
    use cftp_core::tuning::HardwareProfile;

    let (stats, _) = run_profile_transfer(
        HardwareProfile::Aggressive,
        HardwareProfile::Default,
        true,
        None,
        1024 * 1024,
    )
    .await;
    assert_eq!(stats.effective_profile, HardwareProfile::Default);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hotswaps_profile_during_transfer() {
    use cftp_core::tuning::HardwareProfile;

    let (tx, rx) = tokio::sync::watch::channel(HardwareProfile::Light);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tx.send(HardwareProfile::Aggressive);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tx.send(HardwareProfile::Light);
    });

    let (stats, _) = run_profile_transfer(
        HardwareProfile::Light,
        HardwareProfile::Aggressive,
        false,
        Some(rx),
        32 * 1024 * 1024,
    )
    .await;
    assert!(matches!(
        stats.effective_profile,
        HardwareProfile::Light | HardwareProfile::Aggressive
    ));
}

fn cert_pair(temp: &tempfile::TempDir) -> (ServerAuth, ClientAuth) {
    let identity_paths = IdentityPaths::in_dir(temp.path().join("identity"));
    init_identity(&identity_paths, true).unwrap();
    let server = Identity::load(
        &identity_paths.cert,
        &identity_paths.key,
        &identity_paths.ca,
    )
    .unwrap();
    let client = Identity::load(
        &identity_paths.cert,
        &identity_paths.key,
        &identity_paths.ca,
    )
    .unwrap();
    (ServerAuth::Cert(server), ClientAuth::Cert(client))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pulls_a_tree_over_mutual_tls() {
    let temp = tempfile::tempdir().unwrap();
    let (server_auth, client_auth) = cert_pair(&temp);

    let root = temp.path().join("served");
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::create_dir_all(project.join("empty-dir")).unwrap();
    fs::write(project.join("src/main.js"), b"console.log('pulled')\n").unwrap();
    fs::write(project.join("empty.txt"), b"").unwrap();
    fs::write(project.join("large.bin"), vec![0x42; 2 * 1024 * 1024]).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("src/main.js", project.join("link.js")).unwrap();

    let destination = temp.path().join("downloads");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve_once(
            &server_auth,
            ServeOptions {
                root,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: cftp_core::tuning::HardwareProfile::Default,
                force_v2: false,
                policy: ServePolicy::default(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();

    let stats = pull(
        addr,
        &client_auth,
        PullOptions {
            source: "project/".into(),
            destination: destination.clone(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("pull failed");
    assert_eq!(stats.files, 3);
    assert_eq!(stats.bytes, 22 + 2 * 1024 * 1024);
    server.await.unwrap().unwrap();

    assert_eq!(
        fs::read(destination.join("src/main.js")).unwrap(),
        b"console.log('pulled')\n"
    );
    assert!(destination.join("empty.txt").exists());
    assert!(destination.join("empty-dir").is_dir());
    assert_eq!(
        fs::read(destination.join("large.bin")).unwrap(),
        vec![0x42; 2 * 1024 * 1024]
    );
    #[cfg(unix)]
    assert_eq!(
        fs::read_link(destination.join("link.js")).unwrap(),
        std::path::PathBuf::from("src/main.js")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preserves_the_directory_name_without_a_trailing_slash() {
    let temp = tempfile::tempdir().unwrap();
    let (server_auth, client_auth) = cert_pair(&temp);

    let root = temp.path().join("served");
    fs::create_dir_all(root.join("project/src")).unwrap();
    fs::write(root.join("project/src/main.js"), b"pulled\n").unwrap();

    let destination = temp.path().join("downloads");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve_once(
            &server_auth,
            ServeOptions {
                root,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: cftp_core::tuning::HardwareProfile::Default,
                force_v2: false,
                policy: ServePolicy::default(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();

    pull(
        addr,
        &client_auth,
        PullOptions {
            source: "project".into(),
            destination: destination.clone(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("pull failed");
    server.await.unwrap().unwrap();

    assert_eq!(
        fs::read(destination.join("project/src/main.js")).unwrap(),
        b"pulled\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_rejects_disabled_modes() {
    let temp = tempfile::tempdir().unwrap();
    let (server_auth, client_auth) = cert_pair(&temp);
    let root = temp.path().join("served");
    fs::create_dir_all(root.join("project")).unwrap();
    fs::write(root.join("project/a.txt"), b"data").unwrap();

    // Pull-only server: a push is refused with a clear message.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_root = root.clone();
    let server = tokio::spawn(async move {
        serve_once(
            &server_auth,
            ServeOptions {
                root: server_root,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: cftp_core::tuning::HardwareProfile::Default,
                force_v2: false,
                policy: ServePolicy::pull_only(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();
    let source = temp.path().join("incoming");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.txt"), b"data").unwrap();
    let err = push(
        &source,
        addr,
        &client_auth,
        PushOptions {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            ..Default::default()
        },
    )
    .await
    .expect_err("push should be refused");
    assert!(
        format!("{err:#}").contains("push is disabled"),
        "unexpected error: {err:#}"
    );
    assert!(server.await.unwrap().is_err());

    // Push-only server: a pull is refused before any scan or data.
    let (server_auth, client_auth) = cert_pair(&temp);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve_once(
            &server_auth,
            ServeOptions {
                root,
                listen: "127.0.0.1:0".parse().unwrap(),
                progress: None,
                on_ready: Some(ready_tx),
                max_profile: cftp_core::tuning::HardwareProfile::Default,
                force_v2: false,
                policy: ServePolicy::push_only(),
                one_shot: true,
                max_sessions: cftp_core::transfer::MAX_CONCURRENT_SESSIONS,
                on_session: None,
            },
        )
        .await
    });
    let addr = ready_rx.await.unwrap();
    let destination = temp.path().join("downloads");
    let err = pull(
        addr,
        &client_auth,
        PullOptions {
            source: "project/a.txt".into(),
            destination,
            server_name: DEFAULT_SERVER_NAME.to_string(),
            ..Default::default()
        },
    )
    .await
    .expect_err("pull should be refused");
    assert!(
        format!("{err:#}").contains("pull is disabled"),
        "unexpected error: {err:#}"
    );
    assert!(server.await.unwrap().is_err());
}
