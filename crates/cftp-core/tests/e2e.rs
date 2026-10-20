use std::fs;

use cftp_core::identity::{DEFAULT_SERVER_NAME, Identity, IdentityPaths, init_identity};
use cftp_core::transfer::{PushOptions, ServeOptions, push, serve_once};

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
    };
    let server = tokio::spawn(async move { serve_once(&server_identity, server_opts).await });
    let addr = ready_rx.await.unwrap();

    let push_result = push(
        &project,
        addr,
        &client_identity,
        PushOptions {
            server_name: DEFAULT_SERVER_NAME.to_string(),
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
