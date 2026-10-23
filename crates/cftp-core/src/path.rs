use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use filetime::FileTime;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn validate_relative(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        bail!("path is empty");
    }
    if path.contains('\0') {
        bail!("path contains a NUL byte");
    }
    if path.starts_with('/') || path.starts_with('\\') || Path::new(path).is_absolute() {
        bail!("absolute path is not allowed: {path}");
    }
    let bytes = path.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
        bail!("path prefix is not allowed: {path}");
    }

    let mut out = PathBuf::new();
    for part in path.split(['/', '\\']) {
        match part {
            "" | "." => continue,
            ".." => bail!("path escapes the transfer root: {path}"),
            _ => {
                let candidate = Path::new(part);
                if candidate.is_absolute() {
                    bail!("absolute path is not allowed: {path}");
                }
                if let Some(Component::Prefix(_)) = candidate.components().next() {
                    bail!("path prefix is not allowed: {path}");
                }
                out.push(part);
            }
        }
    }

    if out.as_os_str().is_empty() {
        bail!("path is empty after normalization");
    }
    Ok(out)
}

pub fn validate_destination(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        return Ok(PathBuf::new());
    }
    match validate_relative(path) {
        Ok(path) => Ok(path),
        Err(_) if path == "." || path == "/" => Ok(PathBuf::new()),
        Err(err) => Err(err),
    }
}

/// Resolves a client-supplied relative path under `root` for reading.
///
/// The path must already be normalized by [`validate_relative`]. Intermediate
/// components must be real directories and no component may be a symlink: a
/// symlink planted under the root must not redirect a pull outside it. The
/// final component must exist and may be a file or a directory.
pub fn resolve_read_path(root: &Path, relative: &Path) -> Result<PathBuf> {
    let components: Vec<Component> = relative.components().collect();
    if components.is_empty() {
        bail!("path is empty");
    }
    let mut current = root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            bail!("invalid relative path: {}", relative.display());
        };
        current.push(name);
        let meta = fs::symlink_metadata(&current)
            .with_context(|| format!("failed to inspect {}", current.display()))?;
        if meta.file_type().is_symlink() {
            bail!("refusing to read through symlink {}", current.display());
        }
        if index + 1 < components.len() && !meta.is_dir() {
            bail!("{} is not a directory", current.display());
        }
    }
    Ok(current)
}

pub fn ensure_ancestors(root: &Path, relative: &Path) -> Result<()> {
    let parent = match relative.parent() {
        Some(parent) => parent,
        None => return Ok(()),
    };
    let mut current = root.to_path_buf();
    for component in parent.components() {
        let Component::Normal(name) = component else {
            bail!("invalid relative path: {}", relative.display());
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                bail!("refusing to traverse symlink {}", current.display())
            }
            Ok(meta) if !meta.is_dir() => {
                bail!("{} exists and is not a directory", current.display())
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("failed to create {}", current.display()))?;
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to inspect {}", current.display()));
            }
        }
    }
    Ok(())
}

pub fn prepare_file_path(root: &Path, relative: &Path) -> Result<PathBuf> {
    ensure_ancestors(root, relative)?;
    let path = root.join(relative);
    if let Ok(meta) = fs::symlink_metadata(&path) {
        if meta.file_type().is_symlink() {
            bail!("refusing to replace symlink {}", path.display());
        }
        if meta.is_dir() {
            bail!("{} is a directory", path.display());
        }
    }
    Ok(path)
}

pub fn write_atomic(
    root: &Path,
    relative: &Path,
    data: &[u8],
    mode: u32,
    mtime: i64,
) -> Result<()> {
    let final_path = prepare_file_path(root, relative)?;
    let parent = final_path
        .parent()
        .context("destination file has no parent directory")?;
    let (mut file, temp_path) = create_temp(parent)?;
    let result = file
        .write_all(data)
        .with_context(|| format!("failed to write {}", temp_path.display()))
        .and_then(|()| finish_temp(&mut file, &temp_path, &final_path, mode, mtime));
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

pub fn copy_file_atomic(
    root: &Path,
    relative: &Path,
    source: &Path,
    mode: u32,
    mtime: i64,
) -> Result<()> {
    let final_path = prepare_file_path(root, relative)?;
    let parent = final_path
        .parent()
        .context("destination file has no parent directory")?;
    let (mut output, temp_path) = create_temp(parent)?;
    let result = (|| {
        let mut input = File::open(source)
            .with_context(|| format!("failed to open source file {}", source.display()))?;
        std::io::copy(&mut input, &mut output).with_context(|| {
            format!(
                "failed to copy {} to {}",
                source.display(),
                temp_path.display()
            )
        })?;
        finish_temp(&mut output, &temp_path, &final_path, mode, mtime)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

pub fn create_temp_file(root: &Path, relative: &Path) -> Result<(File, PathBuf, PathBuf)> {
    let final_path = prepare_file_path(root, relative)?;
    let parent = final_path
        .parent()
        .context("destination file has no parent directory")?;
    let (file, temp_path) = create_temp(parent)?;
    Ok((file, temp_path, final_path))
}

pub fn finish_temp(
    file: &mut File,
    temp_path: &Path,
    final_path: &Path,
    mode: u32,
    mtime: i64,
) -> Result<()> {
    file.sync_all()
        .with_context(|| format!("failed to sync {}", temp_path.display()))?;
    apply_metadata(temp_path, mode, mtime)?;
    fs::rename(temp_path, final_path).with_context(|| {
        format!(
            "failed to rename {} to {}",
            temp_path.display(),
            final_path.display()
        )
    })?;
    Ok(())
}

pub fn create_directory(root: &Path, relative: &Path, mode: u32, mtime: i64) -> Result<()> {
    let path = root.join(relative);
    ensure_ancestors(root, relative)?;
    match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            bail!("refusing to replace symlink {}", path.display())
        }
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => bail!("{} exists and is not a directory", path.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&path)
                .with_context(|| format!("failed to create {}", path.display()))?;
        }
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", path.display()));
        }
    }
    apply_metadata(&path, mode, mtime)?;
    Ok(())
}

#[cfg(unix)]
pub fn create_symlink(root: &Path, relative: &Path, target: &str) -> Result<()> {
    use std::os::unix::fs::symlink;

    // Validate the target against the real tree, not just lexically: a target can
    // otherwise reach outside the root by walking through a symlink that an
    // earlier manifest entry planted.
    resolve_symlink_target(root, relative, target)?;
    let path = root.join(relative);
    ensure_ancestors(root, relative)?;
    match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            fs::remove_file(&path)
                .with_context(|| format!("failed to replace symlink {}", path.display()))?;
        }
        Ok(_) => bail!("refusing to replace existing path {}", path.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", path.display()));
        }
    }
    symlink(target, &path).with_context(|| format!("failed to create symlink {}", path.display()))
}

#[cfg(not(unix))]
pub fn create_symlink(_root: &Path, relative: &Path, _target: &str) -> Result<()> {
    bail!(
        "symlink creation is not supported on this platform: {}",
        relative.display()
    )
}

/// Resolves a symlink target against the link's parent.
///
/// The walk is performed component by component, and any component that already
/// exists as a symlink is rejected. Normalising `..` first would be unsafe: a
/// planted symlink (`d/s -> ..`) changes what a later `..` means, so
/// `d/s/../../etc/passwd` can look contained while resolving outside the root.
fn resolve_symlink_target(root: &Path, relative: &Path, target: &str) -> Result<PathBuf> {
    if target.is_empty() {
        bail!("empty symlink target");
    }
    let path = Path::new(target);
    if path.is_absolute() {
        bail!("absolute symlink target is not allowed: {target}");
    }

    let mut resolved: PathBuf = relative
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_path_buf();
    for component in path.components() {
        match component {
            Component::Normal(name) => resolved.push(name),
            Component::CurDir => {}
            Component::ParentDir => {
                if !resolved.pop() {
                    bail!("symlink target escapes the root: {target}");
                }
            }
            Component::Prefix(_) | Component::RootDir => {
                bail!("absolute symlink target is not allowed: {target}")
            }
        }

        let current = root.join(&resolved);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                bail!(
                    "symlink target traverses existing symlink {}",
                    current.display()
                )
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to inspect {}", current.display()));
            }
        }
    }

    Ok(resolved)
}

fn create_temp(parent: &Path) -> Result<(File, PathBuf)> {
    for _ in 0..100 {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!(".cftp.tmp.{}.{}", std::process::id(), id);
        let path = parent.join(name);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("failed to create {}", path.display()));
            }
        }
    }
    bail!(
        "failed to allocate a temporary file in {}",
        parent.display()
    )
}

fn apply_metadata(path: &Path, mode: u32, mtime: i64) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Never honour setuid/setgid/sticky bits from a remote manifest: a peer
        // must not be able to plant a setuid binary or tamper with directory
        // ownership semantics.
        let mode = mode & 0o777;
        if mode != 0 {
            fs::set_permissions(path, fs::Permissions::from_mode(mode))
                .with_context(|| format!("failed to set mode on {}", path.display()))?;
        }
    }
    let _ = mode;
    if mtime > 0 {
        let time = FileTime::from_unix_time(mtime, 0);
        filetime::set_file_mtime(path, time)
            .with_context(|| format!("failed to set mtime on {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal() {
        assert!(validate_relative("../escape").is_err());
        assert!(validate_relative("a/../../escape").is_err());
        assert!(validate_relative("/etc/passwd").is_err());
        assert!(validate_relative("a/../b").is_err());
    }

    #[test]
    fn accepts_normal_paths() {
        assert_eq!(
            validate_relative("src/main.js").unwrap(),
            PathBuf::from("src/main.js")
        );
        assert_eq!(
            validate_relative("a//b/./c").unwrap(),
            PathBuf::from("a/b/c")
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        fs::create_dir(&root).unwrap();
        let outside = dir.path().join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(&outside, root.join("link")).unwrap();
        let result = ensure_ancestors(&root, Path::new("link/file"));
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_target_through_a_planted_symlink() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        fs::create_dir_all(root.join("d")).unwrap();
        symlink("..", root.join("d/s")).unwrap();
        // Lexically this stays under the root, but resolving `d/s` would escape.
        let result = create_symlink(&root, Path::new("x"), "d/s/../../etc/passwd");
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn resolve_read_path_rejects_a_symlink_escape() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        fs::create_dir(&root).unwrap();
        let outside = dir.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("secret"), b"x").unwrap();
        symlink(&outside, root.join("link")).unwrap();
        assert!(resolve_read_path(&root, Path::new("link")).is_err());
        assert!(resolve_read_path(&root, Path::new("link/secret")).is_err());
    }

    #[test]
    fn resolve_read_path_accepts_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        fs::create_dir_all(root.join("a/b")).unwrap();
        fs::write(root.join("a/b/file.txt"), b"data").unwrap();
        assert_eq!(
            resolve_read_path(&root, Path::new("a/b/file.txt")).unwrap(),
            root.join("a/b/file.txt")
        );
        assert_eq!(
            resolve_read_path(&root, Path::new("a/b")).unwrap(),
            root.join("a/b")
        );
        assert!(resolve_read_path(&root, Path::new("a/missing")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn strips_setuid_and_setgid_from_remote_modes() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        write_atomic(dir.path(), Path::new("bin"), b"payload", 0o4755, 0).unwrap();
        let meta = fs::metadata(dir.path().join("bin")).unwrap();
        assert_eq!(meta.permissions().mode() & 0o7777, 0o755);
    }
}
