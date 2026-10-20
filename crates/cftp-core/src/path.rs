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
    file.write_all(data)
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    finish_temp(&mut file, &temp_path, &final_path, mode, mtime)
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

    validate_symlink_target(relative, target)?;
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

fn validate_symlink_target(relative: &Path, target: &str) -> Result<()> {
    if target.is_empty() {
        bail!("empty symlink target");
    }
    let path = Path::new(target);
    if path.is_absolute() {
        bail!("absolute symlink target is not allowed: {target}");
    }

    // Resolve the target lexically from the symlink's parent and ensure it cannot
    // climb above the transfer root.
    let mut depth = relative
        .parent()
        .map(|parent| {
            parent
                .components()
                .filter(|component| matches!(component, Component::Normal(_)))
                .count() as i64
        })
        .unwrap_or(0);
    for component in path.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    bail!("symlink target escapes the root: {target}");
                }
            }
            Component::Prefix(_) | Component::RootDir => {
                bail!("absolute symlink target is not allowed: {target}")
            }
        }
    }
    Ok(())
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
        let mode = mode & 0o7777;
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
}
