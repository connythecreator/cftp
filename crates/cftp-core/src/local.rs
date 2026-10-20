use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use cftp_proto::EntryKind;

use crate::path;
use crate::progress::{ProgressEvent, ProgressSender, TransferProgress};
use crate::scan;

#[derive(Debug, Clone, Default)]
pub struct LocalCopyStats {
    pub files: u64,
    pub bytes: u64,
}

pub async fn copy_local(
    src: &Path,
    dest: &Path,
    progress: Option<ProgressSender>,
) -> Result<LocalCopyStats> {
    let scan = scan::scan_source(src)?;
    if let Some(progress) = &progress {
        let _ = progress.send(ProgressEvent::Scan {
            files: scan.total_files,
            bytes: scan.total_bytes,
        });
    }

    let source_root = fs::canonicalize(src)
        .with_context(|| format!("failed to resolve source {}", src.display()))?;
    let destination_root = resolve_destination(dest)?;
    if destination_root.starts_with(&source_root) {
        bail!(
            "destination {} is inside source {}",
            dest.display(),
            src.display()
        );
    }
    if source_root.starts_with(&destination_root) {
        bail!(
            "source {} is inside destination {}",
            src.display(),
            dest.display()
        );
    }

    fs::create_dir_all(&destination_root)
        .with_context(|| format!("failed to create destination {}", dest.display()))?;
    let meta = fs::metadata(&destination_root)
        .with_context(|| format!("failed to inspect destination {}", dest.display()))?;
    if !meta.is_dir() {
        bail!("destination is not a directory: {}", dest.display());
    }

    let mut stats = LocalCopyStats::default();
    for entry in &scan.entries {
        let relative = Path::new(&entry.path);
        match entry.kind {
            EntryKind::Directory => {
                path::create_directory(&destination_root, relative, entry.mode, entry.mtime)?;
            }
            EntryKind::Symlink => {
                let target = entry
                    .link_target
                    .as_deref()
                    .with_context(|| format!("symlink {} has no target", entry.path))?;
                path::create_symlink(&destination_root, relative, target)?;
            }
            EntryKind::File => {
                let source = scan.base.join(entry_path(&entry.path));
                path::copy_file_atomic(
                    &destination_root,
                    relative,
                    &source,
                    entry.mode,
                    entry.mtime,
                )?;
                stats.files += 1;
                stats.bytes += entry.size;
                if let Some(progress) = &progress {
                    let _ = progress.send(ProgressEvent::Sending(TransferProgress {
                        at: std::time::Instant::now(),
                        bytes_done: stats.bytes,
                        bytes_total: scan.total_bytes,
                        files_done: stats.files,
                        files_total: scan.total_files,
                        read_bytes: stats.bytes,
                        sent_bytes: 0,
                        received_bytes: 0,
                        written_bytes: stats.bytes,
                        remote_elapsed_millis: 0,
                    }));
                }
            }
        }
    }

    if let Some(progress) = &progress {
        let _ = progress.send(ProgressEvent::Complete {
            files: stats.files,
            bytes: stats.bytes,
        });
    }
    Ok(stats)
}

fn resolve_destination(dest: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(dest)
        .with_context(|| format!("failed to resolve destination {}", dest.display()))?;
    let normalized = normalize_lexically(&absolute);

    let mut current = normalized.as_path();
    let mut suffix = Vec::new();
    loop {
        match fs::symlink_metadata(current) {
            Ok(_) => break,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let name = current
                    .file_name()
                    .with_context(|| format!("failed to resolve destination {}", dest.display()))?;
                suffix.push(name.to_os_string());
                current = current
                    .parent()
                    .with_context(|| format!("failed to resolve destination {}", dest.display()))?;
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to inspect {}", current.display()));
            }
        }
    }

    let mut resolved = fs::canonicalize(current)
        .with_context(|| format!("failed to resolve {}", current.display()))?;
    for name in suffix.iter().rev() {
        resolved.push(name);
    }
    Ok(resolved)
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let absolute = path.is_absolute();
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() && !absolute {
                    out.push(component.as_os_str());
                }
            }
            Component::Normal(name) => out.push(name),
        }
    }
    out
}

fn entry_path(path: &str) -> PathBuf {
    PathBuf::from(path.replace('/', std::path::MAIN_SEPARATOR_STR))
}
