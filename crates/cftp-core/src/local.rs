use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use cftp_proto::EntryKind;

use crate::path;
use crate::progress::{ProgressEvent, ProgressSender};
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
    let destination_root = if dest.exists() {
        fs::canonicalize(dest)
            .with_context(|| format!("failed to resolve destination {}", dest.display()))?
    } else {
        std::env::current_dir()
            .context("failed to determine the current directory")?
            .join(dest)
    };
    if destination_root.starts_with(&source_root) {
        bail!(
            "destination {} is inside source {}",
            dest.display(),
            src.display()
        );
    }

    fs::create_dir_all(dest)
        .with_context(|| format!("failed to create destination {}", dest.display()))?;
    let meta = fs::metadata(dest)
        .with_context(|| format!("failed to inspect destination {}", dest.display()))?;
    if !meta.is_dir() {
        bail!("destination is not a directory: {}", dest.display());
    }

    let mut stats = LocalCopyStats::default();
    for entry in &scan.entries {
        let relative = Path::new(&entry.path);
        match entry.kind {
            EntryKind::Directory => {
                path::create_directory(dest, relative, entry.mode, entry.mtime)?;
            }
            EntryKind::Symlink => {
                let target = entry
                    .link_target
                    .as_deref()
                    .with_context(|| format!("symlink {} has no target", entry.path))?;
                path::create_symlink(dest, relative, target)?;
            }
            EntryKind::File => {
                let source = scan.base.join(entry_path(&entry.path));
                path::copy_file_atomic(dest, relative, &source, entry.mode, entry.mtime)?;
                stats.files += 1;
                stats.bytes += entry.size;
                if let Some(progress) = &progress {
                    let _ = progress.send(ProgressEvent::Sending {
                        bytes_done: stats.bytes,
                        bytes_total: scan.total_bytes,
                        files_done: stats.files,
                        files_total: scan.total_files,
                    });
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

fn entry_path(path: &str) -> PathBuf {
    PathBuf::from(path.replace('/', std::path::MAIN_SEPARATOR_STR))
}
