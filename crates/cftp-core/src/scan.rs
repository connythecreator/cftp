use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result, bail};
use cftp_proto::{EntryKind, ManifestEntry};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct ScanResult {
    pub base: PathBuf,
    pub entries: Vec<ManifestEntry>,
    pub total_bytes: u64,
    pub total_files: u64,
    pub total_directories: u64,
    pub total_symlinks: u64,
}

pub fn scan_source(src: &Path) -> Result<ScanResult> {
    let meta = fs::symlink_metadata(src)
        .with_context(|| format!("failed to inspect source {}", src.display()))?;

    let source_text = src.to_string_lossy();
    let contents_only =
        meta.is_dir() && (source_text.ends_with('/') || source_text.ends_with('\\'));
    let base = if meta.is_dir() && contents_only {
        src.to_path_buf()
    } else if meta.is_dir() {
        src.parent().unwrap_or(src).to_path_buf()
    } else {
        src.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    };

    let mut entries = Vec::new();
    if meta.is_dir() {
        for item in WalkDir::new(src)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter()
        {
            let item = item.with_context(|| format!("failed to walk {}", src.display()))?;
            let path = item.path();
            let relative = match path.strip_prefix(&base) {
                Ok(relative) => relative,
                Err(_) => path,
            };
            if relative.as_os_str().is_empty() {
                continue;
            }
            if let Some(entry) = make_entry(path, relative)? {
                entries.push(entry);
            }
        }
    } else if let Some(entry) = make_entry(src, src.file_name().map(Path::new).unwrap_or(src))? {
        entries.push(entry);
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));
    for (index, entry) in entries.iter_mut().enumerate() {
        entry.id = index as u64 + 1;
    }

    let total_bytes = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .map(|entry| entry.size)
        .sum();
    let total_files = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .count() as u64;
    let total_directories = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Directory)
        .count() as u64;
    let total_symlinks = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Symlink)
        .count() as u64;

    Ok(ScanResult {
        base,
        entries,
        total_bytes,
        total_files,
        total_directories,
        total_symlinks,
    })
}

fn make_entry(path: &Path, relative: &Path) -> Result<Option<ManifestEntry>> {
    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    let file_type = meta.file_type();
    let kind = if file_type.is_symlink() {
        EntryKind::Symlink
    } else if file_type.is_dir() {
        EntryKind::Directory
    } else if file_type.is_file() {
        EntryKind::File
    } else {
        bail!("unsupported file type at {}", path.display());
    };

    let path_string = relative.to_string_lossy().replace('\\', "/");
    if path_string.is_empty() {
        return Ok(None);
    }

    let link_target = if kind == EntryKind::Symlink {
        Some(
            fs::read_link(path)
                .with_context(|| format!("failed to read symlink {}", path.display()))?
                .to_string_lossy()
                .into_owned(),
        )
    } else {
        None
    };

    Ok(Some(ManifestEntry {
        id: 0,
        path: path_string,
        kind,
        size: if kind == EntryKind::File {
            meta.len()
        } else {
            0
        },
        mode: file_mode(&meta),
        mtime: file_mtime(&meta),
        link_target,
    }))
}

#[cfg(unix)]
fn file_mode(meta: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o7777
}

#[cfg(not(unix))]
fn file_mode(_meta: &fs::Metadata) -> u32 {
    0
}

fn file_mtime(meta: &fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn trailing_slash_transfers_directory_contents() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("a.txt"), b"a").unwrap();
        let path = format!("{}/", root.display());
        let scan = scan_source(Path::new(&path)).unwrap();
        assert!(scan.entries.iter().all(|entry| entry.path != "project"));
        assert!(scan.entries.iter().any(|entry| entry.path == "a.txt"));
    }

    #[test]
    fn scans_a_tree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("src")).unwrap();
        fs::write(root.join("src/main.js"), b"hello").unwrap();
        fs::write(root.join("package.json"), b"{}").unwrap();

        let scan = scan_source(&root).unwrap();
        assert_eq!(scan.total_files, 2);
        assert_eq!(scan.total_bytes, 7);
        assert!(
            scan.entries
                .iter()
                .any(|entry| entry.path == "project/src/main.js")
        );
        assert!(
            scan.entries
                .iter()
                .any(|entry| entry.path == "project" && entry.kind == EntryKind::Directory)
        );
    }
}
