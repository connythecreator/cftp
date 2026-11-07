use super::*;

pub(super) struct Dataset {
    pub(super) name: String,
    pub(super) root: PathBuf,
    pub(super) files: u64,
    pub(super) bytes: u64,
}

pub(super) fn generate_dataset(base: &Path, name: &str, cfg: &Config) -> Result<Dataset, String> {
    let root = base.join(name);
    fs::create_dir_all(&root)
        .map_err(|err| format!("failed to create {}: {err}", root.display()))?;

    match name {
        "many-small" => {
            for index in 0..cfg.small_count {
                let path = root.join(format!("file-{index:05}.bin"));
                write_pattern(&path, cfg.small_size, seed_for(index as u64))
                    .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
            }
        }
        "large" => {
            let path = root.join("large.bin");
            write_pattern(&path, cfg.large_size, seed_for(1))
                .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
        }
        "huge" => {
            for index in 0..cfg.huge_files {
                let path = root.join(format!("huge-{index:02}.bin"));
                write_pattern(&path, cfg.huge_size, seed_for((index + 200) as u64))
                    .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
            }
        }
        "mixed" => {
            let path = root.join("large.bin");
            write_pattern(&path, cfg.mixed_large_size, seed_for(2))
                .map_err(|err| format!("failed to write {}: {err}", path.display()))?;

            let medium_size = cfg.mixed_small_size.saturating_mul(16).max(1);
            for dir_index in 0..cfg.mixed_dirs {
                let top = root.join(format!("dir-{dir_index:02}"));
                let nested = top.join("nested").join("level-2");
                fs::create_dir_all(&nested)
                    .map_err(|err| format!("failed to create {}: {err}", nested.display()))?;

                for file_index in 0..cfg.mixed_per_dir {
                    let path = nested.join(format!("file-{file_index:04}.bin"));
                    let seed = seed_for((dir_index * 1000 + file_index + 10) as u64);
                    write_pattern(&path, cfg.mixed_small_size, seed)
                        .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
                }

                let medium = top.join(format!("medium-{dir_index:02}.bin"));
                write_pattern(&medium, medium_size, seed_for((dir_index + 100) as u64))
                    .map_err(|err| format!("failed to write {}: {err}", medium.display()))?;
            }
        }
        other => return Err(format!("unknown dataset {other:?}")),
    }

    let (files, bytes) = tree_stats(&root)?;
    Ok(Dataset {
        name: name.to_string(),
        root,
        files,
        bytes,
    })
}

pub(super) fn seed_for(index: u64) -> u64 {
    0x9E37_79B9_7F4A_7C15u64
        .wrapping_mul(index.wrapping_add(1))
        .max(1)
}

/// Write `size` deterministic, practically incompressible bytes.
pub(super) fn write_pattern(path: &Path, size: u64, seed: u64) -> io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    let mut rng = Rng(seed | 1);
    let mut buffer = vec![0u8; 64 * 1024];

    let mut remaining = size;
    while remaining > 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        rng.fill(&mut buffer[..take]);
        writer.write_all(&buffer[..take])?;
        remaining -= take as u64;
    }
    writer.flush()
}

pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    pub(super) fn fill(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

pub(super) fn walk_files(root: &Path) -> io::Result<Vec<(PathBuf, u64)>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                let size = entry.metadata()?.len();
                files.push((entry.path(), size));
            }
        }
    }
    files.sort();
    Ok(files)
}

pub(super) fn tree_stats(root: &Path) -> Result<(u64, u64), String> {
    let files =
        walk_files(root).map_err(|err| format!("failed to walk {}: {err}", root.display()))?;
    let mut bytes = 0u64;
    for (_, size) in &files {
        bytes = bytes.saturating_add(*size);
    }
    Ok((files.len() as u64, bytes))
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

pub(super) fn verify_tree(source: &Path, destination: &Path) -> Result<(), String> {
    if !destination.is_dir() {
        return Err(format!(
            "destination {} does not exist after the transfer",
            destination.display()
        ));
    }

    let expected = relative_files(source)?;
    let mut actual = relative_files(destination)?;

    for (relative, expected_size) in &expected {
        let Some(actual_size) = actual.remove(relative) else {
            return Err(format!("missing file {}", relative.display()));
        };
        if actual_size != *expected_size {
            return Err(format!(
                "size mismatch for {}: expected {expected_size} bytes, got {actual_size}",
                relative.display()
            ));
        }
        if !files_equal(&source.join(relative), &destination.join(relative))? {
            return Err(format!("content mismatch for {}", relative.display()));
        }
    }

    if let Some((extra, _)) = actual.iter().next() {
        return Err(format!("unexpected file {}", extra.display()));
    }
    Ok(())
}

pub(super) fn relative_files(root: &Path) -> Result<BTreeMap<PathBuf, u64>, String> {
    let files =
        walk_files(root).map_err(|err| format!("failed to walk {}: {err}", root.display()))?;
    let mut map = BTreeMap::new();
    for (path, size) in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| format!("{} is not under {}", path.display(), root.display()))?
            .to_path_buf();
        map.insert(relative, size);
    }
    Ok(map)
}

pub(super) fn files_equal(left: &Path, right: &Path) -> Result<bool, String> {
    let mut left = BufReader::new(
        File::open(left).map_err(|err| format!("failed to open {}: {err}", left.display()))?,
    );
    let mut right = BufReader::new(
        File::open(right).map_err(|err| format!("failed to open {}: {err}", right.display()))?,
    );
    let mut left_buffer = [0u8; 64 * 1024];
    let mut right_buffer = [0u8; 64 * 1024];

    loop {
        let left_read = read_full(&mut left, &mut left_buffer)?;
        let right_read = read_full(&mut right, &mut right_buffer)?;
        if left_read != right_read {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
        if left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
    }
}

pub(super) fn read_full(reader: &mut impl Read, buffer: &mut [u8]) -> Result<usize, String> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("read failed: {err}")),
        }
    }
    Ok(filled)
}

/// Recursive copy using only `std::fs`.
pub(super) fn copy_dir(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Child process management
// ---------------------------------------------------------------------------
