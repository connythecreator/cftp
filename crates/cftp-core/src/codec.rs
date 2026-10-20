use anyhow::{Result, bail};
use cftp_proto::Codec;

pub const CHUNK_LEVEL: i32 = 1;
pub const PACK_LEVEL: i32 = 3;

const INCOMPRESSIBLE_EXTENSIONS: &[&str] = &[
    "7z", "avi", "bmp", "bz2", "dll", "dylib", "exe", "flac", "gif", "gz", "ico", "jar", "jpeg",
    "jpg", "m4a", "mkv", "mov", "mp3", "mp4", "node", "ogg", "otf", "png", "rar", "so", "tar",
    "tgz", "ttf", "wasm", "webm", "webp", "woff", "woff2", "xz", "zip",
];

pub fn extension_is_incompressible(path: &str) -> bool {
    let extension = path
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase());
    matches!(extension.as_deref(), Some(extension) if INCOMPRESSIBLE_EXTENSIONS.contains(&extension))
}

pub fn compress(data: &[u8], level: i32, allow_skip: bool) -> Result<(Codec, Vec<u8>)> {
    if data.is_empty() {
        return Ok((Codec::Raw, Vec::new()));
    }
    let compressed = zstd::bulk::compress(data, level)?;
    if compressed.len() < data.len() {
        Ok((Codec::Zstd, compressed))
    } else if allow_skip {
        Ok((Codec::Raw, data.to_vec()))
    } else {
        Ok((Codec::Zstd, compressed))
    }
}

pub fn decompress(codec: Codec, data: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    match codec {
        Codec::Raw => {
            if data.len() != expected_len {
                bail!(
                    "raw payload length mismatch: expected {expected_len}, got {}",
                    data.len()
                );
            }
            Ok(data.to_vec())
        }
        Codec::Zstd => {
            let out = zstd::bulk::decompress(data, expected_len)?;
            if out.len() != expected_len {
                bail!(
                    "decompressed length mismatch: expected {expected_len}, got {}",
                    out.len()
                );
            }
            Ok(out)
        }
    }
}

pub fn hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_compressed_data() {
        let data = vec![b'a'; 100_000];
        let (codec, compressed) = compress(&data, PACK_LEVEL, true).unwrap();
        assert_eq!(codec, Codec::Zstd);
        assert!(compressed.len() < data.len());
        assert_eq!(decompress(codec, &compressed, data.len()).unwrap(), data);
    }

    #[test]
    fn skips_incompressible_extensions() {
        assert!(extension_is_incompressible("image.PNG"));
        assert!(!extension_is_incompressible("src/main.js"));
    }
}
