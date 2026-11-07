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

/// Compresses a payload the caller already owns.
///
/// The buffer is taken by value so the cases that keep the input (empty,
/// incompressible, or larger after compression) can return it without a copy.
pub fn compress(data: Vec<u8>, level: i32, allow_skip: bool) -> Result<(Codec, Vec<u8>)> {
    if data.is_empty() {
        return Ok((Codec::Raw, data));
    }
    // Compressing data that zstd cannot shrink is pure CPU cost: the result is
    // discarded and the original is sent. Sampling a few windows is far
    // cheaper than a full level-1 pass over a large chunk and catches random
    // payloads, encrypted archives, and already-compressed media that do not
    // have a recognizable extension.
    if allow_skip && looks_incompressible(&data) {
        return Ok((Codec::Raw, data));
    }
    let compressed = zstd::bulk::compress(&data, level)?;
    if compressed.len() < data.len() {
        Ok((Codec::Zstd, compressed))
    } else if allow_skip {
        Ok((Codec::Raw, data))
    } else {
        Ok((Codec::Zstd, compressed))
    }
}

const PROBE_BYTES: usize = 32 * 1024;
const PROBE_WINDOWS: usize = 3;
/// Samples that compress to at least this percentage are treated as
/// incompressible. Random data lands just above 100% (zstd framing).
const INCOMPRESSIBLE_PERCENT: usize = 97;

/// Cheaply estimates whether `data` can be compressed.
///
/// Three 32 KiB windows are compressed at level 1. A payload with no
/// compressible stretch is sent raw, avoiding a full pass whose output would
/// have been thrown away. Small payloads are always attempted: the probe would
/// cost a meaningful fraction of them and the sample would be the whole input.
fn looks_incompressible(data: &[u8]) -> bool {
    if data.len() < PROBE_BYTES * 2 {
        return false;
    }
    let last_start = data.len() - PROBE_BYTES;
    let mut input = 0usize;
    let mut output = 0usize;
    for window in 0..PROBE_WINDOWS {
        let start = last_start * window / (PROBE_WINDOWS - 1);
        let sample = &data[start..start + PROBE_BYTES];
        let Ok(compressed) = zstd::bulk::compress(sample, 1) else {
            return false;
        };
        input += sample.len();
        output += compressed.len();
    }
    output.saturating_mul(100) >= input.saturating_mul(INCOMPRESSIBLE_PERCENT)
}

/// Decodes a payload that the caller already owns.
///
/// Taking the buffer by value lets the raw case reuse the received bytes
/// instead of copying a whole chunk; a large transfer would otherwise pay an
/// extra full-payload copy at every hop.
pub fn decompress(codec: Codec, data: Vec<u8>, expected_len: usize) -> Result<Vec<u8>> {
    match codec {
        Codec::Raw => {
            if data.len() != expected_len {
                bail!(
                    "raw payload length mismatch: expected {expected_len}, got {}",
                    data.len()
                );
            }
            Ok(data)
        }
        Codec::Zstd => {
            let out = zstd::bulk::decompress(&data, expected_len)?;
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
        let (codec, compressed) = compress(data.clone(), PACK_LEVEL, true).unwrap();
        assert_eq!(codec, Codec::Zstd);
        assert!(compressed.len() < data.len());
        assert_eq!(decompress(codec, compressed, data.len()).unwrap(), data);
    }

    #[test]
    fn skips_incompressible_extensions() {
        assert!(extension_is_incompressible("image.PNG"));
        assert!(!extension_is_incompressible("src/main.js"));
    }

    #[test]
    fn skips_incompressible_payloads_without_a_known_extension() {
        let mut state = 0x1234_5678_9abc_def0u64;
        let data: Vec<u8> = (0..200_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 32) as u8
            })
            .collect();
        let (codec, encoded) = compress(data.clone(), 1, true).unwrap();
        assert_eq!(codec, Codec::Raw);
        assert_eq!(encoded, data);
    }

    #[test]
    fn still_compresses_compressible_payloads() {
        let mut data = Vec::new();
        for index in 0..4000 {
            data.extend_from_slice(format!("line {index}: the quick brown fox\n").as_bytes());
        }
        let (codec, encoded) = compress(data.clone(), 1, true).unwrap();
        assert_eq!(codec, Codec::Zstd);
        assert!(encoded.len() < data.len() / 2);
        assert_eq!(decompress(codec, encoded, data.len()).unwrap(), data);
    }
}
