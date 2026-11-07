//! Unit tests for the transfer engine.

use super::*;

#[test]
fn ranges_accept_out_of_order_and_adjacent_chunks() {
    let mut ranges = ReceivedRanges::default();
    ranges.insert(10, 5).unwrap();
    ranges.insert(0, 10).unwrap();
    assert!(ranges.covers(15));
}

#[test]
fn ranges_reject_overlapping_chunks() {
    let mut ranges = ReceivedRanges::default();
    ranges.insert(0, 5).unwrap();
    assert!(ranges.insert(3, 5).is_err());
}

#[test]
fn ranges_do_not_cover_gaps() {
    let mut ranges = ReceivedRanges::default();
    ranges.insert(0, 5).unwrap();
    ranges.insert(10, 5).unwrap();
    assert!(!ranges.covers(15));
    ranges.insert(5, 5).unwrap();
    assert!(ranges.covers(15));
}

#[test]
fn ranges_ignore_zero_length_chunks() {
    let mut ranges = ReceivedRanges::default();
    ranges.insert(5, 0).unwrap();
    assert!(!ranges.covers(10));
    ranges.insert(0, 10).unwrap();
    assert!(ranges.covers(10));
}

#[test]
fn ranges_reject_offset_overflow() {
    let mut ranges = ReceivedRanges::default();
    assert!(ranges.insert(u64::MAX, 2).is_err());
}

fn manifest_entry(id: u64) -> ManifestEntry {
    ManifestEntry {
        id,
        path: format!("file-{id}"),
        kind: EntryKind::File,
        size: 1,
        mode: 0o644,
        mtime: 0,
        link_target: None,
    }
}

#[test]
fn manifest_requires_dense_ids_starting_at_one() {
    assert!(validate_manifest(&[manifest_entry(1)]).is_ok());
    assert!(validate_manifest(&[manifest_entry(1), manifest_entry(2)]).is_ok());
    assert!(validate_manifest(&[manifest_entry(0)]).is_err());
    assert!(validate_manifest(&[manifest_entry(2)]).is_err());
    assert!(validate_manifest(&[manifest_entry(1), manifest_entry(3)]).is_err());
    assert!(validate_manifest(&[manifest_entry(1), manifest_entry(1)]).is_err());
}

#[test]
fn manifest_rejects_oversized_declarations() {
    let mut entry: ManifestEntry = manifest_entry(1);
    entry.size = cftp_proto::MAX_FILE_SIZE + 1;
    assert!(validate_manifest(&[entry]).is_err());

    let mut entry: ManifestEntry = manifest_entry(1);
    entry.path = "a".repeat(cftp_proto::MAX_MANIFEST_PATH_LEN + 1);
    assert!(validate_manifest(&[entry]).is_err());
}

#[test]
fn manifest_rejects_duplicate_paths() {
    let mut second: ManifestEntry = manifest_entry(2);
    second.path = manifest_entry(1).path;
    assert!(validate_manifest(&[manifest_entry(1), second]).is_err());
}
