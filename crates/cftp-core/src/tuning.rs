use std::sync::{Arc, RwLock};

pub use cftp_proto::HardwareProfile;
use tokio::sync::Semaphore;

use crate::link::compression_levels;

pub const MAX_INFLIGHT_SENDS: usize = 128;
pub const MAX_INFLIGHT_STREAMS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuning {
    pub profile: HardwareProfile,
    pub encode_workers: usize,
    pub chunk_size: usize,
    pub pack_target: usize,
    pub pack_max_files: usize,
    pub small_file_max: u64,
    pub chunk_zstd_level: i32,
    pub pack_zstd_level: i32,
    pub max_inflight_sends: usize,
    pub max_inflight_streams: usize,
    pub max_concurrent_uni_streams: u32,
    pub max_concurrent_bi_streams: u32,
    pub receive_window: u64,
    pub stream_receive_window: u64,
    pub send_window: u64,
}

impl Default for Tuning {
    fn default() -> Self {
        Self::for_profile(HardwareProfile::Default, None)
    }
}

impl Tuning {
    pub fn for_profile(profile: HardwareProfile, link_speed: Option<u64>) -> Self {
        let cores: usize = std::thread::available_parallelism()
            .map(|count: std::num::NonZeroUsize| -> usize { count.get() })
            .unwrap_or(1);
        let (chunk_zstd_level, pack_zstd_level): (i32, i32) =
            compression_levels(profile, link_speed);

        match profile {
            HardwareProfile::Light => Self {
                profile,
                encode_workers: 1,
                chunk_size: 256 * 1024,
                pack_target: 1024 * 1024,
                pack_max_files: 32,
                small_file_max: 512 * 1024,
                chunk_zstd_level,
                pack_zstd_level,
                max_inflight_sends: 2,
                max_inflight_streams: 4,
                max_concurrent_uni_streams: 8,
                max_concurrent_bi_streams: 8,
                receive_window: 8 * 1024 * 1024,
                stream_receive_window: 2 * 1024 * 1024,
                send_window: 8 * 1024 * 1024,
            },
            HardwareProfile::Default => Self {
                profile,
                encode_workers: cores,
                chunk_size: 1024 * 1024,
                pack_target: 4 * 1024 * 1024,
                pack_max_files: 128,
                small_file_max: 2 * 1024 * 1024,
                chunk_zstd_level,
                pack_zstd_level,
                max_inflight_sends: 32,
                max_inflight_streams: 32,
                max_concurrent_uni_streams: 64,
                max_concurrent_bi_streams: 64,
                receive_window: 64 * 1024 * 1024,
                stream_receive_window: 16 * 1024 * 1024,
                send_window: 64 * 1024 * 1024,
            },
            HardwareProfile::Aggressive => Self {
                profile,
                encode_workers: cores,
                chunk_size: 4 * 1024 * 1024,
                pack_target: 16 * 1024 * 1024,
                pack_max_files: 256,
                small_file_max: 4 * 1024 * 1024,
                chunk_zstd_level,
                pack_zstd_level,
                max_inflight_sends: MAX_INFLIGHT_SENDS,
                max_inflight_streams: MAX_INFLIGHT_STREAMS,
                max_concurrent_uni_streams: 256,
                max_concurrent_bi_streams: 256,
                receive_window: 256 * 1024 * 1024,
                stream_receive_window: 64 * 1024 * 1024,
                send_window: 256 * 1024 * 1024,
            },
        }
    }

    pub fn with_overrides(
        mut self,
        chunk_size: Option<usize>,
        pack_target: Option<usize>,
        pack_max_files: Option<usize>,
        small_file_max: Option<u64>,
    ) -> Self {
        if let Some(value) = chunk_size {
            self.chunk_size = value;
        }
        if let Some(value) = pack_target {
            self.pack_target = value;
        }
        if let Some(value) = pack_max_files {
            self.pack_max_files = value;
        }
        if let Some(value) = small_file_max {
            self.small_file_max = value;
        }
        self
    }
}

#[derive(Debug)]
pub struct TuningController {
    tuning: RwLock<Tuning>,
    permits: Arc<Semaphore>,
}

impl TuningController {
    pub fn new(tuning: Tuning) -> Arc<Self> {
        let permits: Arc<Semaphore> = Arc::new(Semaphore::new(tuning.encode_workers.max(1)));
        Arc::new(Self {
            tuning: RwLock::new(tuning),
            permits,
        })
    }

    pub fn snapshot(&self) -> Tuning {
        self.tuning.read().expect("tuning lock poisoned").clone()
    }

    pub fn permits(&self) -> &Arc<Semaphore> {
        &self.permits
    }

    pub fn set_profile(&self, profile: HardwareProfile, link_speed: Option<u64>) {
        let new: Tuning = Tuning::for_profile(profile, link_speed);
        let mut current: std::sync::RwLockWriteGuard<'_, Tuning> =
            self.tuning.write().expect("tuning lock poisoned");
        resize_permits(&self.permits, current.encode_workers, new.encode_workers);
        *current = new;
    }

    pub fn set_compression(&self, chunk_level: i32, pack_level: i32) {
        let mut current: std::sync::RwLockWriteGuard<'_, Tuning> =
            self.tuning.write().expect("tuning lock poisoned");
        current.chunk_zstd_level = chunk_level;
        current.pack_zstd_level = pack_level;
    }
}

fn resize_permits(permits: &Semaphore, old: usize, new: usize) {
    if new > old {
        permits.add_permits(new - old);
    } else if old > new {
        permits.forget_permits(old - new);
    }
}

pub fn apply_connection_tuning(connection: &quinn::Connection, tuning: &Tuning) {
    connection
        .set_max_concurrent_uni_streams(quinn::VarInt::from_u32(tuning.max_concurrent_uni_streams));
    connection
        .set_max_concurrent_bi_streams(quinn::VarInt::from_u32(tuning.max_concurrent_bi_streams));
    connection.set_send_window(tuning.send_window);
    connection.set_receive_window(
        quinn::VarInt::from_u64(tuning.receive_window).expect("valid receive window"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_matches_existing_tuning() {
        let tuning: Tuning = Tuning::for_profile(HardwareProfile::Default, None);
        assert_eq!(tuning.chunk_size, 1024 * 1024);
        assert_eq!(tuning.pack_target, 4 * 1024 * 1024);
        assert_eq!(tuning.pack_max_files, 128);
        assert_eq!(tuning.small_file_max, 2 * 1024 * 1024);
        assert_eq!(tuning.chunk_zstd_level, 1);
        assert_eq!(tuning.pack_zstd_level, 3);
        assert_eq!(tuning.max_inflight_sends, 32);
        assert_eq!(tuning.max_inflight_streams, 32);
    }

    #[test]
    fn light_is_minimal_and_aggressive_is_larger() {
        let light: Tuning = Tuning::for_profile(HardwareProfile::Light, None);
        let aggressive: Tuning = Tuning::for_profile(HardwareProfile::Aggressive, None);
        assert_eq!(light.encode_workers, 1);
        assert!(light.chunk_size < aggressive.chunk_size);
        assert!(light.max_inflight_sends < aggressive.max_inflight_sends);
        assert!(light.receive_window < aggressive.receive_window);
    }

    #[test]
    fn overrides_replace_profile_values() {
        let tuning: Tuning = Tuning::default().with_overrides(Some(2), Some(3), Some(4), Some(5));
        assert_eq!(tuning.chunk_size, 2);
        assert_eq!(tuning.pack_target, 3);
        assert_eq!(tuning.pack_max_files, 4);
        assert_eq!(tuning.small_file_max, 5);
    }

    #[test]
    fn controller_resizes_permits() {
        let controller: Arc<TuningController> =
            TuningController::new(Tuning::for_profile(HardwareProfile::Light, None));
        assert_eq!(controller.permits().available_permits(), 1);
        controller.set_profile(HardwareProfile::Aggressive, None);
        assert!(controller.permits().available_permits() > 1);
        controller.set_profile(HardwareProfile::Light, None);
        assert_eq!(controller.permits().available_permits(), 1);
    }
}
