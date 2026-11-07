use std::sync::{Arc, RwLock};

pub use cftp_proto::HardwareProfile;
use tokio::sync::Semaphore;

use crate::link::compression_levels;

/// Backlog for encoded items waiting to be written to a data stream. This is a
/// queue bound, not a transport limit: how many sends may actually be in
/// flight is `Tuning::max_inflight_sends`.
pub const ENCODED_QUEUE_CAPACITY: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuning {
    pub profile: HardwareProfile,
    pub encode_workers: usize,
    pub chunk_size: usize,
    pub pack_target: usize,
    pub pack_max_files: usize,
    pub small_file_max: u64,
    /// Chunks fully read and waiting to be encoded. Peak read-ahead memory is
    /// `readahead_chunks * chunk_size`, plus one in-flight buffer per reader
    /// while it is blocked on a full queue.
    pub readahead_chunks: usize,
    /// Concurrent blocking readers used to fill the read-ahead queue.
    pub read_workers: usize,
    pub chunk_zstd_level: i32,
    pub pack_zstd_level: i32,
    pub max_inflight_sends: usize,
    pub max_inflight_streams: usize,
    pub max_concurrent_uni_streams: u32,
    pub max_concurrent_bi_streams: u32,
    pub receive_window: u64,
    pub stream_receive_window: u64,
    pub send_window: u64,
    /// Best-effort `SO_RCVBUF`/`SO_SNDBUF` for the UDP socket. The kernel
    /// clamps this to its `rmem_max`/`wmem_max`, so a value above the system
    /// limit is harmless.
    pub socket_buffer: usize,
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
        // Leave cores for the QUIC I/O drivers and the peer process. A 4 MiB
        // chunk is only a few milliseconds of hashing, so a pair of workers
        // already outruns a gigabit path; more workers on a small machine
        // mostly contend for CPU and delay draining the UDP socket.
        let encode_workers: usize = cores.div_ceil(2).clamp(1, 8);
        let default_readers: usize = (cores / 2).clamp(1, 3);
        let aggressive_readers: usize = (cores / 2).clamp(1, 5);

        match profile {
            HardwareProfile::Light => Self {
                profile,
                encode_workers: 1,
                chunk_size: 256 * 1024,
                pack_target: 1024 * 1024,
                pack_max_files: 32,
                small_file_max: 512 * 1024,
                readahead_chunks: 1,
                read_workers: 1,
                chunk_zstd_level,
                pack_zstd_level,
                max_inflight_sends: 2,
                max_inflight_streams: 4,
                max_concurrent_uni_streams: 8,
                max_concurrent_bi_streams: 8,
                receive_window: 8 * 1024 * 1024,
                stream_receive_window: 2 * 1024 * 1024,
                send_window: 8 * 1024 * 1024,
                socket_buffer: 1024 * 1024,
            },
            HardwareProfile::Default => Self {
                profile,
                encode_workers,
                chunk_size: 4 * 1024 * 1024,
                pack_target: 4 * 1024 * 1024,
                pack_max_files: 128,
                small_file_max: 2 * 1024 * 1024,
                readahead_chunks: 3,
                read_workers: default_readers,
                chunk_zstd_level,
                pack_zstd_level,
                max_inflight_sends: 32,
                max_inflight_streams: 32,
                max_concurrent_uni_streams: 64,
                max_concurrent_bi_streams: 64,
                receive_window: 64 * 1024 * 1024,
                stream_receive_window: 16 * 1024 * 1024,
                send_window: 64 * 1024 * 1024,
                socket_buffer: 4 * 1024 * 1024,
            },
            HardwareProfile::Aggressive => Self {
                profile,
                encode_workers,
                chunk_size: 4 * 1024 * 1024,
                pack_target: 16 * 1024 * 1024,
                pack_max_files: 256,
                small_file_max: 4 * 1024 * 1024,
                readahead_chunks: 8,
                read_workers: aggressive_readers,
                chunk_zstd_level,
                pack_zstd_level,
                max_inflight_sends: 32,
                max_inflight_streams: 32,
                max_concurrent_uni_streams: 64,
                max_concurrent_bi_streams: 64,
                receive_window: 64 * 1024 * 1024,
                stream_receive_window: 16 * 1024 * 1024,
                send_window: 64 * 1024 * 1024,
                socket_buffer: 8 * 1024 * 1024,
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
        let old: Tuning = self.snapshot();
        let mut new: Tuning = Tuning::for_profile(profile, link_speed);
        preserve_overrides(&mut new, &old, link_speed);
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

/// Carries explicit CLI overrides across a hotkey profile switch. A value is
/// treated as an override when it differs from the old profile's default.
fn preserve_overrides(new: &mut Tuning, old: &Tuning, link_speed: Option<u64>) {
    let defaults: Tuning = Tuning::for_profile(old.profile, link_speed);
    if old.chunk_size != defaults.chunk_size {
        new.chunk_size = old.chunk_size;
    }
    if old.pack_target != defaults.pack_target {
        new.pack_target = old.pack_target;
    }
    if old.pack_max_files != defaults.pack_max_files {
        new.pack_max_files = old.pack_max_files;
    }
    if old.small_file_max != defaults.small_file_max {
        new.small_file_max = old.small_file_max;
    }
}

fn resize_permits(permits: &Arc<Semaphore>, old: usize, new: usize) {
    if new > old {
        permits.add_permits(new - old);
    } else if old > new {
        // Permits currently held by encoding tasks cannot be forgotten yet.
        // Reclaim the shortfall as they are released; otherwise the semaphore
        // stays permanently over-provisioned after switching to a smaller
        // profile.
        let shortfall: usize = permits.forget_permits(old - new);
        if shortfall > 0
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let permits: Arc<Semaphore> = permits.clone();
            handle.spawn(async move {
                for _ in 0..shortfall {
                    let Ok(permit) = permits.acquire().await else {
                        break;
                    };
                    permit.forget();
                }
            });
        }
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
        assert_eq!(tuning.chunk_size, 4 * 1024 * 1024);
        assert_eq!(tuning.pack_target, 4 * 1024 * 1024);
        assert_eq!(tuning.pack_max_files, 128);
        assert_eq!(tuning.small_file_max, 2 * 1024 * 1024);
        assert_eq!(tuning.chunk_zstd_level, 1);
        assert_eq!(tuning.pack_zstd_level, 3);
        assert_eq!(tuning.max_inflight_sends, 32);
        assert_eq!(tuning.max_inflight_streams, 32);
        assert_eq!(tuning.socket_buffer, 4 * 1024 * 1024);
    }

    #[test]
    fn light_is_minimal_and_aggressive_is_larger() {
        let light: Tuning = Tuning::for_profile(HardwareProfile::Light, None);
        let aggressive: Tuning = Tuning::for_profile(HardwareProfile::Aggressive, None);
        assert_eq!(light.encode_workers, 1);
        assert!(light.chunk_size < aggressive.chunk_size);
        assert!(light.max_inflight_sends < aggressive.max_inflight_sends);
        assert!(light.receive_window < aggressive.receive_window);
        assert!(light.socket_buffer < aggressive.socket_buffer);
    }

    #[test]
    fn read_pool_scales_with_profile() {
        let cores: usize = std::thread::available_parallelism()
            .map(|count: std::num::NonZeroUsize| -> usize { count.get() })
            .unwrap_or(1);
        let light: Tuning = Tuning::for_profile(HardwareProfile::Light, None);
        let default_profile: Tuning = Tuning::for_profile(HardwareProfile::Default, None);
        let aggressive: Tuning = Tuning::for_profile(HardwareProfile::Aggressive, None);
        assert_eq!(light.readahead_chunks, 1);
        assert_eq!(default_profile.readahead_chunks, 3);
        assert_eq!(aggressive.readahead_chunks, 8);
        assert_eq!(light.read_workers, 1);
        // Reader pools leave cores for the QUIC drivers and the peer, and are
        // capped so a many-core host does not oversubscribe the socket.
        assert_eq!(default_profile.read_workers, (cores / 2).clamp(1, 3));
        assert_eq!(aggressive.read_workers, (cores / 2).clamp(1, 5));
        assert_eq!(
            default_profile.encode_workers,
            cores.div_ceil(2).clamp(1, 8)
        );
        assert_eq!(aggressive.encode_workers, cores.div_ceil(2).clamp(1, 8));
        assert!(aggressive.readahead_chunks >= default_profile.readahead_chunks);
        assert!(default_profile.readahead_chunks >= light.readahead_chunks);
        assert!(aggressive.read_workers >= default_profile.read_workers);
        assert!(default_profile.read_workers >= light.read_workers);
    }

    #[test]
    fn aggressive_keeps_default_transport_limits() {
        let default_profile: Tuning = Tuning::for_profile(HardwareProfile::Default, None);
        let aggressive: Tuning = Tuning::for_profile(HardwareProfile::Aggressive, None);
        // Oversized windows and more concurrent sends made the sender outrun
        // the receiver on loopback, so aggressive spends its extra capacity on
        // read-ahead and packing instead of on QUIC flow control.
        assert_eq!(
            aggressive.max_inflight_sends,
            default_profile.max_inflight_sends
        );
        assert_eq!(
            aggressive.max_inflight_streams,
            default_profile.max_inflight_streams
        );
        assert_eq!(aggressive.receive_window, default_profile.receive_window);
        assert_eq!(aggressive.send_window, default_profile.send_window);
        assert_eq!(
            aggressive.stream_receive_window,
            default_profile.stream_receive_window
        );
        assert!(aggressive.read_workers >= default_profile.read_workers);
        assert!(aggressive.socket_buffer > default_profile.socket_buffer);
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
        let aggressive: Tuning = Tuning::for_profile(HardwareProfile::Aggressive, None);
        assert_eq!(
            controller.permits().available_permits(),
            aggressive.encode_workers
        );
        controller.set_profile(HardwareProfile::Light, None);
        assert_eq!(controller.permits().available_permits(), 1);
    }
}
