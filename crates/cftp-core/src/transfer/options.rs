//! Request and statistics types shared by the client and server.

use super::*;

/// Client-side transfer settings.
///
/// `destination` is the path relative to the server's root. Build one with
/// [`PushOptions::default`] and change only the fields you need.
#[derive(Debug, Clone)]
pub struct PushOptions {
    pub destination: String,
    pub server_name: String,
    pub progress: Option<ProgressSender>,
    pub profile: HardwareProfile,
    pub tuning: Tuning,
    pub profile_flag_used: bool,
    pub force_v2: bool,
    pub profile_requests: Option<watch::Receiver<HardwareProfile>>,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            destination: String::new(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            progress: None,
            profile: HardwareProfile::Default,
            tuning: Tuning::default(),
            profile_flag_used: false,
            force_v2: false,
            profile_requests: None,
        }
    }
}

impl PushOptions {
    /// Starts a push into `destination`, a path relative to the server root.
    pub fn new(destination: impl Into<String>) -> Self {
        Self {
            destination: destination.into(),
            ..Self::default()
        }
    }

    /// Delivers [`ProgressEvent`]s to `progress` while the transfer runs.
    ///
    /// Create a channel with [`crate::progress::channel`].
    pub fn with_progress(mut self, progress: ProgressSender) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Requests `profile` and uses that profile's default tuning.
    ///
    /// The server may cap the request; the transfer continues at the effective
    /// profile reported in [`PushStats::effective_profile`].
    pub fn with_profile(mut self, profile: HardwareProfile) -> Self {
        self.profile = profile;
        self.tuning = Tuning::for_profile(profile, None);
        self
    }
}

/// Transport-level counters sampled when a transfer finishes.
///
/// These describe the QUIC path, not the files: they are what `--stats` prints
/// and what an adaptive optimisation needs in order to tell a slow link from a
/// self-inflicted bottleneck.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportStats {
    /// Smoothed round-trip time.
    pub rtt_micros: u64,
    /// Lowest round-trip time observed on this path.
    pub min_rtt_micros: u64,
    /// Congestion window in bytes.
    pub cwnd: u64,
    /// Path MTU in bytes.
    pub mtu: u16,
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub congestion_events: u64,
    pub udp_tx_datagrams: u64,
    pub udp_rx_datagrams: u64,
    pub udp_tx_bytes: u64,
    pub udp_rx_bytes: u64,
}

impl TransportStats {
    /// Samples the connection before it is closed.
    pub(super) fn sample(connection: &Connection) -> Self {
        let stats = connection.stats();
        let path = stats.path;
        Self {
            rtt_micros: u64::try_from(path.rtt.as_micros()).unwrap_or(u64::MAX),
            min_rtt_micros: u64::try_from(path.min_rtt.as_micros()).unwrap_or(u64::MAX),
            cwnd: path.cwnd,
            mtu: path.current_mtu,
            sent_packets: path.sent_packets,
            lost_packets: path.lost_packets,
            congestion_events: path.congestion_events,
            udp_tx_datagrams: stats.udp_tx.datagrams,
            udp_rx_datagrams: stats.udp_rx.datagrams,
            udp_tx_bytes: stats.udp_tx.bytes,
            udp_rx_bytes: stats.udp_rx.bytes,
        }
    }
}

/// Counters from one completed client-side transfer.
#[derive(Debug, Clone, Default)]
pub struct PushStats {
    pub files: u64,
    pub bytes: u64,
    pub packs: u64,
    pub chunks: u64,
    pub effective_profile: HardwareProfile,
    /// Transport counters sampled when the transfer finished.
    pub transport: TransportStats,
}

/// Client-side pull settings.
///
/// `source` is the path relative to the server root, and `destination` is the
/// local directory that received entries are written under. A trailing slash on
/// `source` pulls a directory's contents instead of the directory itself,
/// mirroring [`PushOptions::destination`] on the push side.
#[derive(Debug, Clone)]
pub struct PullOptions {
    pub source: String,
    pub destination: PathBuf,
    pub server_name: String,
    pub progress: Option<ProgressSender>,
    pub profile: HardwareProfile,
    pub tuning: Tuning,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            source: String::new(),
            destination: PathBuf::new(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
            progress: None,
            profile: HardwareProfile::Default,
            tuning: Tuning::default(),
        }
    }
}

impl PullOptions {
    /// Pulls `source` (a path under the server root) into `destination`.
    ///
    /// `destination` is created if it does not exist.
    pub fn new(source: impl Into<String>, destination: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            ..Self::default()
        }
    }

    /// Delivers [`ProgressEvent`]s to `progress` while the transfer runs.
    ///
    /// Create a channel with [`crate::progress::channel`].
    pub fn with_progress(mut self, progress: ProgressSender) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Requests `profile` and uses that profile's default tuning.
    ///
    /// The server may cap the request; the transfer continues at the effective
    /// profile reported in [`PullStats::effective_profile`].
    pub fn with_profile(mut self, profile: HardwareProfile) -> Self {
        self.profile = profile;
        self.tuning = Tuning::for_profile(profile, None);
        self
    }
}

/// Counters from one completed client-side pull.
#[derive(Debug, Clone, Default)]
pub struct PullStats {
    pub files: u64,
    pub bytes: u64,
    /// Payload bytes received over the wire, before decompression.
    pub wire_bytes: u64,
    pub effective_profile: HardwareProfile,
    /// Transport counters sampled when the transfer finished.
    pub transport: TransportStats,
}
