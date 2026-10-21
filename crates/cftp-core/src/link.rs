use std::net::IpAddr;
use std::time::{Duration, Instant};

use cftp_proto::HardwareProfile;

pub fn detect_link_speed(peer: IpAddr) -> Option<u64> {
    if peer.is_loopback() {
        return None;
    }

    let interfaces: Vec<netdev::Interface> = netdev::get_interfaces();
    let interface: netdev::Interface = interfaces
        .into_iter()
        .find(|interface: &netdev::Interface| -> bool {
            match peer {
                IpAddr::V4(ip) => interface
                    .ipv4
                    .iter()
                    .any(|net: &netdev::ipnet::Ipv4Net| -> bool { net.contains(&ip) }),
                IpAddr::V6(ip) => interface
                    .ipv6
                    .iter()
                    .any(|net: &netdev::ipnet::Ipv6Net| -> bool { net.contains(&ip) }),
            }
        })
        .or_else(|| -> Option<netdev::Interface> { netdev::get_default_interface().ok() })?;
    if interface.is_loopback() {
        return None;
    }
    interface
        .transmit_speed
        .or(interface.receive_speed)
        .filter(|speed: &u64| -> bool { *speed > 0 })
}

pub fn compression_levels(profile: HardwareProfile, link_speed: Option<u64>) -> (i32, i32) {
    match profile {
        HardwareProfile::Light => (1, 1),
        HardwareProfile::Default => (1, 3),
        HardwareProfile::Aggressive => match link_speed {
            Some(speed) if speed >= 10_000_000_000 => (1, 1),
            Some(speed) if speed >= 1_000_000_000 => (1, 3),
            Some(_) => (3, 6),
            None => (1, 3),
        },
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NetworkSample {
    pub link_speed: Option<u64>,
    pub wire_bps: f64,
    pub rtt: Duration,
    pub min_rtt: Duration,
    pub lost_bytes: u64,
    pub sent_bytes: u64,
}

#[derive(Debug)]
pub struct NetworkOptimizer {
    chunk_level: i32,
    pack_level: i32,
    pressure_samples: u8,
    relax_samples: u8,
    last_change: Instant,
}

impl NetworkOptimizer {
    pub fn new(chunk_level: i32, pack_level: i32, now: Instant) -> Self {
        Self {
            chunk_level,
            pack_level,
            pressure_samples: 0,
            relax_samples: 0,
            last_change: now,
        }
    }

    pub fn levels(&self) -> (i32, i32) {
        (self.chunk_level, self.pack_level)
    }

    pub fn observe(&mut self, sample: &NetworkSample, now: Instant) -> Option<(i32, i32)> {
        const COOLDOWN: Duration = Duration::from_secs(10);

        let loss_rate = if sample.sent_bytes > 0 {
            sample.lost_bytes as f64 / sample.sent_bytes as f64
        } else {
            0.0
        };
        let rtt_ratio = if sample.min_rtt > Duration::ZERO {
            sample.rtt.as_secs_f64() / sample.min_rtt.as_secs_f64()
        } else {
            1.0
        };
        let utilization = sample
            .link_speed
            .filter(|speed: &u64| -> bool { *speed > 0 })
            .map(|speed| sample.wire_bps / speed as f64)
            .unwrap_or(0.0);

        let pressure = loss_rate > 0.01 || rtt_ratio > 1.4 || utilization > 0.9;
        let relax = !pressure
            && loss_rate < 0.001
            && rtt_ratio < 1.15
            && (sample.link_speed.is_none() || utilization < 0.6);

        if pressure {
            self.pressure_samples = self.pressure_samples.saturating_add(1);
            self.relax_samples = 0;
        } else if relax {
            self.relax_samples = self.relax_samples.saturating_add(1);
            self.pressure_samples = 0;
        } else {
            self.pressure_samples = 0;
            self.relax_samples = 0;
        }

        if now.duration_since(self.last_change) < COOLDOWN {
            return None;
        }

        if self.pressure_samples >= 2 && (self.chunk_level < 9 || self.pack_level < 9) {
            let previous: (i32, i32) = self.levels();
            self.chunk_level = (self.chunk_level + 1).min(9);
            self.pack_level = (self.pack_level + 1).min(9);
            self.pressure_samples = 0;
            self.last_change = now;
            return (previous != self.levels()).then_some(self.levels());
        }

        if self.relax_samples >= 2 && (self.chunk_level > 1 || self.pack_level > 1) {
            let previous: (i32, i32) = self.levels();
            self.chunk_level = (self.chunk_level - 1).max(1);
            self.pack_level = (self.pack_level - 1).max(1);
            self.relax_samples = 0;
            self.last_change = now;
            return (previous != self.levels()).then_some(self.levels());
        }

        None
    }

    pub fn reset(&mut self, chunk_level: i32, pack_level: i32, now: Instant) {
        self.chunk_level = chunk_level;
        self.pack_level = pack_level;
        self.pressure_samples = 0;
        self.relax_samples = 0;
        self.last_change = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_link_speeds_to_levels() {
        assert_eq!(
            compression_levels(HardwareProfile::Aggressive, Some(10_000_000_000)),
            (1, 1)
        );
        assert_eq!(
            compression_levels(HardwareProfile::Aggressive, Some(1_000_000_000)),
            (1, 3)
        );
        assert_eq!(
            compression_levels(HardwareProfile::Aggressive, Some(100_000_000)),
            (3, 6)
        );
        assert_eq!(
            compression_levels(HardwareProfile::Aggressive, None),
            (1, 3)
        );
        assert_eq!(
            compression_levels(HardwareProfile::Light, Some(10_000_000_000)),
            (1, 1)
        );
        assert_eq!(
            compression_levels(HardwareProfile::Default, Some(100_000_000)),
            (1, 3)
        );
    }

    #[test]
    fn raises_compression_under_pressure() {
        let now: Instant = Instant::now();
        let mut optimizer: NetworkOptimizer =
            NetworkOptimizer::new(1, 3, now - Duration::from_secs(20));
        let sample: NetworkSample = NetworkSample {
            link_speed: Some(1_000_000_000),
            wire_bps: 950_000_000.0,
            rtt: Duration::from_millis(20),
            min_rtt: Duration::from_millis(10),
            lost_bytes: 0,
            sent_bytes: 1000,
        };
        assert_eq!(optimizer.observe(&sample, now), None);
        assert_eq!(
            optimizer.observe(&sample, now + Duration::from_secs(1)),
            Some((2, 4))
        );
    }

    #[test]
    fn lowers_compression_when_idle() {
        let now: Instant = Instant::now();
        let mut optimizer: NetworkOptimizer =
            NetworkOptimizer::new(3, 6, now - Duration::from_secs(20));
        let sample: NetworkSample = NetworkSample {
            link_speed: Some(1_000_000_000),
            wire_bps: 50_000_000.0,
            rtt: Duration::from_millis(10),
            min_rtt: Duration::from_millis(10),
            lost_bytes: 0,
            sent_bytes: 1000,
        };
        assert_eq!(optimizer.observe(&sample, now), None);
        assert_eq!(
            optimizer.observe(&sample, now + Duration::from_secs(1)),
            Some((2, 5))
        );
    }

    #[test]
    fn respects_cooldown_and_bounds() {
        let now: Instant = Instant::now();
        let mut optimizer: NetworkOptimizer = NetworkOptimizer::new(9, 9, now);
        let sample: NetworkSample = NetworkSample {
            link_speed: Some(1_000_000_000),
            wire_bps: 1_000_000_000.0,
            rtt: Duration::from_millis(50),
            min_rtt: Duration::from_millis(10),
            lost_bytes: 100,
            sent_bytes: 1000,
        };
        assert_eq!(optimizer.observe(&sample, now), None);
        assert_eq!(
            optimizer.observe(&sample, now + Duration::from_secs(1)),
            None
        );
        assert_eq!(
            optimizer.observe(&sample, now + Duration::from_secs(20)),
            None
        );
        assert_eq!(optimizer.levels(), (9, 9));
    }
}
