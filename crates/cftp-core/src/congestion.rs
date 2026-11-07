//! Adaptive congestion control.
//!
//! CUBIC is the right default on a clean path: it fills a fast LAN with little
//! queueing. On a lossy path, though, it treats random packet loss as
//! congestion and keeps collapsing its window; BBR, which estimates bandwidth
//! from delivery rate, keeps going. Neither is right everywhere, so this module
//! starts every connection on CUBIC and switches to BBR only after the path has
//! shown sustained loss.
//!
//! The switch is deliberately one-way: re-seeding CUBIC from BBR's much larger
//! window is not safe, and a path that drops packets does not become clean
//! again during one transfer.

use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::congestion::{BbrConfig, Controller, ControllerFactory, ControllerMetrics, CubicConfig};
use quinn_proto::RttEstimator;

/// A window must span at least this long before its loss ratio is judged.
const WINDOW: Duration = Duration::from_millis(250);
/// Ignore the first moments of a connection, where CUBIC's slow start can
/// overshoot a shallow queue once even on a healthy path.
const WARMUP: Duration = Duration::from_millis(250);
/// Do not judge a window that delivered less than this; the ratio would be
/// dominated by one or two packets.
const MIN_WINDOW_BYTES: u64 = 128 * 1024;
/// Packet loss above this fraction means CUBIC is being held back by loss
/// rather than by the link. A clean LAN measures zero; a Wi-Fi path that
/// benefits from BBR shows well above this.
const LOSS_THRESHOLD: f64 = 0.003;
/// Consecutive lossy windows required before switching. Two windows rule out a
/// single burst that is not representative of the path.
const LOSSY_WINDOWS_TO_SWITCH: u8 = 2;

/// Factory for [`AdaptiveController`]. Placed on every connection's transport
/// configuration by [`crate::identity::transport_config`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AdaptiveCongestionConfig;

impl ControllerFactory for AdaptiveCongestionConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(AdaptiveController {
            cubic: Arc::new(CubicConfig::default()).build(now, current_mtu),
            bbr: None,
            started: now,
            window_started: now,
            window_delivered: 0,
            window_lost: 0,
            lossy_windows: 0,
            mtu: current_mtu,
        })
    }
}

/// CUBIC until the path proves lossy, then BBR.
pub(crate) struct AdaptiveController {
    cubic: Box<dyn Controller>,
    bbr: Option<Box<dyn Controller>>,
    started: Instant,
    window_started: Instant,
    window_delivered: u64,
    window_lost: u64,
    lossy_windows: u8,
    mtu: u16,
}

impl AdaptiveController {
    fn active(&self) -> &dyn Controller {
        match &self.bbr {
            Some(bbr) => bbr.as_ref(),
            None => self.cubic.as_ref(),
        }
    }

    fn active_mut(&mut self) -> &mut dyn Controller {
        match &mut self.bbr {
            Some(bbr) => bbr.as_mut(),
            None => self.cubic.as_mut(),
        }
    }

    /// Records delivered bytes and returns whether the path looks lossy.
    fn observe_delivery(&mut self, now: Instant, bytes: u64) -> bool {
        self.window_delivered = self.window_delivered.saturating_add(bytes);
        self.evaluate_window(now)
    }

    fn observe_loss(&mut self, now: Instant, bytes: u64) -> bool {
        self.window_lost = self.window_lost.saturating_add(bytes);
        self.evaluate_window(now)
    }

    /// Closes the current measurement window once it is old and large enough,
    /// and switches the controller after enough consecutive lossy windows.
    /// Returns true if this call performed the switch.
    fn evaluate_window(&mut self, now: Instant) -> bool {
        if self.bbr.is_some() {
            return false;
        }
        if now.saturating_duration_since(self.started) < WARMUP {
            return false;
        }
        if now.saturating_duration_since(self.window_started) < WINDOW {
            return false;
        }
        let total: u64 = self.window_delivered.saturating_add(self.window_lost);
        if total < MIN_WINDOW_BYTES {
            // Too little traffic to judge; keep accumulating.
            return false;
        }

        let loss_rate: f64 = self.window_lost as f64 / total as f64;
        if loss_rate >= LOSS_THRESHOLD {
            self.lossy_windows = self.lossy_windows.saturating_add(1);
        } else {
            self.lossy_windows = 0;
        }
        self.window_delivered = 0;
        self.window_lost = 0;
        self.window_started = now;

        if self.lossy_windows < LOSSY_WINDOWS_TO_SWITCH {
            return false;
        }
        self.switch_to_bbr(now);
        true
    }

    fn switch_to_bbr(&mut self, now: Instant) {
        // Seed BBR with a window close to what the path has been carrying, so
        // the switch does not stall the transfer while it re-learns. The cap
        // stops a very large CUBIC window from turning into one huge burst.
        let seed: u64 = self
            .cubic
            .window()
            .clamp(10 * u64::from(self.mtu), 256 * 1024);
        let mut config: BbrConfig = BbrConfig::default();
        config.initial_window(seed);
        tracing::debug!(
            seed,
            mtu = self.mtu,
            "path is lossy; switching congestion control from CUBIC to BBR"
        );
        self.bbr = Some(Arc::new(config).build(now, self.mtu));
    }

    #[cfg(test)]
    fn uses_bbr(&self) -> bool {
        self.bbr.is_some()
    }
}

impl Controller for AdaptiveController {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.active_mut().on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.observe_delivery(now, bytes);
        self.active_mut().on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.active_mut()
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        if lost_bytes > 0 {
            self.observe_loss(now, lost_bytes);
        }
        self.active_mut()
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = new_mtu;
        self.cubic.on_mtu_update(new_mtu);
        if let Some(bbr) = &mut self.bbr {
            bbr.on_mtu_update(new_mtu);
        }
    }

    fn window(&self) -> u64 {
        self.active().window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.active().metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            cubic: self.cubic.clone_box(),
            bbr: self.bbr.as_ref().map(|bbr| bbr.clone_box()),
            started: self.started,
            window_started: self.window_started,
            window_delivered: self.window_delivered,
            window_lost: self.window_lost,
            lossy_windows: self.lossy_windows,
            mtu: self.mtu,
        })
    }

    fn initial_window(&self) -> u64 {
        self.active().initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controller() -> AdaptiveController {
        let config: Arc<AdaptiveCongestionConfig> = Arc::new(AdaptiveCongestionConfig);
        let built: Box<dyn Controller> = config.build(Instant::now(), 1452);
        let any: Box<dyn std::any::Any> = built.into_any();
        *any.downcast::<AdaptiveController>()
            .expect("factory builds an adaptive controller")
    }

    /// Simulates one batch of acknowledgements and losses. A window is judged
    /// when the next batch arrives a window later.
    fn batch(controller: &mut AdaptiveController, at: Instant, delivered: u64, lost: u64) {
        controller.observe_delivery(at, delivered);
        controller.observe_loss(at, lost);
    }

    #[test]
    fn stays_on_cubic_for_a_clean_path() {
        let now: Instant = Instant::now();
        let mut controller: AdaptiveController = controller();
        for step in 0..20u32 {
            batch(
                &mut controller,
                now + WARMUP + WINDOW * step,
                4 * 1024 * 1024,
                0,
            );
        }
        assert!(!controller.uses_bbr());
    }

    #[test]
    fn switches_after_sustained_loss() {
        let now: Instant = Instant::now();
        let mut controller: AdaptiveController = controller();
        // ~1.2% loss in every batch. The first batch closes the clean warm-up
        // window; the next two close lossy windows.
        let step = |i: u32| now + WARMUP + WINDOW * i;
        batch(&mut controller, step(0), 4 * 1024 * 1024, 50 * 1024);
        batch(&mut controller, step(1), 4 * 1024 * 1024, 50 * 1024);
        assert!(!controller.uses_bbr());
        batch(&mut controller, step(2), 4 * 1024 * 1024, 50 * 1024);
        assert!(controller.uses_bbr());
        assert!(controller.window() >= 4 * 1452);
    }

    #[test]
    fn a_single_lossy_window_is_not_enough() {
        let now: Instant = Instant::now();
        let mut controller: AdaptiveController = controller();
        let step = |i: u32| now + WARMUP + WINDOW * i;
        batch(&mut controller, step(0), 4 * 1024 * 1024, 50 * 1024);
        batch(&mut controller, step(1), 4 * 1024 * 1024, 0);
        batch(&mut controller, step(2), 4 * 1024 * 1024, 0);
        assert!(!controller.uses_bbr());
    }

    #[test]
    fn low_loss_stays_on_cubic() {
        let now: Instant = Instant::now();
        let mut controller: AdaptiveController = controller();
        // 0.2% loss in every window is below the threshold.
        for step in 0..10u32 {
            batch(
                &mut controller,
                now + WARMUP + WINDOW * step,
                4 * 1024 * 1024,
                8 * 1024,
            );
        }
        assert!(!controller.uses_bbr());
    }

    #[test]
    fn tiny_windows_are_not_judged() {
        let now: Instant = Instant::now();
        let mut controller: AdaptiveController = controller();
        // Every window is heavily lossy but far too small to be meaningful.
        for step in 0..20u32 {
            batch(
                &mut controller,
                now + WARMUP + WINDOW * step,
                8 * 1024,
                1024,
            );
        }
        assert!(!controller.uses_bbr());
    }
}
