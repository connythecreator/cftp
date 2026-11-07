use super::*;

pub(super) fn progress_channel(quiet: bool) -> (ProgressSender, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let bar: Option<ProgressBar> = if quiet {
            None
        } else {
            let bar = ProgressBar::new(0);
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg}",
                )
                .unwrap()
                .progress_chars("=>-"),
            );
            // Redraw on a timer so the spinner and elapsed time keep moving even
            // when the peer is between events.
            bar.enable_steady_tick(Duration::from_millis(250));
            Some(bar)
        };
        let mut tracker = RateTracker::new();
        let mut last_message = String::from("0 files");
        let mut last_event = Instant::now();
        let mut last_stall = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                event = rx.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    last_event = Instant::now();
                    last_stall = Instant::now();
                    match event {
                        ProgressEvent::Scan { files, bytes } => {
                            if let Some(bar) = &bar {
                                bar.set_length(bytes);
                            }
                            last_message = format!("{files} files");
                            if let Some(bar) = &bar {
                                bar.set_message(last_message.clone());
                            }
                        }
                        ProgressEvent::Sending(stats) | ProgressEvent::Receiving(stats) => {
                            if let Some(bar) = &bar {
                                bar.set_length(stats.bytes_total);
                                bar.set_position(stats.bytes_done);
                            }
                            let rates = tracker.update(&stats);
                            last_message = format_progress(&stats, &rates);
                            if let Some(bar) = &bar {
                                bar.set_message(last_message.clone());
                            }
                        }
                        ProgressEvent::Complete { .. } => {
                            if let Some(bar) = &bar {
                                bar.finish_with_message(format!("complete | {last_message}"));
                            }
                        }
                        ProgressEvent::Notice(message) => match &bar {
                            Some(bar) => bar.println(message),
                            None => eprintln!("{message}"),
                        },
                    }
                }
                _ = ticker.tick() => {
                    // Event-driven progress freezes completely when a peer goes
                    // quiet; say so instead of leaving a stale display.
                    if last_event.elapsed() >= Duration::from_secs(15)
                        && last_stall.elapsed() >= Duration::from_secs(15)
                    {
                        last_stall = Instant::now();
                        let message = format!(
                            "still working: no progress update for {}s",
                            last_event.elapsed().as_secs()
                        );
                        match &bar {
                            Some(bar) => bar.println(message),
                            None => eprintln!("{message}"),
                        }
                    }
                }
            }
        }
    });
    (tx, task)
}

#[derive(Clone, Copy, Default)]
struct Rates {
    read: f64,
    sent: f64,
    received: f64,
    written: f64,
}

struct RateTracker {
    last: Instant,
    read: u64,
    sent: u64,
    received: u64,
    written: u64,
    rates: Rates,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            last: Instant::now(),
            read: 0,
            sent: 0,
            received: 0,
            written: 0,
            rates: Rates::default(),
        }
    }

    fn update(&mut self, stats: &TransferProgress) -> Rates {
        let elapsed = stats
            .at
            .checked_duration_since(self.last)
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0);
        let remote = (stats.remote_elapsed_millis > 0).then(|| {
            let seconds = stats.remote_elapsed_millis as f64 / 1000.0;
            (
                stats.received_bytes as f64 / seconds,
                stats.written_bytes as f64 / seconds,
            )
        });

        if let Some((received, written)) = remote {
            self.rates.received = received;
            self.rates.written = written;
        }

        if elapsed < 0.05 {
            return self.rates;
        }

        let instant =
            |current: u64, previous: u64| current.saturating_sub(previous) as f64 / elapsed;
        let alpha = 0.4;
        let smooth = |old: f64, new: f64| {
            if old == 0.0 {
                new
            } else {
                old * (1.0 - alpha) + new * alpha
            }
        };

        self.rates.read = smooth(self.rates.read, instant(stats.read_bytes, self.read));
        self.rates.sent = smooth(self.rates.sent, instant(stats.sent_bytes, self.sent));
        if remote.is_none() {
            self.rates.received = smooth(
                self.rates.received,
                instant(stats.received_bytes, self.received),
            );
            self.rates.written = smooth(
                self.rates.written,
                instant(stats.written_bytes, self.written),
            );
        }
        self.last = stats.at;
        self.read = stats.read_bytes;
        self.sent = stats.sent_bytes;
        self.received = stats.received_bytes;
        self.written = stats.written_bytes;
        self.rates
    }
}

fn format_progress(stats: &TransferProgress, rates: &Rates) -> String {
    let mut parts = vec![format!("{}/{} files", stats.files_done, stats.files_total)];
    if stats.read_bytes > 0 {
        parts.push(format!("read {}", format_rate(rates.read)));
    }
    if stats.sent_bytes > 0 {
        parts.push(format!("sent {}", format_rate(rates.sent)));
    }
    if stats.received_bytes > 0 {
        parts.push(format!("recv {}", format_rate(rates.received)));
    }
    if stats.written_bytes > 0 {
        parts.push(format!("write {}", format_rate(rates.written)));
    }
    parts.join(" | ")
}

fn format_rate(bytes_per_second: f64) -> String {
    let bits = bytes_per_second * 8.0;
    if bits >= 1_000_000_000.0 {
        format!("{:.2} Gbps", bits / 1_000_000_000.0)
    } else if bits >= 1_000_000.0 {
        format!("{:.1} Mbps", bits / 1_000_000.0)
    } else if bits >= 1_000.0 {
        format!("{:.0} Kbps", bits / 1_000.0)
    } else {
        format!("{bits:.0} bps")
    }
}
