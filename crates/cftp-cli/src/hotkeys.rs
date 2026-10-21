use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use cftp_core::tuning::HardwareProfile;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal;

use tokio::sync::watch;

pub struct Hotkeys {
    pub receiver: watch::Receiver<HardwareProfile>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Hotkeys {
    pub fn start(initial: HardwareProfile, quiet: bool) -> Result<Option<Self>> {
        if quiet || !std::io::stdin().is_terminal() {
            return Ok(None);
        }
        if let Err(err) = terminal::enable_raw_mode() {
            tracing::warn!("profile hotkeys disabled: {err}");
            return Ok(None);
        }
        let (tx, rx): (
            watch::Sender<HardwareProfile>,
            watch::Receiver<HardwareProfile>,
        ) = watch::channel(initial);
        let stop: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let thread_stop: Arc<AtomicBool> = stop.clone();
        let handle: thread::JoinHandle<()> = thread::spawn(move || -> () {
            while !thread_stop.load(Ordering::Relaxed) {
                let polled: std::io::Result<bool> = event::poll(Duration::from_millis(200));
                match polled {
                    Ok(true) => {
                        let read_event: std::io::Result<Event> = event::read();
                        match read_event {
                            Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                                if key.modifiers.contains(KeyModifiers::CONTROL)
                                    && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
                                {
                                    let _disable: std::io::Result<()> =
                                        terminal::disable_raw_mode();
                                    std::process::exit(130);
                                }
                                let profile: Option<HardwareProfile> = match key.code {
                                    KeyCode::Char('l') | KeyCode::Char('L') => {
                                        Some(HardwareProfile::Light)
                                    }
                                    KeyCode::Char('d') | KeyCode::Char('D') => {
                                        Some(HardwareProfile::Default)
                                    }
                                    KeyCode::Char('a') | KeyCode::Char('A') => {
                                        Some(HardwareProfile::Aggressive)
                                    }
                                    _ => None,
                                };
                                if let Some(profile) = profile {
                                    let _sent: Result<(), _> = tx.send(profile);
                                }
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                    Ok(false) => {}
                    Err(_) => break,
                }
            }
        });
        Ok(Some(Self {
            receiver: rx,
            stop,
            handle: Some(handle),
        }))
    }
}

impl Drop for Hotkeys {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _disabled: std::io::Result<()> = terminal::disable_raw_mode();
        if let Some(handle) = self.handle.take() {
            let _joined: std::result::Result<(), Box<dyn std::any::Any + Send>> = handle.join();
        }
    }
}
