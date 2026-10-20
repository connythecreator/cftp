#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Scan {
        files: u64,
        bytes: u64,
    },
    Sending {
        bytes_done: u64,
        bytes_total: u64,
        files_done: u64,
        files_total: u64,
    },
    Receiving {
        bytes_done: u64,
        bytes_total: u64,
        files_done: u64,
        files_total: u64,
    },
    Complete {
        files: u64,
        bytes: u64,
    },
}

pub type ProgressSender = tokio::sync::mpsc::UnboundedSender<ProgressEvent>;
