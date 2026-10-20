#[derive(Debug, Clone)]
pub struct TransferProgress {
    pub at: std::time::Instant,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub files_done: u64,
    pub files_total: u64,
    pub read_bytes: u64,
    pub sent_bytes: u64,
    pub received_bytes: u64,
    pub written_bytes: u64,
    pub remote_elapsed_millis: u64,
}

#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Scan { files: u64, bytes: u64 },
    Sending(TransferProgress),
    Receiving(TransferProgress),
    Complete { files: u64, bytes: u64 },
}

pub type ProgressSender = tokio::sync::mpsc::UnboundedSender<ProgressEvent>;
