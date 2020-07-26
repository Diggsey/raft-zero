use std::time::Duration;

pub struct Config {
    pub(crate) heartbeat_interval: Duration,
    pub(crate) election_timeout: Duration,
    pub(crate) max_replication_buffer_len: u64,
    pub(crate) max_append_entries_len: u64,
}
