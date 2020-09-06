use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub(crate) heartbeat_interval: Duration,
    pub(crate) min_election_timeout: Duration,
    pub(crate) max_election_timeout: Duration,
    pub(crate) max_replication_buffer_len: u64,
    pub(crate) max_append_entries_len: u64,
    pub(crate) max_in_flight_requests: Option<u64>,
    pub(crate) snapshot_chunk_size: u64,
}

impl Config {
    pub fn new() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(100),
            min_election_timeout: Duration::from_millis(200),
            max_election_timeout: Duration::from_millis(400),
            max_replication_buffer_len: 256,
            max_append_entries_len: 64,
            max_in_flight_requests: Some(128),
            snapshot_chunk_size: 16 * 1024,
        }
    }
    pub fn set_heartbeat_interval(&mut self, value: Duration) -> &mut Self {
        self.heartbeat_interval = value;
        self
    }
    pub fn set_election_timeout(&mut self, min_value: Duration, max_value: Duration) -> &mut Self {
        assert!(max_value > min_value);
        self.min_election_timeout = min_value;
        self.max_election_timeout = max_value;
        self
    }
    pub fn set_max_replication_buffer_len(&mut self, value: u64) -> &mut Self {
        self.max_replication_buffer_len = value;
        self
    }
    pub fn set_max_append_entries_len(&mut self, value: u64) -> &mut Self {
        self.max_append_entries_len = value;
        self
    }
    pub fn set_max_in_flight_requests(&mut self, value: Option<u64>) -> &mut Self {
        self.max_in_flight_requests = value;
        self
    }
}
