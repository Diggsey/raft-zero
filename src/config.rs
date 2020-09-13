use std::time::Duration;

#[derive(Debug)]
#[non_exhaustive]
pub enum MembershipChangeCond {
    /// Sufficient nodes from the target configuration must be
    /// up-to-date such that the requested fault tolerance can be
    /// respected.
    MinimumUpToDate,
    /// In addition to the minimum requirements, all new nodes must
    /// be up-to-date.
    NewUpToDate,
    /// All nodes in the target configuration must be up-to-date.
    AllUpToDate,
}

#[derive(Debug)]
pub struct Config {
    pub(crate) heartbeat_interval: Duration,
    pub(crate) min_election_timeout: Duration,
    pub(crate) max_election_timeout: Duration,
    pub(crate) max_replication_buffer_len: u64,
    pub(crate) max_append_entries_len: u64,
    pub(crate) max_in_flight_requests: Option<u64>,
    pub(crate) snapshot_chunk_size: u64,
    pub(crate) pre_vote: bool,
    pub(crate) leader_stickiness: bool,
    pub(crate) membership_change_cond: MembershipChangeCond,
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
            pre_vote: true,
            leader_stickiness: true,
            membership_change_cond: MembershipChangeCond::NewUpToDate,
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
    pub fn set_pre_vote(&mut self, value: bool) -> &mut Self {
        self.pre_vote = value;
        self
    }
    pub fn set_leader_stickiness(&mut self, value: bool) -> &mut Self {
        self.leader_stickiness = value;
        self
    }
    pub fn set_membership_change_cond(&mut self, value: MembershipChangeCond) -> &mut Self {
        self.membership_change_cond = value;
        self
    }
}
