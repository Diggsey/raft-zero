use std::time::Instant;

use act_zero::*;

use crate::messages::Membership;
use crate::types::{LogIndex, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservedState {
    pub node_id: NodeId,
    pub leader_id: Option<NodeId>,
    pub current_term: Term,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub committed_index: LogIndex,
    pub committed_term: Term,
    pub voted_for: Option<NodeId>,
    pub election_deadline: Option<Instant>,
}

#[act_zero]
pub trait Observer {
    fn observe_state(&self, _state: ObservedState) {}
    fn observe_membership(&self, _membership: Membership) {}
}
