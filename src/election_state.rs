use std::collections::HashSet;

use crate::messages::{Membership, VotingGroup};
use crate::types::NodeId;

// Counts votes within a single voting group
#[derive(Clone)]
struct MajorityCounter {
    group: VotingGroup,
    votes_received: HashSet<NodeId>,
    has_majority: bool,
}

impl MajorityCounter {
    fn add_vote(&mut self, from: NodeId) -> bool {
        if self.group.members.contains(&from) {
            self.votes_received.insert(from);
            if self.votes_received.len() > self.group.members.len() / 2 {
                self.has_majority = true;
            }
        }
        self.has_majority
    }
    fn new(group: VotingGroup) -> Self {
        Self {
            group,
            votes_received: HashSet::new(),
            has_majority: false,
        }
    }
}

pub(crate) struct ElectionState {
    // One vote counter per voting group
    vote_counters: Vec<MajorityCounter>,
}

impl ElectionState {
    // Returns true if we gained a majority
    pub(crate) fn add_vote(&mut self, from: NodeId) -> bool {
        // Record the vote separately for each cluster
        let mut has_majority = true;
        for counter in &mut self.vote_counters {
            has_majority &= counter.add_vote(from);
        }
        // Return true if reached a majority in all clusters
        has_majority
    }
    pub(crate) fn new(membership: &Membership) -> Self {
        Self {
            vote_counters: membership
                .voting_groups
                .iter()
                .cloned()
                .map(MajorityCounter::new)
                .collect(),
        }
    }
}
