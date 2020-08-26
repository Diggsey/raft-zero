use std::collections::HashSet;

use crate::messages::{Membership, VotingGroup};
use crate::types::NodeId;

// Counts votes within a single voting group
#[derive(Default, Clone)]
struct MajorityCounter {
    votes_received: HashSet<NodeId>,
    has_majority: bool,
}

impl MajorityCounter {
    fn add_vote(&mut self, from: NodeId, group: &VotingGroup) {
        if group.members.contains(&from) {
            self.votes_received.insert(from);
            if self.votes_received.len() > group.members.len() / 2 {
                self.has_majority = true;
            }
        }
    }
}

pub(crate) struct CandidateState {
    // One vote counter per voting group
    vote_counters: Vec<MajorityCounter>,
}

impl CandidateState {
    // Returns true if we gained a majority
    pub(crate) fn add_vote(&mut self, membership: &Membership, from: NodeId) -> bool {
        // Record the vote separately for each cluster
        for (counter, cluster) in self.vote_counters.iter_mut().zip(&membership.voting_groups) {
            counter.add_vote(from, cluster);
        }
        // Return true if reached a majority in all clusters
        self.vote_counters
            .iter()
            .all(|counter| counter.has_majority)
    }
    pub(crate) fn new(membership: &Membership) -> Self {
        Self {
            vote_counters: vec![Default::default(); membership.voting_groups.len()],
        }
    }
}
