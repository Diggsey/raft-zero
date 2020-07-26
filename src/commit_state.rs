use std::collections::HashMap;
use std::mem;

use act_zero::*;

use crate::messages::Membership;
use crate::types::{LogIndex, NodeId, Term};

pub struct CommitStateActor {
    term: Term,
    membership: Membership,
    commit_index: LogIndex,
    match_index: HashMap<NodeId, LogIndex>,
    receiver: WeakAddr<dyn CommitStateReceiver>,
}

impl CommitStateActor {
    pub fn new(
        term: Term,
        membership: Membership,
        commit_index: LogIndex,
        receiver: WeakAddr<dyn CommitStateReceiver>,
    ) -> Self {
        let match_index = membership.map_members(true, |_| LogIndex::ZERO);
        Self {
            term,
            membership,
            commit_index,
            match_index,
            receiver,
        }
    }
    fn recalculate_commit_index(&mut self) {
        let new_commit_index = self
            .membership
            .voting_groups
            .iter()
            .map(|group| {
                // Find the upper-median commit index within each voting group
                let mut indexes: Vec<_> = group
                    .members
                    .iter()
                    .map(|member| self.match_index[member])
                    .collect();
                indexes.sort();
                indexes[indexes.len() / 2]
            })
            // Take the minimum commit index from all the voting groups
            .min()
            .unwrap_or_default();

        // If the commit index has advanced, notify the receiver
        if new_commit_index > self.commit_index {
            self.commit_index = new_commit_index;
            self.receiver.set_commit_index(self.term, new_commit_index);
        }
    }
}

impl Actor for CommitStateActor {
    type Error = ();
}

#[act_zero]
pub trait CommitState {
    fn set_match_index(&self, node: NodeId, match_index: LogIndex, match_term: Term);
    fn set_membership(&self, membership: Membership);
}

#[act_zero]
impl CommitState for CommitStateActor {
    async fn set_match_index(&mut self, node: NodeId, match_index: LogIndex, match_term: Term) {
        if match_term == self.term {
            if let Some(stored_index) = self.match_index.get_mut(&node) {
                let old_value = mem::replace(stored_index, match_index);
                if old_value <= self.commit_index && match_index > self.commit_index {
                    self.recalculate_commit_index();
                }
            }
        }
    }
    async fn set_membership(&mut self, membership: Membership) {
        self.match_index = membership.map_members(true, |node_id| {
            self.match_index.get(&node_id).copied().unwrap_or_default()
        });
        self.recalculate_commit_index();
    }
}

#[act_zero]
pub trait CommitStateReceiver {
    fn set_commit_index(&self, term: Term, commit_index: LogIndex);
}
