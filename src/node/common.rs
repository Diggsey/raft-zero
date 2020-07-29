use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use rand::{thread_rng, Rng};
use tokio::time::Instant;

use act_zero::*;

use crate::config::Config;
use crate::connection::Connection;
use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, ClientResponse, ConflictOpt, Entry, EntryPayload,
    Membership, ResponseMode, VoteRequest, VoteResponse,
};
use crate::replication_stream;
use crate::storage::{HardState, LogRange, Storage, StorageExt};
use crate::timer::TimerToken;
use crate::types::{LogIndex, NodeId, Term};
use crate::Application;

use super::{ClientResult, NodeActor, NodeError, PrivateNodeExt};

pub(crate) struct Notifier<A: Application> {
    pub(crate) sender: Sender<ClientResult<A>>,
    pub(crate) mode: ResponseMode,
}

pub(crate) struct UncommittedEntry<A: Application> {
    pub(crate) entry: Arc<Entry<A::LogData>>,
    pub(crate) notify: Option<Notifier<A>>,
}

pub(crate) struct CommonState<A: Application> {
    pub(crate) app: A,
    pub(crate) this: WeakAddr<Local<NodeActor<A>>>,
    pub(crate) this_id: NodeId,
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) storage: Addr<dyn Storage<A>>,
    pub(crate) timer_token: TimerToken,
    pub(crate) connections: HashMap<NodeId, Addr<dyn Connection<A>>>,
    pub(crate) config: Arc<Config>,
    pub(crate) uncommitted_membership: Membership,
    pub(crate) uncommitted_entries: VecDeque<UncommittedEntry<A>>,
    pub(crate) committed_index: LogIndex,
    pub(crate) committed_term: Term,
    pub(crate) committed_membership: Membership,
    // Best guess at current leader, may not be accurate...
    pub(crate) leader_id: Option<NodeId>,
}

impl<A: Application> CommonState<A> {
    pub(crate) fn last_log_index(&self) -> LogIndex {
        self.committed_index + self.uncommitted_entries.len() as u64
    }
    pub(crate) fn last_log_term(&self) -> Term {
        if let Some(x) = self.uncommitted_entries.back() {
            x.entry.term
        } else {
            self.committed_term
        }
    }
    pub(crate) fn is_membership_changing(&self) -> bool {
        self.uncommitted_membership.is_joint() || self.committed_membership.is_joint()
    }
    pub(crate) fn schedule_election_timeout(&mut self) {
        let timeout = thread_rng().gen_range(
            self.config.min_election_timeout,
            self.config.max_election_timeout,
        );
        self.this
            .set_timeout(self.timer_token.inc(), Instant::now() + timeout);
    }
    pub(crate) fn state_for_replication(&self) -> replication_stream::LeaderState {
        replication_stream::LeaderState {
            term: self.current_term,
            commit_index: self.committed_index,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }
    pub(crate) fn update_membership(&mut self) {
        // Look for the most recent membership change log entry
        self.uncommitted_membership = self
            .uncommitted_entries
            .iter()
            .rev()
            .find_map(|x| {
                if let EntryPayload::MembershipChange(m) = &x.entry.payload {
                    Some(m.membership.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| self.committed_membership.clone());

        let app = &mut self.app;
        let connections = &mut self.connections;
        self.connections = self.uncommitted_membership.map_members(false, |node_id| {
            connections
                .remove(&node_id)
                .unwrap_or_else(|| app.establish_connection(node_id))
        });
    }
    pub(crate) fn is_up_to_date(&self, last_log_term: Term, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }
    pub(crate) fn can_vote_for(&self, term: Term, candidate_id: NodeId) -> bool {
        term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
    }
    pub(crate) async fn handle_vote_request(
        &mut self,
        req: VoteRequest,
    ) -> Result<VoteResponse, NodeError> {
        let vote_granted = self.can_vote_for(req.term, req.candidate_id)
            && self.is_up_to_date(req.last_log_term, req.last_log_index);

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
            self.save_hard_state().await?;
        }

        Ok(VoteResponse {
            term: self.current_term,
            vote_granted,
        })
    }

    pub(crate) async fn replicate_to_log(
        &mut self,
        log_range: LogRange<A::LogData>,
    ) -> Result<(), NodeError> {
        // Cannot replace committed entries
        if log_range.prev_log_index < self.committed_index {
            return Err(NodeError::SafetyViolation);
        }

        // Splice in the replacement log entries
        self.uncommitted_entries
            .truncate((log_range.prev_log_index - self.committed_index) as usize);
        self.uncommitted_entries
            .extend(
                log_range
                    .entries
                    .iter()
                    .cloned()
                    .map(|entry| UncommittedEntry {
                        entry,
                        notify: None,
                    }),
            );

        // Replicate the entries to storage
        self.storage
            .call_replicate_to_log(log_range)
            .await
            .map_err(|_| NodeError::StorageFailure)
    }

    pub(crate) async fn handle_append_entries(
        &mut self,
        mut req: AppendEntriesRequest<A>,
    ) -> Result<AppendEntriesResponse, NodeError> {
        // Update leader ID
        self.leader_id = Some(req.leader_id);

        let mut has_membership_change = false;

        // Remove any entries prior to our commit index, as we know these match
        if req.prev_log_index < self.committed_index && !req.entries.is_empty() {
            let num_to_remove = cmp::min(
                self.committed_index - req.prev_log_index,
                req.entries.len() as u64,
            );
            req.prev_log_term = req.entries[num_to_remove as usize - 1].term;
            req.prev_log_index += num_to_remove;
            req.entries.drain(0..num_to_remove as usize);
        }

        // Figure out whether the remaining entries can be applied cleanly
        let (success, conflict_opt) = if req.term == self.current_term {
            if self.last_log_term() == req.prev_log_term
                && self.last_log_index() == req.prev_log_index
            {
                // Happy path, new entries can just be appended
                (true, None)
            } else if req.prev_log_index < self.committed_index {
                // There were no entries more recent than our commit index, so we know they all match
                assert!(req.entries.is_empty());
                (true, None)
            } else if req.prev_log_index < self.last_log_index() {
                // We need to check that the entries match our uncommitted entries
                let mut uncommitted_offset = req.prev_log_index - self.committed_index;

                let expected_log_term = if uncommitted_offset == 0 {
                    self.committed_term
                } else {
                    self.uncommitted_entries[uncommitted_offset as usize - 1]
                        .entry
                        .term
                };

                if expected_log_term == req.prev_log_term {
                    let num_matching = req
                        .entries
                        .iter()
                        .zip(
                            self.uncommitted_entries
                                .iter()
                                .skip(uncommitted_offset as usize),
                        )
                        .position(|(a, b)| a.term != b.entry.term)
                        .unwrap_or(cmp::min(
                            self.uncommitted_entries.len() - uncommitted_offset as usize,
                            req.entries.len(),
                        ));

                    // Remove matching entries from the incoming request
                    if num_matching > 0 {
                        req.prev_log_term = req.entries[num_matching - 1].term;
                        req.prev_log_index += num_matching as u64;
                        uncommitted_offset += num_matching as u64;
                        req.entries.drain(0..num_matching);
                    }

                    // If there exist conflicting entries left over
                    if req.entries.is_empty() {
                        // Remove conflicting uncommitted entries, and check them for membership changes
                        if self
                            .uncommitted_entries
                            .drain(uncommitted_offset as usize..)
                            .any(|x| x.entry.is_membership_change())
                        {
                            // Membership may have changed
                            has_membership_change = true
                        }
                    }

                    (true, None)
                } else {
                    // New entries would conflict
                    (
                        false,
                        // Jump back to the most recent committed entry
                        Some(ConflictOpt {
                            index: self.committed_index,
                        }),
                    )
                }
            } else {
                // New entries are from the future, we need to fill in the gap
                (
                    false,
                    Some(ConflictOpt {
                        index: self.last_log_index(),
                    }),
                )
            }
        } else {
            // Ignore request completely, entries are from an ex-leader
            (false, None)
        };

        if success {
            if !req.entries.is_empty() {
                has_membership_change |=
                    req.entries.iter().any(|entry| entry.is_membership_change());

                self.replicate_to_log(LogRange {
                    prev_log_index: req.prev_log_index,
                    prev_log_term: req.prev_log_term,
                    entries: req.entries,
                })
                .await?;

                if has_membership_change {
                    self.update_membership();
                }
            }

            self.set_commit_index(cmp::min(req.leader_commit, self.last_log_index()))
                .await;
        }

        Ok(AppendEntriesResponse {
            success,
            term: self.current_term,
            conflict_opt,
        })
    }
    pub(crate) async fn set_commit_index(&mut self, commit_index: LogIndex) {
        while commit_index > self.committed_index {
            let uncommitted_entry = self
                .uncommitted_entries
                .pop_front()
                .expect("Cannot advance commit index past latest log entry!");

            // Update our internal state machine
            self.committed_index += 1;
            self.committed_term = uncommitted_entry.entry.term;

            if let EntryPayload::MembershipChange(m) = &uncommitted_entry.entry.payload {
                self.committed_membership = m.membership.clone();
            }

            // Spawn a task to update the application state machine
            let receiver = self
                .storage
                .call_apply_to_state_machine(self.committed_index, uncommitted_entry.entry);

            if let Some(notify) = uncommitted_entry.notify {
                match notify.mode {
                    ResponseMode::Committed => {
                        notify
                            .sender
                            .send(Ok(ClientResponse {
                                data: None,
                                term: self.committed_term,
                                index: self.committed_index,
                            }))
                            .ok();
                    }
                    ResponseMode::Applied => self.this.send_log_response(
                        self.committed_term,
                        self.committed_index,
                        receiver,
                        notify.sender,
                    ),
                }
            }
        }
    }
    pub(crate) async fn save_hard_state(&self) -> Result<(), NodeError> {
        self.storage
            .call_save_hard_state(HardState {
                current_term: self.current_term,
                voted_for: self.voted_for,
            })
            .await
            .map_err(|_| NodeError::StorageFailure)
    }
}
