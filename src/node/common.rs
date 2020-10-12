use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use futures::channel::oneshot;
use rand::{thread_rng, Rng};
use tokio::io::AsyncWriteExt;

use act_zero::runtimes::default::Timer;
use act_zero::*;

use crate::config::Config;
use crate::connection::Connection;
use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, ClientResponse, ConflictOpt, Entry, EntryPayload,
    InstallSnapshotRequest, Membership, ResponseMode,
};
use crate::observer::{ObservedState, Observer};
use crate::replication_stream;
use crate::storage::{BoxAsyncWrite, HardState, LogRange, Snapshot, Storage};
use crate::types::{DatabaseId, LogIndex, NodeId, Term};
use crate::Application;

use super::{ClientResult, NodeActor, NodeError};

pub(crate) struct Notifier<A: Application> {
    pub(crate) sender: oneshot::Sender<Produces<ClientResult<A>>>,
    pub(crate) mode: ResponseMode,
}

pub(crate) struct UncommittedEntry<A: Application> {
    pub(crate) entry: Arc<Entry<A::LogData>>,
    pub(crate) notify: Option<Notifier<A>>,
}

pub(crate) struct CommonState<A: Application> {
    // Constants
    pub(crate) this_id: NodeId,
    pub(crate) app: A,
    pub(crate) config: Arc<Config>,

    // Raft state
    pub(crate) database_id: DatabaseId,
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) uncommitted_membership: Membership,
    pub(crate) uncommitted_entries: VecDeque<UncommittedEntry<A>>,
    pub(crate) committed_index: LogIndex,
    pub(crate) committed_term: Term,
    pub(crate) committed_membership: Membership,
    // Best guess at current leader, may not be accurate...
    pub(crate) leader_id: Option<NodeId>,

    // Actors
    pub(crate) this: WeakAddr<NodeActor<A>>,
    pub(crate) storage: Addr<dyn Storage<A>>,
    pub(crate) observer: Addr<dyn Observer>,
    pub(crate) connections: HashMap<NodeId, Addr<dyn Connection<A>>>,

    // Timer
    pub(crate) timer: Timer,

    // Other
    last_observed_state: ObservedState,
    installing_snapshot: Option<(Snapshot<A::SnapshotId>, BoxAsyncWrite)>,
}

impl<A: Application> CommonState<A> {
    pub(crate) fn new(this_id: NodeId, app: A) -> Self {
        let config = app.config();
        let storage = app.storage();
        let observer = app.observer();
        let membership = Membership::empty();
        Self {
            this: WeakAddr::default(),
            app,
            this_id,
            config,

            database_id: DatabaseId::UNSET,
            current_term: Term(0),
            voted_for: None,
            uncommitted_membership: membership.clone(),
            uncommitted_entries: VecDeque::new(),
            committed_index: LogIndex::ZERO,
            committed_term: Term(0),
            committed_membership: membership,
            leader_id: None,

            storage,
            observer,
            connections: HashMap::new(),

            timer: Default::default(),

            last_observed_state: ObservedState::default(),
            installing_snapshot: None,
        }
    }
    pub(crate) fn update_observer(&mut self) {
        let observed_state = ObservedState {
            node_id: self.this_id,
            leader_id: self.leader_id,
            current_term: self.current_term,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
            committed_index: self.committed_index,
            committed_term: self.committed_term,
            voted_for: self.voted_for,
            election_deadline: self.timer.state().deadline(),
        };

        if observed_state != self.last_observed_state {
            self.last_observed_state = observed_state.clone();
            send!(self.observer.observe_state(observed_state));
        }
    }
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
    pub(crate) fn mark_not_leader(&mut self) {
        if self.leader_id == Some(self.this_id) {
            self.leader_id = None;
        }
    }
    pub(crate) fn schedule_election_timeout(&mut self) {
        let timeout = thread_rng().gen_range(
            self.config.min_election_timeout,
            self.config.max_election_timeout,
        );
        self.timer.set_timeout_for_weak(self.this.clone(), timeout);
    }
    pub(crate) fn clear_election_timeout(&mut self) {
        self.timer.clear();
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
        self.connections =
            self.uncommitted_membership
                .map_members(Some(self.this_id), false, |node_id| {
                    connections
                        .remove(&node_id)
                        .unwrap_or_else(|| app.establish_connection(node_id))
                });

        send!(self
            .observer
            .observe_membership(self.uncommitted_membership.clone()));
    }
    pub(crate) fn is_up_to_date(&self, last_log_term: Term, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }
    pub(crate) fn can_vote_for(&self, term: Term, candidate_id: NodeId) -> bool {
        term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
    }
    pub(crate) async fn load_log_state(&mut self) -> Result<(), NodeError> {
        let log_state = call!(self.storage.get_log_state())
            .await
            .map_err(|_| NodeError::StorageFailure)?;

        let loaded_range = call!(self
            .storage
            .get_log_range(log_state.last_log_applied + 1..log_state.last_log_index + 1))
        .await
        .map_err(|_| NodeError::StorageFailure)?
        .log_range()
        .ok_or(NodeError::SafetyViolation)?;

        self.committed_index = log_state.last_log_index;
        self.committed_term = loaded_range.prev_log_term;
        self.committed_membership = log_state.last_membership_applied;
        self.uncommitted_entries = loaded_range
            .entries
            .into_iter()
            .map(|entry| UncommittedEntry {
                entry,
                notify: None,
            })
            .collect();
        self.update_membership();
        Ok(())
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
        call!(self.storage.replicate_to_log(log_range))
            .await
            .map_err(|_| NodeError::StorageFailure)
    }

    pub(crate) async fn handle_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> Result<bool, NodeError> {
        // It's a new snapshot
        if req.offset == 0 {
            let (snapshot_id, target) = call!(self.storage.create_snapshot())
                .await
                .map_err(|_| NodeError::StorageFailure)?;

            self.installing_snapshot = Some((
                Snapshot {
                    id: snapshot_id,
                    last_log_index: req.last_included_index,
                    last_log_term: req.last_included_term,
                },
                target,
            ));
        }

        if let Some((snapshot, mut target)) = self.installing_snapshot.take() {
            target
                .write_all(&req.data)
                .await
                .map_err(|_| NodeError::StorageFailure)?;

            if req.done {
                target
                    .shutdown()
                    .await
                    .map_err(|_| NodeError::StorageFailure)?;

                // Snapshot is useless, discard it
                if snapshot.last_log_index < self.committed_index {
                    return Ok(false);
                }

                call!(self.storage.install_snapshot(snapshot))
                    .await
                    .map_err(|_| NodeError::StorageFailure)?;

                self.load_log_state().await?;
                return Ok(true);
            } else {
                self.installing_snapshot = Some((snapshot, target));
            }
        }

        Ok(false)
    }

    pub(crate) async fn handle_append_entries(
        &mut self,
        mut req: AppendEntriesRequest<A>,
    ) -> Result<AppendEntriesResponse, NodeError> {
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
        let (success, conflict_opt) = if self.last_log_term() == req.prev_log_term
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
                if !req.entries.is_empty() {
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
            let receiver = call!(self
                .storage
                .apply_to_state_machine(self.committed_index, uncommitted_entry.entry));

            if let Some(notify) = uncommitted_entry.notify {
                match notify.mode {
                    ResponseMode::Committed => {
                        notify
                            .sender
                            .send(Produces::Value(Ok(ClientResponse {
                                data: None,
                                term: self.committed_term,
                                index: self.committed_index,
                            })))
                            .ok();
                    }
                    ResponseMode::Applied => {
                        notify
                            .sender
                            .send(call!(self.this.send_log_response(
                                self.committed_term,
                                self.committed_index,
                                receiver
                            )))
                            .ok();
                    }
                }
            }
        }
    }
    pub(crate) async fn save_hard_state(&self) -> Result<(), NodeError> {
        call!(self.storage.save_hard_state(HardState {
            database_id: self.database_id,
            current_term: self.current_term,
            voted_for: self.voted_for,
        }))
        .await
        .map_err(|_| NodeError::StorageFailure)
    }
}
