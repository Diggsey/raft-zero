use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use rand::{thread_rng, Rng};
use tokio::time::{delay_until, Instant};

use act_zero::*;

use crate::commit_state::{CommitStateActor, CommitStateExt, CommitStateReceiverImpl};
use crate::config::Config;
use crate::connection::{Connection, ConnectionExt};
use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, BootstrapRequest, ClientError, ClientRequest,
    ClientResponse, ConflictOpt, Entry, EntryMembershipChange, EntryNormal, EntryPayload,
    InstallSnapshotRequest, InstallSnapshotResponse, Membership, ResponseMode, SetMembersError,
    SetMembersRequest, SetNonVotersError, SetNonVotersRequest, VoteRequest, VoteResponse,
    VotingGroup,
};
use crate::replication_stream::{self, ReplicationStreamActor, ReplicationStreamExt};
use crate::storage::{LogRange, Storage, StorageExt};
use crate::timer::TimerToken;
use crate::types::{LogIndex, NodeId, Term};
use crate::{spawn_actor, Application};

type ClientResult<A> = Result<
    ClientResponse<<A as Application>::LogResponse>,
    ClientError<<A as Application>::LogError>,
>;

struct Notifier<A: Application> {
    sender: Sender<ClientResult<A>>,
    mode: ResponseMode,
}

struct UncommittedEntry<A: Application> {
    entry: Arc<Entry<A::LogData>>,
    notify: Option<Notifier<A>>,
}

#[act_zero]
pub trait Node<A: Application> {
    // RPCs from the network
    fn request_vote(&self, req: VoteRequest, res: Sender<VoteResponse>);
    fn append_entries(&self, req: AppendEntriesRequest<A>, res: Sender<AppendEntriesResponse>);
    fn install_snapshot(&self, req: InstallSnapshotRequest, res: Sender<InstallSnapshotResponse>);

    // Initial setup
    fn bootstrap_cluster(&self, req: BootstrapRequest, res: Sender<ClientResult<A>>);

    // Leader commands
    fn client_request(&self, req: ClientRequest<A::LogData>, res: Sender<ClientResult<A>>);
    fn set_members(&self, req: SetMembersRequest, res: Sender<ClientResult<A>>);
    fn set_non_voters(&self, req: SetNonVotersRequest, res: Sender<ClientResult<A>>);
}

struct ReplicationState<A: Application> {
    is_up_to_date: bool,
    addr: Addr<Local<ReplicationStreamActor<A>>>,
}

type ReplicationStreamMap<A> = HashMap<NodeId, ReplicationState<A>>;

pub enum NodeError {
    StorageFailure,
    // Returned if the "State Machine Safety" property is violated
    SafetyViolation,
}

struct CommonState<A: Application> {
    app: A,
    this: WeakAddr<Local<NodeActor<A>>>,
    this_id: NodeId,
    current_term: Term,
    voted_for: Option<NodeId>,
    storage: Addr<dyn Storage<A>>,
    timer_token: TimerToken,
    connections: HashMap<NodeId, Addr<dyn Connection<A>>>,
    config: Arc<Config>,
    uncommitted_membership: Membership,
    uncommitted_entries: VecDeque<UncommittedEntry<A>>,
    committed_index: LogIndex,
    committed_term: Term,
    committed_membership: Membership,
    // Best guess at current leader, may not be accurate...
    leader_id: Option<NodeId>,
}

impl<A: Application> CommonState<A> {
    fn last_log_index(&self) -> LogIndex {
        self.committed_index + self.uncommitted_entries.len() as u64
    }
    fn last_log_term(&self) -> Term {
        if let Some(x) = self.uncommitted_entries.back() {
            x.entry.term
        } else {
            self.committed_term
        }
    }
    fn is_membership_changing(&self) -> bool {
        self.uncommitted_membership.is_joint() || self.committed_membership.is_joint()
    }
    fn schedule_election_timeout(&mut self) {
        let timeout = thread_rng().gen_range(
            self.config.min_election_timeout,
            self.config.max_election_timeout,
        );
        self.this
            .set_timeout(self.timer_token.inc(), Instant::now() + timeout);
    }
    fn state_for_replication(&self) -> replication_stream::LeaderState {
        replication_stream::LeaderState {
            term: self.current_term,
            commit_index: self.committed_index,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }
    fn build_replication_streams(
        &self,
        commit_state: &Addr<Local<CommitStateActor>>,
        prev_streams: &mut ReplicationStreamMap<A>,
    ) -> ReplicationStreamMap<A> {
        self.connections
            .iter()
            .map(|(&node_id, conn)| {
                (
                    node_id,
                    prev_streams
                        .remove(&node_id)
                        .unwrap_or_else(|| ReplicationState {
                            is_up_to_date: false,
                            addr: spawn_actor(ReplicationStreamActor::new(
                                node_id,
                                self.this.clone(),
                                self.this_id,
                                self.state_for_replication(),
                                self.config.clone(),
                                conn.clone(),
                                self.storage.clone(),
                                commit_state.clone(),
                            )),
                        }),
                )
            })
            .collect()
    }
    fn update_membership(&mut self) {
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
    fn is_up_to_date(&self, last_log_term: Term, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }
    fn can_vote_for(&self, term: Term, candidate_id: NodeId) -> bool {
        term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
    }
    fn handle_vote_request(&mut self, req: VoteRequest) -> VoteResponse {
        let vote_granted = self.can_vote_for(req.term, req.candidate_id)
            && self.is_up_to_date(req.last_log_term, req.last_log_index);

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
        }

        VoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    async fn replicate_to_log(&mut self, log_range: LogRange<A::LogData>) -> Result<(), NodeError> {
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

    async fn handle_append_entries(
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
    async fn set_commit_index(&mut self, commit_index: LogIndex) {
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
}

enum Role<A: Application> {
    NonVoter,
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState<A>),
}

struct LeaderState<A: Application> {
    commit_state: Addr<Local<CommitStateActor>>,
    replication_streams: ReplicationStreamMap<A>,
}

impl<A: Application> LeaderState<A> {
    async fn append_entry(
        &mut self,
        state: &mut CommonState<A>,
        payload: EntryPayload<A::LogData>,
        notify: Option<Notifier<A>>,
    ) -> Result<(), NodeError> {
        let entry = Arc::new(Entry {
            payload,
            term: state.current_term,
        });

        // First, try to append the entry to our own log
        if let Err(e) = state
            .storage
            .call_append_entry_to_log(entry.clone())
            .await
            .map_err(|_| NodeError::StorageFailure)?
        {
            // Entry rejected by application, report the error to the caller
            if let Some(notify) = notify {
                notify.sender.send(Err(ClientError::Application(e))).ok();
            }
            return Ok(());
        }

        // Add this to our list of uncommmitted entries
        state.uncommitted_entries.push_back(UncommittedEntry {
            entry: entry.clone(),
            notify,
        });

        // If this was a membership change, update our membership
        if entry.is_membership_change() {
            state.update_membership();
            self.commit_state
                .set_membership(state.uncommitted_membership.clone());
            self.replication_streams =
                state.build_replication_streams(&self.commit_state, &mut self.replication_streams);
        }

        // Replicate entry to other nodes
        for rs in self.replication_streams.values() {
            rs.addr
                .append_entry(state.state_for_replication(), entry.clone());
        }

        Ok(())
    }
}

// Counts votes within a single voting group
#[derive(Default, Clone)]
struct MajorityCounter {
    votes_received: u64,
    has_majority: bool,
}

impl MajorityCounter {
    fn add_vote(&mut self, from: NodeId, group: &VotingGroup) {
        if group.members.contains(&from) {
            self.votes_received += 1;
            if self.votes_received > (group.members.len() as u64) / 2 {
                self.has_majority = true;
            }
        }
    }
}

struct CandidateState {
    // One vote counter per voting group
    vote_counters: Vec<MajorityCounter>,
}

impl CandidateState {
    // Returns true if we gained a majority
    fn add_vote(&mut self, membership: &Membership, from: NodeId) -> bool {
        // Record the vote separately for each cluster
        for (counter, cluster) in self.vote_counters.iter_mut().zip(&membership.voting_groups) {
            counter.add_vote(from, cluster);
        }
        // Return true if reached a majority in all clusters
        self.vote_counters
            .iter()
            .all(|counter| counter.has_majority)
    }
    fn new(membership: &Membership) -> Self {
        Self {
            vote_counters: vec![Default::default(); membership.voting_groups.len()],
        }
    }
}

pub(crate) struct NodeActor<A: Application> {
    role: Role<A>,
    state: CommonState<A>,
}

impl<A: Application> Actor for NodeActor<A> {
    type Error = NodeError;

    fn started(&mut self, addr: Addr<Local<Self>>) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        self.state.this = addr.downgrade();
        Ok(())
    }
}

impl<A: Application> NodeActor<A> {
    fn received_vote(&mut self, from: NodeId) {
        if let Role::Candidate(candidate) = &mut self.role {
            if candidate.add_vote(&self.state.uncommitted_membership, from) {
                self.become_leader();
            }
        }
    }
    fn become_candidate(&mut self) {
        self.state.current_term.inc();
        self.state.voted_for = Some(self.state.this_id);
        self.role = Role::Candidate(CandidateState::new(&self.state.uncommitted_membership));
        self.state.schedule_election_timeout();

        // Vote for ourselves
        self.received_vote(self.state.this_id);

        let req = VoteRequest {
            term: self.state.current_term,
            candidate_id: self.state.this_id,
            last_log_index: self.state.last_log_index(),
            last_log_term: self.state.last_log_term(),
        };

        for (&node_id, conn) in &self.state.connections {
            // Don't send vote requests to non-voting members
            if !self
                .state
                .uncommitted_membership
                .non_voters
                .contains(&node_id)
            {
                self.state.this.await_vote_response(
                    req.term,
                    node_id,
                    conn.call_request_vote(req.clone()),
                );
            }
        }
    }
    fn become_follower(&mut self) {
        self.role = Role::Follower;
        self.state.schedule_election_timeout();
    }
    fn become_non_voter(&mut self) {
        self.role = Role::NonVoter;
        self.state.timer_token.inc();
    }
    fn become_leader(&mut self) {
        self.state.timer_token.inc();
        let commit_state = spawn_actor(CommitStateActor::new(
            self.state.current_term,
            self.state.uncommitted_membership.clone(),
            self.state.committed_index,
            self.state.this.clone().upcast(),
        ));
        self.role = Role::Leader(LeaderState {
            replication_streams: self
                .state
                .build_replication_streams(&commit_state, &mut HashMap::new()),
            commit_state,
        });

        // Replicate a blank entry on election to ensure we have an entry from our own term
        self.state.this.send_blank_entry();
    }
    fn validate_term(&mut self, term: Term) -> bool {
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            match self.role {
                Role::NonVoter => {}
                _ => self.become_follower(),
            }
            false
        } else {
            true
        }
    }
    fn update_role_from_membership(&mut self) {
        let committed_voter = self.state.committed_membership.is_voter(self.state.this_id);
        let uncommitted_voter = self
            .state
            .uncommitted_membership
            .is_voter(self.state.this_id);

        match (&self.role, committed_voter, uncommitted_voter) {
            // The Leader -> NonVoter transition is special, and doesn't occur until the membership
            // change is committed.
            (Role::Leader(_), false, false) => self.become_non_voter(),

            // Other transitions occur immediately.
            (Role::Candidate(_), _, false) | (Role::Follower, _, false) => self.become_non_voter(),
            (Role::NonVoter, _, true) => self.become_follower(),

            // Do nothing.
            _ => {}
        }
    }
    async fn internal_request(
        &mut self,
        payload: EntryPayload<A::LogData>,
        notify: Option<Notifier<A>>,
        rate_limited: bool,
    ) -> Result<(), NodeError> {
        if let Role::Leader(leader_state) = &mut self.role {
            if !rate_limited
                || (self.state.uncommitted_entries.len() as u64)
                    < self.state.config.max_in_flight_requests.unwrap_or(u64::MAX)
            {
                leader_state
                    .append_entry(&mut self.state, payload, notify)
                    .await?;
            } else {
                // Too many in-flight requests already
                if let Some(notify) = notify {
                    notify.sender.send(Err(ClientError::Busy)).ok();
                }
            }
        } else {
            // Only the leader can respond to client requests
            if let Some(notify) = notify {
                notify
                    .sender
                    .send(Err(ClientError::NotLeader {
                        leader_id: self.state.leader_id,
                    }))
                    .ok();
            }
        }
        Ok(())
    }
}

#[act_zero]
impl<A: Application> Node<A> for NodeActor<A> {
    async fn request_vote(&mut self, req: VoteRequest, res: Sender<VoteResponse>) {
        self.validate_term(req.term);

        res.send(self.state.handle_vote_request(req)).ok();
    }
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<A>,
        res: Sender<AppendEntriesResponse>,
    ) -> Result<(), NodeError> {
        self.validate_term(req.term);

        let resp = self.state.handle_append_entries(req).await?;

        if resp.success {
            // Check for role changes caused by membership changes
            self.update_role_from_membership();
        }

        res.send(resp).ok();
        Ok(())
    }
    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest,
        _res: Sender<InstallSnapshotResponse>,
    ) {
        unimplemented!()
    }
    // Leader commands
    async fn client_request(
        &mut self,
        req: ClientRequest<A::LogData>,
        res: Sender<ClientResult<A>>,
    ) -> Result<(), NodeError> {
        self.internal_request(
            EntryPayload::Application(EntryNormal { data: req.data }),
            Some(Notifier {
                sender: res,
                mode: req.response_mode,
            }),
            true,
        )
        .await
    }
    async fn set_members(
        &mut self,
        req: SetMembersRequest,
        res: Sender<ClientResult<A>>,
    ) -> Result<(), NodeError> {
        // Check destination state for validity
        if req.ids.is_empty() {
            // Empty cluster is not allowed...
            res.send(Err(ClientError::SetMembers(
                SetMembersError::InvalidMembers,
            )))
            .ok();
            return Ok(());
        }

        // Check that we are sufficiently fault tolerant
        if req.fault_tolerance > 0 {
            let original_ids = &self.state.uncommitted_membership.voting_groups[0].members;

            let old_fault_tolerance = (original_ids.len() as u64 - 1) / 2;
            if old_fault_tolerance < req.fault_tolerance {
                // Requested cluster is too small to provide desired fault tolerance
                res.send(Err(ClientError::SetMembers(
                    SetMembersError::InvalidFaultTolerance,
                )))
                .ok();
                return Ok(());
            }

            let new_fault_tolerance = (req.ids.len() as u64 - 1) / 2;
            if new_fault_tolerance < req.fault_tolerance {
                // Requested cluster is too small to provide desired fault tolerance
                res.send(Err(ClientError::SetMembers(
                    SetMembersError::InvalidMembers,
                )))
                .ok();
                return Ok(());
            }

            // At this point we know the old and new clusters each have at least 3 nodes
            let static_ids = original_ids | &req.ids;
            let added_ids = &req.ids - original_ids;
            let removed_ids = original_ids - &req.ids;

            // Check if we need to make the change in multiple steps
            if static_ids.len() < added_ids.len() + req.fault_tolerance as usize {
                // Work out how many add or remove operations must be cancelled before we can do it in one step
                let mut needed_reduction =
                    added_ids.len() + req.fault_tolerance as usize - static_ids.len();

                // Try to balance out the actions we cancel
                let balanced_reduction = cmp::min(
                    cmp::min(added_ids.len(), removed_ids.len()),
                    needed_reduction / 2,
                );
                let mut cancel_adds = balanced_reduction;
                let mut cancel_removes = balanced_reduction;

                // Compute unbalanced remainder
                needed_reduction -= balanced_reduction * 2;
                if removed_ids.len() >= balanced_reduction + needed_reduction {
                    cancel_removes += needed_reduction;
                } else {
                    cancel_adds += needed_reduction;
                }

                let proposed_ids = static_ids
                    .into_iter()
                    .chain(removed_ids.into_iter().take(cancel_removes))
                    .chain(added_ids.into_iter().skip(cancel_adds))
                    .collect();

                // Requested member change must be done in multiple steps
                res.send(Err(ClientError::SetMembers(
                    SetMembersError::InvalidTransition { proposed_ids },
                )))
                .ok();
                return Ok(());
            }
        }

        // Don't allow member changes whilst members are already changing
        if self.state.is_membership_changing() {
            res.send(Err(ClientError::SetMembers(SetMembersError::InvalidState)))
                .ok();
            return Ok(());
        }

        // Check if a majority of new nodes are up to date
        if let Role::Leader(leader_state) = &mut self.role {
            let lagging_ids: HashSet<NodeId> = req
                .ids
                .iter()
                .copied()
                .filter(|node_id| !leader_state.replication_streams[node_id].is_up_to_date)
                .collect();
            let num_up_to_date = req.ids.len() - lagging_ids.len();
            if num_up_to_date == 0 || (((num_up_to_date - 1) / 2) as u64) < req.fault_tolerance {
                // Too many lagging members
                res.send(Err(ClientError::SetMembers(
                    SetMembersError::LaggingMembers { ids: lagging_ids },
                )))
                .ok();
                return Ok(());
            }
        }

        // Everything is good to go, build the new membership configuration!
        let mut membership = self.state.uncommitted_membership.clone();
        membership
            .voting_groups
            .push(VotingGroup { members: req.ids });

        self.internal_request(
            EntryPayload::MembershipChange(EntryMembershipChange { membership }),
            Some(Notifier {
                sender: res,
                mode: ResponseMode::Applied,
            }),
            true,
        )
        .await
    }
    async fn set_non_voters(
        &mut self,
        req: SetNonVotersRequest,
        res: Sender<ClientResult<A>>,
    ) -> Result<(), NodeError> {
        // For simplicity, don't allow non-voter changes whilst members are changing
        if self.state.is_membership_changing() {
            res.send(Err(ClientError::SetNonVoters(
                SetNonVotersError::InvalidState,
            )))
            .ok();
            return Ok(());
        }

        // Don't allow members to be added as non-voters
        let existing_voters: HashSet<NodeId> = req
            .ids
            .iter()
            .copied()
            .filter(|&node_id| self.state.uncommitted_membership.is_voter(node_id))
            .collect();

        if !existing_voters.is_empty() {
            res.send(Err(ClientError::SetNonVoters(
                SetNonVotersError::ExistingMembers {
                    ids: existing_voters,
                },
            )))
            .ok();
            return Ok(());
        }

        // All good, make the change
        let membership = Membership {
            voting_groups: self.state.uncommitted_membership.voting_groups.clone(),
            non_voters: req.ids,
        };

        self.internal_request(
            EntryPayload::MembershipChange(EntryMembershipChange { membership }),
            Some(Notifier {
                sender: res,
                mode: ResponseMode::Applied,
            }),
            true,
        )
        .await
    }
    async fn bootstrap_cluster(
        &mut self,
        req: BootstrapRequest,
        res: Sender<ClientResult<A>>,
    ) -> Result<(), NodeError> {
        assert_eq!(
            self.state.last_log_index(),
            LogIndex::ZERO,
            "Cannot bootstrap cluster when it already has log entries"
        );

        self.become_leader();
        self.internal_request(
            EntryPayload::MembershipChange(EntryMembershipChange {
                membership: Membership {
                    voting_groups: vec![VotingGroup {
                        members: req.members,
                    }],
                    non_voters: req.non_voters,
                },
            }),
            Some(Notifier {
                sender: res,
                mode: ResponseMode::Applied,
            }),
            false,
        )
        .await
    }
}

#[act_zero]
impl<A: Application> CommitStateReceiver for NodeActor<A> {
    async fn set_commit_index(
        &mut self,
        term: Term,
        commit_index: LogIndex,
    ) -> Result<(), NodeError> {
        if let Role::Leader(leader_state) = &mut self.role {
            if self.state.current_term == term {
                self.state.set_commit_index(commit_index).await;

                // If we just committed a joint-consensus membership
                if self.state.committed_membership.is_joint() {
                    // If we haven't already requested to leave joint membership
                    if self.state.uncommitted_membership.is_joint()
                        && self.state.committed_membership == self.state.uncommitted_membership
                    {
                        // Request to leave joint membership
                        let single_membership = self.state.uncommitted_membership.to_single();

                        leader_state
                            .append_entry(
                                &mut self.state,
                                EntryPayload::MembershipChange(EntryMembershipChange {
                                    membership: single_membership,
                                }),
                                None,
                            )
                            .await?;
                    }
                }

                // If we're not part of the new configuration, step down
                self.update_role_from_membership();
            }
        }
        Ok(())
    }
}

#[act_zero]
pub(crate) trait PrivateNode<A: Application> {
    fn set_timeout(&self, token: TimerToken, deadline: Instant);
    fn timer_tick(&self, token: TimerToken);
    fn await_vote_response(&self, term: Term, from: NodeId, receiver: Receiver<VoteResponse>);
    fn record_vote_response(&self, term: Term, from: NodeId, resp: VoteResponse);
    fn record_term(&self, term: Term, node_id: NodeId, is_up_to_date: bool, res: Sender<()>);
    fn send_log_response(
        &self,
        term: Term,
        index: LogIndex,
        receiver: Receiver<A::LogResponse>,
        res: Sender<ClientResult<A>>,
    );
    fn send_blank_entry(&self);
}

#[act_zero]
impl<A: Application> PrivateNode<A> for NodeActor<A> {
    async fn set_timeout(self: Addr<Local<NodeActor<A>>>, token: TimerToken, deadline: Instant) {
        delay_until(deadline).await;
        self.timer_tick(token);
    }
    async fn timer_tick(&mut self, token: TimerToken) {
        // Ignore spurious wake-ups from cancelled timeouts
        if token != self.state.timer_token {
            return;
        }

        match &mut self.role {
            // If we're a non-voter or leader, there's no timeout
            Role::NonVoter | Role::Leader(_) => unreachable!(),
            // If we're a follower, the timeout means we should convert to a candidate
            // If we're a candidate, the timeout means we should start a new election
            Role::Follower | Role::Candidate(_) => {
                self.become_candidate();
            }
        }
    }
    async fn await_vote_response(
        self: Addr<Local<NodeActor<A>>>,
        term: Term,
        from: NodeId,
        receiver: Receiver<VoteResponse>,
    ) {
        if let Ok(resp) = receiver.await {
            self.record_vote_response(term, from, resp);
        }
    }
    async fn record_vote_response(&mut self, term: Term, from: NodeId, resp: VoteResponse) {
        self.validate_term(resp.term);
        if term == self.state.current_term {
            if resp.vote_granted {
                self.received_vote(from)
            }
        }
    }
    async fn record_term(
        &mut self,
        term: Term,
        node_id: NodeId,
        is_up_to_date: bool,
        res: Sender<()>,
    ) {
        self.validate_term(term);

        if let Role::Leader(leader_state) = &mut self.role {
            if let Some(rs) = leader_state.replication_streams.get_mut(&node_id) {
                rs.is_up_to_date = is_up_to_date;
            }

            res.send(()).ok();
        }
    }
    async fn send_log_response(
        &self,
        term: Term,
        index: LogIndex,
        receiver: Receiver<A::LogResponse>,
        res: Sender<ClientResult<A>>,
    ) {
        // Wait for the log entry to be applied, and the forward the response to the client.
        if let Ok(data) = receiver.await {
            res.send(Ok(ClientResponse {
                data: Some(data),
                term,
                index,
            }))
            .ok();
        }
    }
    async fn send_blank_entry(&mut self) -> Result<(), NodeError> {
        // If there are no log entries for our term yet, create a blank one to ensure stuff
        // gets committed promptly.
        if self.state.last_log_term() != self.state.current_term {
            self.internal_request(EntryPayload::Blank, None, false)
                .await?
        }
        Ok(())
    }
}
