use std::collections::{HashMap, HashSet, VecDeque};

use tokio::time::{delay_until, Instant};

use act_zero::*;

use crate::commit_state::CommitStateReceiverImpl;
use crate::connection::ConnectionExt;
use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, BootstrapRequest, ClientError, ClientRequest,
    ClientResponse, EntryMembershipChange, EntryNormal, EntryPayload, InstallSnapshotRequest,
    InstallSnapshotResponse, Membership, ResponseMode, SetLearnersError, SetLearnersRequest,
    SetMembersError, SetMembersRequest, VoteRequest, VoteResponse, VotingGroup,
};
use crate::replication_stream::ReplicationStreamActor;
use crate::storage::StorageExt;
use crate::timer::TimerToken;
use crate::types::{LogIndex, NodeId, Term};
use crate::{spawn_actor, Application};

mod candidate;
mod common;
mod leader;

use candidate::CandidateState;
use common::{CommonState, Notifier, UncommittedEntry};
use leader::LeaderState;

pub type ClientResult<A> = Result<
    ClientResponse<<A as Application>::LogResponse>,
    ClientError<<A as Application>::LogError>,
>;

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
    fn set_learners(&self, req: SetLearnersRequest, res: Sender<ClientResult<A>>);
}

pub(crate) struct ReplicationState<A: Application> {
    is_up_to_date: bool,
    addr: Addr<Local<ReplicationStreamActor<A>>>,
}

type ReplicationStreamMap<A> = HashMap<NodeId, ReplicationState<A>>;

#[doc(hidden)]
pub enum NodeError {
    StorageFailure,
    // Returned if the "State Machine Safety" property is violated
    SafetyViolation,
}

enum Role<A: Application> {
    Learner,
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState<A>),
}

pub struct NodeActor<A: Application> {
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
        // Run initialization
        addr.init();

        Ok(())
    }
}

impl<A: Application> NodeActor<A> {
    pub fn spawn(this_id: NodeId, app: A) -> Addr<Local<Self>> {
        let config = app.config();
        let storage = app.storage();
        let membership = Membership::solo(this_id);
        spawn_actor(Self {
            state: CommonState {
                app,
                this: WeakAddr::default(),
                this_id,
                current_term: Term(0),
                voted_for: None,
                storage,
                timer_token: TimerToken::default(),
                connections: HashMap::new(),
                config,
                uncommitted_membership: membership.clone(),
                uncommitted_entries: VecDeque::new(),
                committed_index: LogIndex::ZERO,
                committed_membership: membership,
                committed_term: Term(0),
                leader_id: None,
            },
            role: Role::Learner,
        })
    }

    fn received_vote(&mut self, from: NodeId) {
        if let Role::Candidate(candidate) = &mut self.role {
            if candidate.add_vote(&self.state.uncommitted_membership, from) {
                self.become_leader();
            }
        }
    }
    async fn become_candidate(&mut self) -> Result<(), NodeError> {
        self.state.current_term.inc();
        self.state.voted_for = Some(self.state.this_id);
        self.role = Role::Candidate(CandidateState::new(&self.state.uncommitted_membership));
        self.state.schedule_election_timeout();
        self.state.save_hard_state().await?;

        // Vote for ourselves
        self.received_vote(self.state.this_id);

        let req = VoteRequest {
            term: self.state.current_term,
            candidate_id: self.state.this_id,
            last_log_index: self.state.last_log_index(),
            last_log_term: self.state.last_log_term(),
        };

        for (&node_id, conn) in &self.state.connections {
            // Don't bother sending vote requests to learners
            if !self
                .state
                .uncommitted_membership
                .is_learner_or_unknown(node_id)
            {
                // Send out the vote request, and spawn a background task to await it
                self.state.this.await_vote_response(
                    req.term,
                    node_id,
                    conn.call_request_vote(req.clone()),
                );
            }
        }
        Ok(())
    }
    fn become_follower(&mut self) {
        self.role = Role::Follower;
        self.state.schedule_election_timeout();
    }
    fn become_learner(&mut self) {
        self.role = Role::Learner;
        self.state.timer_token.inc();
    }
    fn become_leader(&mut self) {
        self.role = Role::Leader(LeaderState::new(&mut self.state));
    }
    async fn validate_term(&mut self, term: Term) -> Result<(), NodeError> {
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            self.state.save_hard_state().await?;
            match self.role {
                Role::Learner => {}
                _ => self.become_follower(),
            }
        }
        Ok(())
    }
    fn update_role_from_membership(&mut self) {
        let committed_learner = self
            .state
            .committed_membership
            .is_learner_or_unknown(self.state.this_id);
        let uncommitted_learner = self
            .state
            .uncommitted_membership
            .is_learner_or_unknown(self.state.this_id);

        match (&self.role, committed_learner, uncommitted_learner) {
            // The Leader -> Learner transition is special, and doesn't occur until the membership
            // change is committed.
            (Role::Leader(_), true, true) => self.become_learner(),

            // Other transitions occur immediately.
            (Role::Candidate(_), _, true) | (Role::Follower, _, false) => self.become_learner(),
            (Role::Learner, _, false) => self.become_follower(),

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
    async fn request_vote(
        &mut self,
        req: VoteRequest,
        res: Sender<VoteResponse>,
    ) -> Result<(), NodeError> {
        self.validate_term(req.term).await?;

        res.send(self.state.handle_vote_request(req).await?).ok();
        Ok(())
    }
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<A>,
        res: Sender<AppendEntriesResponse>,
    ) -> Result<(), NodeError> {
        self.validate_term(req.term).await?;

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
        let original_ids = &self
            .state
            .uncommitted_membership
            .voting_groups
            .last()
            .expect("At least one voting group at all times")
            .members;

        let old_fault_tolerance = (original_ids.len() as u64 - 1) / 2;
        if old_fault_tolerance < req.fault_tolerance {
            // Current cluster is too small to provide desired fault tolerance
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
        let static_ids = original_ids & &req.ids;

        // Check if we need to make the change in multiple steps
        let excessive_changes = (req.fault_tolerance as i64) + 1 - (static_ids.len() as i64);
        if excessive_changes > 0 {
            let added_ids = &req.ids - original_ids;
            let removed_ids = original_ids - &req.ids;

            let proposed_ids = static_ids
                .into_iter()
                .chain(removed_ids.into_iter().take(excessive_changes as usize))
                .chain(added_ids.into_iter().skip(excessive_changes as usize))
                .collect();

            // Requested member change must be done in multiple steps
            res.send(Err(ClientError::SetMembers(
                SetMembersError::InvalidTransition { proposed_ids },
            )))
            .ok();
            return Ok(());
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
                .filter(|&node_id| !leader_state.is_up_to_date(node_id))
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
        let membership = self.state.uncommitted_membership.to_joint(req.ids);

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
    async fn set_learners(
        &mut self,
        req: SetLearnersRequest,
        res: Sender<ClientResult<A>>,
    ) -> Result<(), NodeError> {
        // For simplicity, don't allow learner changes whilst members are changing
        if self.state.is_membership_changing() {
            res.send(Err(ClientError::SetLearners(
                SetLearnersError::InvalidState,
            )))
            .ok();
            return Ok(());
        }

        // Don't allow members to be added as learners
        let existing_members: HashSet<NodeId> = req
            .ids
            .iter()
            .copied()
            .filter(|&node_id| {
                !self
                    .state
                    .uncommitted_membership
                    .is_learner_or_unknown(node_id)
            })
            .collect();

        if !existing_members.is_empty() {
            res.send(Err(ClientError::SetLearners(
                SetLearnersError::ExistingMembers {
                    ids: existing_members,
                },
            )))
            .ok();
            return Ok(());
        }

        // All good, make the change
        let membership = Membership {
            voting_groups: self.state.uncommitted_membership.voting_groups.clone(),
            learners: req.ids,
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
                    learners: req.learners,
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
    fn init(&self);
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
    async fn init(&mut self) -> Result<(), NodeError> {
        if let Some(initial_state) = self
            .state
            .storage
            .call_get_initial_state()
            .await
            .map_err(|_| NodeError::StorageFailure)?
        {
            let loaded_range = self
                .state
                .storage
                .call_get_log_range(
                    initial_state.last_log_applied + 1..initial_state.last_log_index + 1,
                )
                .await
                .map_err(|_| NodeError::StorageFailure)?;

            self.state.current_term = initial_state.current_term;
            self.state.voted_for = initial_state.voted_for;
            self.state.committed_index = initial_state.last_log_index;
            self.state.committed_term = loaded_range.prev_log_term;
            self.state.committed_membership = initial_state.last_membership_applied;
            self.state.uncommitted_entries = loaded_range
                .entries
                .into_iter()
                .map(|entry| UncommittedEntry {
                    entry,
                    notify: None,
                })
                .collect();
            self.state.update_membership();
        }

        Ok(())
    }
    async fn set_timeout(self: Addr<Local<NodeActor<A>>>, token: TimerToken, deadline: Instant) {
        delay_until(deadline).await;
        self.timer_tick(token);
    }
    async fn timer_tick(&mut self, token: TimerToken) -> Result<(), NodeError> {
        // Ignore spurious wake-ups from cancelled timeouts
        if token != self.state.timer_token {
            return Ok(());
        }

        match &mut self.role {
            // If we're a learner or leader, there's no timeout
            Role::Learner | Role::Leader(_) => unreachable!(),
            // If we're a follower, the timeout means we should convert to a candidate
            // If we're a candidate, the timeout means we should start a new election
            Role::Follower | Role::Candidate(_) => {
                self.become_candidate().await?;
            }
        }
        Ok(())
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
    async fn record_vote_response(
        &mut self,
        term: Term,
        from: NodeId,
        resp: VoteResponse,
    ) -> Result<(), NodeError> {
        self.validate_term(resp.term).await?;
        if term == self.state.current_term {
            if resp.vote_granted {
                self.received_vote(from)
            }
        }
        Ok(())
    }
    async fn record_term(
        &mut self,
        term: Term,
        node_id: NodeId,
        is_up_to_date: bool,
        res: Sender<()>,
    ) -> Result<(), NodeError> {
        self.validate_term(term).await?;

        if let Role::Leader(leader_state) = &mut self.role {
            leader_state.set_up_to_date(node_id, is_up_to_date);

            res.send(()).ok();
        }
        Ok(())
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
