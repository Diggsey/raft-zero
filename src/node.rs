use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use futures::channel::oneshot;
use log::error;
use thiserror::Error;

use act_zero::runtimes::default::spawn_actor;
use act_zero::timer::Tick;
use act_zero::*;

use crate::commit_state::CommitStateReceiver;
use crate::config::MembershipChangeCond;
use crate::election_state::ElectionState;
use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, BootstrapRequest, ClientError, ClientRequest,
    ClientResponse, EntryMembershipChange, EntryNormal, EntryPayload, InstallSnapshotRequest,
    InstallSnapshotResponse, Membership, PreVoteRequest, PreVoteResponse, ResponseMode,
    SetLearnersError, SetLearnersRequest, SetMembersError, SetMembersRequest, VoteRequest,
    VoteResponse, VotingGroup,
};
use crate::replication_stream::ReplicationStreamActor;
use crate::types::{DatabaseId, LogIndex, NodeId, Term};
use crate::Application;

mod common;
mod leader;

use common::{CommonState, Notifier};
use leader::LeaderState;

pub type ClientResult<A> = Result<
    ClientResponse<<A as Application>::LogResponse>,
    ClientError<<A as Application>::LogError>,
>;

#[async_trait]
pub trait Node<A: Application>: Actor {
    // RPCs from the network
    async fn request_vote(&mut self, req: VoteRequest) -> ActorResult<VoteResponse>;
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<A>,
    ) -> ActorResult<AppendEntriesResponse>;
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> ActorResult<InstallSnapshotResponse>;
    async fn request_pre_vote(&mut self, req: PreVoteRequest) -> ActorResult<PreVoteResponse>;

    // Initial setup
    async fn bootstrap_cluster(&mut self, req: BootstrapRequest) -> ActorResult<ClientResult<A>>;

    // Leader commands
    async fn client_request(
        &mut self,
        req: ClientRequest<A::LogData>,
    ) -> ActorResult<ClientResult<A>>;
    async fn set_members(&mut self, req: SetMembersRequest) -> ActorResult<ClientResult<A>>;
    async fn set_learners(&mut self, req: SetLearnersRequest) -> ActorResult<ClientResult<A>>;
}

pub(crate) struct ReplicationState<A: Application> {
    is_up_to_date: bool,
    addr: Addr<ReplicationStreamActor<A>>,
}

type ReplicationStreamMap<A> = HashMap<NodeId, ReplicationState<A>>;

#[derive(Debug, Error)]
pub(crate) enum NodeError {
    #[error("An error occurred whilst talking to the Storage actor")]
    StorageFailure,
    // Returned if the "State Machine Safety" property is violated
    #[error("A constraint of the Raft algorithm was violated")]
    SafetyViolation,
    #[error("A system operator has made an error")]
    DatabaseMismatch,
}

enum Role<A: Application> {
    Learner,
    Follower,
    Applicant(ElectionState),
    Candidate(ElectionState),
    Leader(LeaderState<A>),
}

impl<A: Application> Role<A> {
    fn is_learner(&self) -> bool {
        if let Role::Learner = self {
            true
        } else {
            false
        }
    }
}

pub struct NodeActor<A: Application> {
    role: Role<A>,
    state: CommonState<A>,
}

#[async_trait]
impl<A: Application> Actor for NodeActor<A> {
    async fn started(&mut self, addr: Addr<Self>) -> ActorResult<()>
    where
        Self: Sized,
    {
        self.state.this = addr.downgrade();

        let hard_state = call!(self.state.storage.init())
            .await
            .map_err(|_| NodeError::StorageFailure)?;

        self.state.current_term = hard_state.current_term;
        self.state.voted_for = hard_state.voted_for;

        self.state.load_log_state().await?;
        self.state.update_observer();

        Produces::ok(())
    }
}

impl<A: Application> NodeActor<A> {
    pub fn spawn(this_id: NodeId, app: A) -> Addr<Self> {
        spawn_actor(Self {
            state: CommonState::new(this_id, app),
            role: Role::Learner,
        })
    }

    fn received_vote(&mut self, from: NodeId) {
        if let Role::Candidate(candidate) = &mut self.role {
            if candidate.add_vote(from) {
                self.become_leader();
            }
        }
    }
    async fn received_pre_vote(&mut self, from: NodeId) -> Result<(), NodeError> {
        if let Role::Applicant(applicant) = &mut self.role {
            if applicant.add_vote(from) {
                self.become_candidate().await?;
            }
        }
        Ok(())
    }
    async fn become_candidate(&mut self) -> Result<(), NodeError> {
        self.state.current_term.inc();
        self.state.voted_for = Some(self.state.this_id);
        self.role = Role::Candidate(ElectionState::new(&self.state.uncommitted_membership));
        self.state.mark_not_leader();
        self.state.schedule_election_timeout();
        self.state.save_hard_state().await?;

        // Vote for ourselves
        self.received_vote(self.state.this_id);

        let req = VoteRequest {
            database_id: self.state.database_id,
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
                let res = call!(conn.request_vote(req.clone()));
                let term = req.term;
                self.state.this.send_fut_with(|addr| async move {
                    if let Ok(resp) = res.await {
                        send!(addr.record_vote_response(term, node_id, resp));
                    }
                });
            }
        }
        Ok(())
    }
    async fn become_applicant(&mut self) -> Result<(), NodeError> {
        self.role = Role::Applicant(ElectionState::new(&self.state.uncommitted_membership));
        self.state.mark_not_leader();
        self.state.schedule_election_timeout();

        self.received_pre_vote(self.state.this_id).await?;

        let req = PreVoteRequest {
            database_id: self.state.database_id,
            next_term: self.state.current_term.next(),
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
                let res = call!(conn.request_pre_vote(req.clone()));
                let current_term = self.state.current_term;
                self.state.this.send_fut_with(|addr| async move {
                    if let Ok(resp) = res.await {
                        send!(addr.record_pre_vote_response(current_term, node_id, resp));
                    }
                });
            }
        }
        Ok(())
    }
    fn become_follower(&mut self) {
        self.role = Role::Follower;
        self.state.mark_not_leader();
        self.state.schedule_election_timeout();
    }
    fn become_learner(&mut self) {
        self.role = Role::Learner;
        self.state.mark_not_leader();
        self.state.clear_election_timeout();
    }
    fn become_leader(&mut self) {
        self.role = Role::Leader(LeaderState::new(&mut self.state));
    }
    async fn acknowledge_term(
        &mut self,
        term: Term,
        is_from_leader: bool,
    ) -> Result<(), NodeError> {
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
            self.state.save_hard_state().await?;
            match self.role {
                // If leader stickiness is enabled, we don't want to revert to follower unless
                // we actually receive an append entries request, otherwise we'll start rejecting
                // other vote requests on the basis that the leader is still healthy.
                Role::Candidate(_) if !is_from_leader && self.state.config.leader_stickiness => {}
                Role::Learner | Role::Applicant(_) | Role::Follower => {}
                Role::Leader(_) | Role::Candidate(_) => self.become_follower(),
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
            (Role::Candidate(_), _, true)
            | (Role::Follower, _, true)
            | (Role::Applicant(_), _, true) => self.become_learner(),
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
                    notify
                        .sender
                        .send(Produces::Value(Err(ClientError::Busy)))
                        .ok();
                }
            }
        } else {
            // Only the leader can respond to client requests
            if let Some(notify) = notify {
                notify
                    .sender
                    .send(Produces::Value(Err(ClientError::NotLeader {
                        leader_id: self.state.leader_id,
                    })))
                    .ok();
            }
        }
        Ok(())
    }
    async fn acknowledge_leader(&mut self, leader_id: NodeId) {
        self.state.leader_id = Some(leader_id);
        if !self.role.is_learner() {
            self.become_follower();
        }
    }
    async fn acknowledge_database_id(&mut self, database_id: DatabaseId) -> Result<(), NodeError> {
        if !self.state.database_id.is_set() {
            self.state.database_id = database_id;
            self.state.save_hard_state().await?;
        }
        if self.state.database_id == database_id {
            Ok(())
        } else {
            Err(NodeError::DatabaseMismatch)
        }
    }
    fn should_betray_leader(&self) -> bool {
        // If leader stickiness is enabled, reject pre-votes unless
        // we haven't heard from the leader in a while.
        if self.state.config.leader_stickiness {
            match &self.role {
                Role::Follower | Role::Leader(_) => false,
                Role::Applicant(_) | Role::Candidate(_) | Role::Learner => true,
            }
        } else {
            true
        }
    }

    fn handle_vote_request(&mut self, req: &VoteRequest) -> bool {
        self.state.can_vote_for(req.term, req.candidate_id)
            && self
                .state
                .is_up_to_date(req.last_log_term, req.last_log_index)
            && self.should_betray_leader()
    }
    fn handle_pre_vote_request(&self, req: &PreVoteRequest) -> bool {
        req.next_term >= self.state.current_term
            && self
                .state
                .is_up_to_date(req.last_log_term, req.last_log_index)
            && self.should_betray_leader()
    }
}

#[async_trait]
impl<A: Application> Node<A> for NodeActor<A> {
    async fn request_vote(&mut self, req: VoteRequest) -> ActorResult<VoteResponse> {
        self.acknowledge_database_id(req.database_id).await?;
        self.acknowledge_term(req.term, false).await?;

        let vote_granted = self.handle_vote_request(&req);
        if vote_granted {
            self.state.voted_for = Some(req.candidate_id);
            self.state.save_hard_state().await?;
        }

        self.state.update_observer();

        Produces::ok(VoteResponse {
            term: self.state.current_term,
            vote_granted,
        })
    }
    async fn request_pre_vote(&mut self, req: PreVoteRequest) -> ActorResult<PreVoteResponse> {
        self.acknowledge_database_id(req.database_id).await?;
        Produces::ok(PreVoteResponse {
            term: self.state.current_term,
            vote_granted: self.handle_pre_vote_request(&req),
        })
    }
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<A>,
    ) -> ActorResult<AppendEntriesResponse> {
        self.acknowledge_database_id(req.database_id).await?;
        self.acknowledge_term(req.term, true).await?;

        // Ignore requests from old terms
        let resp = if req.term != self.state.current_term {
            AppendEntriesResponse {
                success: false,
                term: self.state.current_term,
                conflict_opt: None,
            }
        } else {
            self.acknowledge_leader(req.leader_id).await;
            self.state.handle_append_entries(req).await?
        };

        if resp.success {
            // Check for role changes caused by membership changes
            self.update_role_from_membership();
        }

        self.state.update_observer();

        Produces::ok(resp)
    }
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> ActorResult<InstallSnapshotResponse> {
        self.acknowledge_database_id(req.database_id).await?;
        self.acknowledge_term(req.term, true).await?;

        // Ignore requests from old terms
        if req.term == self.state.current_term {
            self.acknowledge_leader(req.leader_id).await;
            if self.state.handle_install_snapshot(req).await? {
                // Check for role changes caused by membership changes
                self.update_role_from_membership();
            }
        }

        self.state.update_observer();

        Produces::ok(InstallSnapshotResponse {
            term: self.state.current_term,
        })
    }
    // Leader commands
    async fn client_request(
        &mut self,
        req: ClientRequest<A::LogData>,
    ) -> ActorResult<ClientResult<A>> {
        let (tx, rx) = oneshot::channel();

        self.internal_request(
            EntryPayload::Application(EntryNormal { data: req.data }),
            Some(Notifier {
                sender: tx,
                mode: req.response_mode,
            }),
            true,
        )
        .await?;

        self.state.update_observer();

        Ok(Produces::Deferred(rx))
    }
    async fn set_members(&mut self, req: SetMembersRequest) -> ActorResult<ClientResult<A>> {
        // Check destination state for validity
        if req.ids.is_empty() {
            // Empty cluster is not allowed...
            return Produces::ok(Err(ClientError::SetMembers(
                SetMembersError::InvalidMembers,
            )));
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
            return Produces::ok(Err(ClientError::SetMembers(
                SetMembersError::InvalidFaultTolerance,
            )));
        }

        let new_fault_tolerance = (req.ids.len() as u64 - 1) / 2;
        if new_fault_tolerance < req.fault_tolerance {
            // Requested cluster is too small to provide desired fault tolerance
            return Produces::ok(Err(ClientError::SetMembers(
                SetMembersError::InvalidMembers,
            )));
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
            return Produces::ok(Err(ClientError::SetMembers(
                SetMembersError::InvalidTransition { proposed_ids },
            )));
        }

        // Don't allow member changes whilst members are already changing
        if self.state.is_membership_changing() {
            return Produces::ok(Err(ClientError::SetMembers(SetMembersError::InvalidState)));
        }

        // Check that sufficiently many nodes are up-to-date
        if let Role::Leader(leader_state) = &mut self.role {
            let this_id = self.state.this_id;
            let lagging_ids: HashSet<NodeId> = req
                .ids
                .iter()
                .copied()
                .filter(|&node_id| !leader_state.is_up_to_date(this_id, node_id))
                .collect();
            let num_up_to_date = req.ids.len() - lagging_ids.len();
            let min_up_to_date =
                num_up_to_date > 0 && (((num_up_to_date - 1) / 2) as u64) >= req.fault_tolerance;

            let allowed_by_cond = match self.state.config.membership_change_cond {
                MembershipChangeCond::MinimumUpToDate => min_up_to_date,
                MembershipChangeCond::NewUpToDate => {
                    min_up_to_date
                        && !lagging_ids.iter().any(|&lagging_id| {
                            self.state
                                .uncommitted_membership
                                .is_learner_or_unknown(lagging_id)
                        })
                }
                MembershipChangeCond::AllUpToDate => lagging_ids.is_empty(),
            };
            if !allowed_by_cond {
                // Too many lagging members
                return Produces::ok(Err(ClientError::SetMembers(
                    SetMembersError::LaggingMembers { ids: lagging_ids },
                )));
            }
        }

        // Everything is good to go, build the new membership configuration!
        let membership = self.state.uncommitted_membership.to_joint(req.ids);

        let (tx, rx) = oneshot::channel();
        self.internal_request(
            EntryPayload::MembershipChange(EntryMembershipChange { membership }),
            Some(Notifier {
                sender: tx,
                mode: ResponseMode::Applied,
            }),
            true,
        )
        .await?;

        self.state.update_observer();
        Ok(Produces::Deferred(rx))
    }
    async fn set_learners(&mut self, req: SetLearnersRequest) -> ActorResult<ClientResult<A>> {
        // For simplicity, don't allow learner changes whilst members are changing
        if self.state.is_membership_changing() {
            return Produces::ok(Err(ClientError::SetLearners(
                SetLearnersError::InvalidState,
            )));
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
            return Produces::ok(Err(ClientError::SetLearners(
                SetLearnersError::ExistingMembers {
                    ids: existing_members,
                },
            )));
        }

        // All good, make the change
        let membership = Membership {
            voting_groups: self.state.uncommitted_membership.voting_groups.clone(),
            learners: req.ids,
        };

        let (tx, rx) = oneshot::channel();
        self.internal_request(
            EntryPayload::MembershipChange(EntryMembershipChange { membership }),
            Some(Notifier {
                sender: tx,
                mode: ResponseMode::Applied,
            }),
            true,
        )
        .await?;

        self.state.update_observer();
        Ok(Produces::Deferred(rx))
    }
    async fn bootstrap_cluster(&mut self, req: BootstrapRequest) -> ActorResult<ClientResult<A>> {
        let cluster_initialized = self.state.database_id.is_set()
            || self.state.last_log_index() != LogIndex::ZERO
            || self.state.voted_for.is_some();
        assert!(
            !cluster_initialized,
            "Cannot bootstrap cluster once it's already been initialized"
        );

        self.state.database_id = DatabaseId::new();
        self.state.voted_for = Some(self.state.this_id);
        self.state.save_hard_state().await?;

        let (tx, rx) = oneshot::channel();
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
                sender: tx,
                mode: ResponseMode::Applied,
            }),
            false,
        )
        .await?;

        self.state.update_observer();
        Ok(Produces::Deferred(rx))
    }
}

#[async_trait]
impl<A: Application> CommitStateReceiver for NodeActor<A> {
    async fn set_commit_index(&mut self, term: Term, commit_index: LogIndex) -> ActorResult<()> {
        if let Role::Leader(leader_state) = &mut self.role {
            if self.state.current_term == term {
                self.state.set_commit_index(commit_index).await;

                // Update replication streams with the commit index
                leader_state.update_replication_state(&self.state);

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
                self.state.update_observer();
            }
        }
        Produces::ok(())
    }
}

impl<A: Application> NodeActor<A> {
    async fn record_vote_response(
        &mut self,
        term: Term,
        from: NodeId,
        resp: VoteResponse,
    ) -> ActorResult<()> {
        self.acknowledge_term(resp.term, false).await?;
        if term == self.state.current_term {
            if resp.vote_granted {
                self.received_vote(from)
            }
        }
        self.state.update_observer();
        Produces::ok(())
    }
    async fn record_pre_vote_response(
        &mut self,
        term: Term,
        from: NodeId,
        resp: PreVoteResponse,
    ) -> ActorResult<()> {
        self.acknowledge_term(resp.term, false).await?;
        if term == self.state.current_term {
            if resp.vote_granted {
                self.received_pre_vote(from).await?;
            }
        }
        self.state.update_observer();
        Produces::ok(())
    }
    pub(crate) async fn record_term(
        &mut self,
        term: Term,
        node_id: NodeId,
        is_up_to_date: bool,
    ) -> ActorResult<()> {
        self.acknowledge_term(term, false).await?;

        if let Role::Leader(leader_state) = &mut self.role {
            leader_state.set_up_to_date(node_id, is_up_to_date);
        }
        self.state.update_observer();
        Produces::ok(())
    }
    async fn send_log_response(
        &self,
        term: Term,
        index: LogIndex,
        receiver: Produces<A::LogResponse>,
    ) -> ActorResult<ClientResult<A>> {
        // Wait for the log entry to be applied, and the forward the response to the client.
        if let Ok(data) = receiver.await {
            Produces::ok(Ok(ClientResponse {
                data: Some(data),
                term,
                index,
            }))
        } else {
            Ok(Produces::None)
        }
    }
    async fn send_blank_entry(&mut self) -> ActorResult<()> {
        // If there are no log entries for our term yet, create a blank one to ensure stuff
        // gets committed promptly.
        if self.state.last_log_term() != self.state.current_term {
            self.internal_request(EntryPayload::Blank, None, false)
                .await?;
            self.state.update_observer();
        }
        Produces::ok(())
    }
}

#[async_trait]
impl<A: Application> Tick for NodeActor<A> {
    async fn tick(&mut self) -> ActorResult<()> {
        if self.state.timer.tick() {
            match &mut self.role {
                // If we're a learner or leader, there's no timeout
                Role::Learner | Role::Leader(_) => unreachable!(),
                // If we're a follower or applicant and pre-vote is enabled, begin pre-vote
                Role::Follower | Role::Applicant(_) if self.state.config.pre_vote => {
                    self.become_applicant().await?;
                }
                // If we're a follower, the timeout means we should convert to a candidate
                // If we're a candidate, the timeout means we should start a new election
                Role::Follower | Role::Applicant(_) | Role::Candidate(_) => {
                    self.become_candidate().await?;
                }
            }

            self.state.update_observer();
        }
        Produces::ok(())
    }
}
