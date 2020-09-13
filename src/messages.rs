use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::types::{LogIndex, NodeId, Term};
use crate::{Application, LogData, LogResponse};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest<A: Application> {
    /// The leader's current term.
    pub term: Term,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: NodeId,
    /// The index of the log entry immediately preceding the new entries.
    pub prev_log_index: LogIndex,
    /// The term of the `prev_log_index` entry.
    pub prev_log_term: Term,
    /// The new log entries to store.
    ///
    /// This may be empty when the leader is sending heartbeats. Entries
    /// may be batched for efficiency.
    #[serde(bound(
        serialize = "A::LogData: Serialize",
        deserialize = "A::LogData: Deserialize<'de>"
    ))]
    pub entries: Vec<Arc<Entry<A::LogData>>>,
    /// The leader's commit index.
    pub leader_commit: LogIndex,
}

/// An RPC response to an `AppendEntriesRequest` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The responding node's current term, for leader to update itself.
    pub term: Term,
    /// Will be true if follower contained entry matching `prev_log_index` and `prev_log_term`.
    pub success: bool,
    /// A value used to implement the _conflicting term_ optimization outlined in §5.3.
    ///
    /// This value will only be present, and should only be considered, when `success` is `false`.
    pub conflict_opt: Option<ConflictOpt>,
}

/// A struct used to implement the _conflicting term_ optimization outlined in §5.3 for log replication.
///
/// This value will only be present, and should only be considered, when an `AppendEntriesResponse`
/// object has a `success` value of `false`.
///
/// This implementation of Raft uses this value to more quickly synchronize a leader with its
/// followers which may be some distance behind in replication, may have conflicting entries, or
/// which may be new to the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictOpt {
    /// The index of the most recent entry which does not conflict with the received request.
    pub index: LogIndex,
}

/// An RPC invoked by candidates to gather votes (§5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    /// The candidate's current term.
    pub term: Term,
    /// The candidate's ID.
    pub candidate_id: NodeId,
    /// The index of the candidate’s last log entry (§5.4).
    pub last_log_index: LogIndex,
    /// The term of the candidate’s last log entry (§5.4).
    pub last_log_term: Term,
}

/// An RPC response to a `VoteRequest` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    /// The current term of the responding node, for the candidate to update itself.
    pub term: Term,
    /// Will be true if the candidate received a vote from the responder.
    pub vote_granted: bool,
}

/// An RPC invoked by candidates to gather votes (§5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteRequest {
    /// The term which we will enter if the pre-vote succeeds.
    pub next_term: Term,
    /// The candidate's ID.
    pub candidate_id: NodeId,
    /// The index of the candidate’s last log entry (§5.4).
    pub last_log_index: LogIndex,
    /// The term of the candidate’s last log entry (§5.4).
    pub last_log_term: Term,
}

/// An RPC response to a `PreVoteRequest` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteResponse {
    /// The current term of the responding node, for the candidate to update itself.
    pub term: Term,
    /// Will be true if the candidate received a vote from the responder.
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// The leader's current term.
    pub term: Term,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: NodeId,
    /// The snapshot replaces all log entries up through and including this index.
    pub last_included_index: LogIndex,
    /// The term of the `last_included_index`.
    pub last_included_term: Term,
    /// The byte offset where chunk is positioned in the snapshot file.
    pub offset: u64,
    /// The raw Vec<u8> of the snapshot chunk, starting at `offset`.
    pub data: Vec<u8>,
    /// Will be `true` if this is the last chunk in the snapshot.
    pub done: bool,
}

/// An RPC response to an `InstallSnapshotRequest` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// The receiving node's current term, for leader to update itself.
    pub term: Term,
}

/// A Raft log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry<D: LogData> {
    /// This entry's term.
    pub term: Term,
    /// This entry's payload.
    pub payload: EntryPayload<D>,
}

impl<D: LogData> Entry<D> {
    pub fn is_membership_change(&self) -> bool {
        if let EntryPayload::MembershipChange(_) = self.payload {
            true
        } else {
            false
        }
    }
}

/// Log entry payload variants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EntryPayload<D: LogData> {
    /// An empty payload committed by a new cluster leader.
    Blank,
    /// A normal log entry.
    Application(EntryNormal<D>),
    /// A membership change log entry.
    MembershipChange(EntryMembershipChange),
}

/// A normal log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryNormal<D: LogData> {
    /// The contents of this entry.
    pub data: D,
}

/// A log entry holding a config change.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryMembershipChange {
    /// The full list of node IDs to be considered cluster members as part of this config change.
    pub membership: Membership,
}

/// Each voting group reaches consensus independently. Nodes may belong to multiple
/// voting groups.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VotingGroup {
    pub members: HashSet<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct Membership {
    /// During joint consensus, there will be two voting groups. The old group will be first, followed
    /// by the new group.
    pub voting_groups: Vec<VotingGroup>,
    /// List of nodes which are not part of a voting group.
    pub learners: HashSet<NodeId>,
}

impl Membership {
    pub fn empty() -> Self {
        Self::default()
    }
    pub fn solo(node_id: NodeId) -> Self {
        Self {
            voting_groups: Vec::new(),
            learners: Some(node_id).into_iter().collect(),
        }
    }
    pub fn is_joint(&self) -> bool {
        self.voting_groups.len() > 1
    }
    /// Should only be called when in joint consensus
    pub fn to_single(&self) -> Membership {
        let new_learners = &self.voting_groups[0].members - &self.voting_groups[1].members;
        Membership {
            voting_groups: vec![self.voting_groups[1].clone()],
            learners: &self.learners | &new_learners,
        }
    }
    /// Should only be called when not in joint consensus
    pub fn to_joint(&self, new_members: HashSet<NodeId>) -> Membership {
        Membership {
            learners: &self.learners - &new_members,
            voting_groups: vec![
                self.voting_groups[0].clone(),
                VotingGroup {
                    members: new_members,
                },
            ],
        }
    }
    pub fn map_members<V>(
        &self,
        omit_id: Option<NodeId>,
        omit_learners: bool,
        mut f: impl FnMut(NodeId) -> V,
    ) -> HashMap<NodeId, V> {
        let mut result = HashMap::new();
        for group in &self.voting_groups {
            for &member in &group.members {
                if Some(member) != omit_id {
                    result.entry(member).or_insert_with(|| f(member));
                }
            }
        }
        if !omit_learners {
            for &member in &self.learners {
                if Some(member) != omit_id {
                    result.entry(member).or_insert_with(|| f(member));
                }
            }
        }
        result
    }
    pub fn is_learner_or_unknown(&self, node_id: NodeId) -> bool {
        !self
            .voting_groups
            .iter()
            .any(|group| group.members.contains(&node_id))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ResponseMode {
    Committed,
    Applied,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRequest<D: LogData> {
    pub data: D,
    pub response_mode: ResponseMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientResponse<R: LogResponse> {
    pub term: Term,
    pub index: LogIndex,
    /// Only present if the entry has been committed
    pub data: Option<R>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientError<E> {
    NotLeader { leader_id: Option<NodeId> },
    Busy,
    Application(E),
    SetMembers(SetMembersError),
    SetLearners(SetLearnersError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetMembersRequest {
    /// These IDs must correspond to existing members or learners.
    pub ids: HashSet<NodeId>,
    /// Number of node failures we should remain tolerant to at all times.
    pub fault_tolerance: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SetMembersError {
    /// Cannot change members at this time. Possibly because a membership
    /// change is already in progress.
    InvalidState,
    /// The requested members would produce a cluster which is not sufficiently
    /// fault tolerant.
    InvalidMembers,
    /// The existing cluster already fails to meet the requested fault tolerance.
    InvalidFaultTolerance,
    /// One or more of the requested members were unknown to the leader. New
    /// members must be added as learners first.
    UnknownMembers { ids: HashSet<NodeId> },
    /// Too many of the requested members are lagging too far behind to
    /// safely continue.
    LaggingMembers { ids: HashSet<NodeId> },
    /// The cluster could not transition directly to the requested set of
    /// members whilst remaining sufficiently fault tolerant.
    InvalidTransition {
        /// A proposed set of members which we could transition to whilst
        /// remaining fault tolerant, and which takes us closer to the
        /// requested set of members.
        proposed_ids: HashSet<NodeId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetLearnersRequest {
    /// These IDs must not correspond to existing members. To convert an
    /// existing member to a learner, you must remove it as a member,
    /// at which point it will automatically become a learner.
    pub ids: HashSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SetLearnersError {
    /// Cannot change learners at this time. Possibly because a membership
    /// change is already in progress.
    InvalidState,
    /// One or more of the requested learners is a member.
    ExistingMembers { ids: HashSet<NodeId> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapRequest {
    pub members: HashSet<NodeId>,
    pub learners: HashSet<NodeId>,
}
