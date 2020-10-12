use std::collections::HashMap;
use std::sync::Arc;

use act_zero::runtimes::default::spawn_actor;
use act_zero::*;

use crate::commit_state::{CommitState, CommitStateActor};
use crate::messages::{ClientError, Entry, EntryPayload};
use crate::replication_stream::{self, ReplicationStream, ReplicationStreamActor};
use crate::types::NodeId;
use crate::Application;

use super::common::{CommonState, Notifier, UncommittedEntry};
use super::{NodeError, ReplicationState, ReplicationStreamMap};

pub(crate) struct LeaderState<A: Application> {
    commit_state: Addr<CommitStateActor>,
    replication_streams: ReplicationStreamMap<A>,
}

fn build_replication_streams<A: Application>(
    state: &CommonState<A>,
    state_for_rep: replication_stream::LeaderState,
    commit_state: &Addr<CommitStateActor>,
    prev_streams: &mut ReplicationStreamMap<A>,
) -> ReplicationStreamMap<A> {
    state
        .connections
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
                            state.this.clone(),
                            state.this_id,
                            state.database_id,
                            state_for_rep.clone(),
                            state.config.clone(),
                            conn.clone(),
                            state.storage.clone(),
                            commit_state.clone(),
                        )),
                    }),
            )
        })
        .collect()
}

impl<A: Application> LeaderState<A> {
    pub(crate) fn new(state: &mut CommonState<A>) -> Self {
        // Cancel any previous timeouts
        state.clear_election_timeout();
        state.leader_id = Some(state.this_id);

        // Start tracking our commit state
        let commit_state = spawn_actor(CommitStateActor::new(
            state.current_term,
            state.uncommitted_membership.clone(),
            state.committed_index,
            upcast!(state.this.clone()),
        ));

        // Replicate a blank entry on election to ensure we have an entry from our own term
        send!(state.this.send_blank_entry());

        let state_for_rep = state.state_for_replication();

        Self {
            replication_streams: build_replication_streams(
                state,
                state_for_rep,
                &commit_state,
                &mut HashMap::new(),
            ),
            commit_state,
        }
    }
    pub(crate) async fn append_entry(
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
        if let Err(e) = call!(state.storage.append_entry_to_log(entry.clone()))
            .await
            .map_err(|_| NodeError::StorageFailure)?
        {
            // Entry rejected by application, report the error to the caller
            if let Some(notify) = notify {
                notify
                    .sender
                    .send(Produces::Value(Err(ClientError::Application(e))))
                    .ok();
            }
            return Ok(());
        }

        // If this adds new members, we need to build their replication state
        // based on the state prior to this entry being added, so save that here.
        let prev_replication_state = state.state_for_replication();

        // Add this to our list of uncommmitted entries
        state.uncommitted_entries.push_back(UncommittedEntry {
            entry: entry.clone(),
            notify,
        });

        // If this was a membership change, update our membership
        if entry.is_membership_change() {
            state.update_membership();
            send!(self
                .commit_state
                .set_membership(state.uncommitted_membership.clone()));

            // Build the replication streams using the old replication state
            self.replication_streams = build_replication_streams(
                state,
                prev_replication_state.clone(),
                &self.commit_state,
                &mut self.replication_streams,
            );
        }

        // Replicate entry to other nodes
        let new_replication_state = state.state_for_replication();
        for rs in self.replication_streams.values() {
            send!(rs
                .addr
                .append_entry(new_replication_state.clone(), entry.clone()));
        }

        // Update our own match index
        send!(self.commit_state.set_match_index(
            state.this_id,
            state.last_log_index(),
            state.last_log_term()
        ));

        Ok(())
    }

    pub(crate) fn update_replication_state(&self, state: &CommonState<A>) {
        let new_replication_state = state.state_for_replication();
        for rs in self.replication_streams.values() {
            send!(rs.addr.update_leader_state(new_replication_state.clone()));
        }
    }

    pub(crate) fn is_up_to_date(&self, this_id: NodeId, node_id: NodeId) -> bool {
        if node_id == this_id {
            true
        } else if let Some(rs) = self.replication_streams.get(&node_id) {
            rs.is_up_to_date
        } else {
            false
        }
    }

    pub(crate) fn set_up_to_date(&mut self, node_id: NodeId, is_up_to_date: bool) {
        if let Some(rs) = self.replication_streams.get_mut(&node_id) {
            rs.is_up_to_date = is_up_to_date;
        }
    }
}
