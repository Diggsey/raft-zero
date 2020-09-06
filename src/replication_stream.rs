use std::sync::Arc;

use tokio::time::{delay_until, timeout_at, Instant};

use act_zero::*;

use crate::commit_state::CommitStateActor;
use crate::config::Config;
use crate::connection::Connection;
use crate::messages::{AppendEntriesResponse, Entry, InstallSnapshotResponse};
use crate::node::{NodeActor, PrivateNodeExt};
use crate::storage::{Snapshot, Storage, StorageExt};
use crate::timer::TimerToken;
use crate::types::{LogIndex, NodeId, Term};
use crate::Application;

mod log_replication;
mod snapshot_replication;

use log_replication::LogReplication;
use snapshot_replication::SnapshotReplication;

#[derive(Clone, Debug)]
pub(crate) struct LeaderState {
    pub term: Term,
    pub commit_index: LogIndex,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[act_zero]
pub(crate) trait ReplicationStream<A: Application> {
    fn append_entry(&self, leader_state: LeaderState, entry: Arc<Entry<A::LogData>>);
    fn update_leader_state(&self, leader_state: LeaderState);
}

#[act_zero]
trait PrivateReplicationStream {
    fn timer_tick(&self, token: TimerToken);
    fn await_append_entries_response(
        &self,
        count: u64,
        receiver: Receiver<AppendEntriesResponse>,
        token: TimerToken,
        deadline: Instant,
    );
    fn handle_append_entries_response(&self, count: u64, resp: AppendEntriesResponse);
    fn await_install_snapshot_response(
        &self,
        receiver: Receiver<InstallSnapshotResponse>,
        token: TimerToken,
        deadline: Instant,
    );
    fn handle_install_snapshot_response(&self, resp: InstallSnapshotResponse);
}

pub enum ReplicationError {
    Stopping,
    StorageFailure,
}

enum ErrorOrChange<A: Application> {
    Error(ReplicationError),
    Change(ChangeMode<A>),
}

enum ChangeMode<A: Application> {
    Log {
        match_index: LogIndex,
        match_term: Term,
    },
    Snapshot {
        snapshot: Snapshot<A::SnapshotId>,
    },
}

enum ReplicationMode<A: Application> {
    Log(LogReplication<A>),
    Snapshot(SnapshotReplication<A>),
}

impl<A: Application> ReplicationMode<A> {
    fn is_up_to_date(&self, shared: &SharedState<A>) -> bool {
        match self {
            Self::Log(log_replication) => log_replication.is_up_to_date(shared),
            Self::Snapshot(_) => false,
        }
    }
    async fn fill_buffer(&mut self, shared: &mut SharedState<A>) -> Result<(), ErrorOrChange<A>> {
        match self {
            Self::Log(log_replication) => log_replication.fill_buffer(shared).await,
            Self::Snapshot(snapshot_replication) => snapshot_replication.fill_buffer().await,
        }
    }
    fn flush_buffer(&mut self, shared: &mut SharedState<A>) {
        match self {
            Self::Log(log_replication) => log_replication.flush_buffer(shared),
            Self::Snapshot(snapshot_replication) => snapshot_replication.flush_buffer(shared),
        }
    }
    async fn kickstart(&mut self, shared: &mut SharedState<A>) -> Result<(), ErrorOrChange<A>> {
        self.fill_buffer(shared).await?;
        self.flush_buffer(shared);
        Ok(())
    }
    fn log_replication(&mut self) -> &mut LogReplication<A> {
        match self {
            Self::Log(log_replication) => log_replication,
            Self::Snapshot(_) => unreachable!(),
        }
    }
    fn snapshot_replication(&mut self) -> &mut SnapshotReplication<A> {
        match self {
            Self::Log(_) => unreachable!(),
            Self::Snapshot(snapshot_replication) => snapshot_replication,
        }
    }
}

struct SharedState<A: Application> {
    this: WeakAddr<Local<ReplicationStreamActor<A>>>,
    leader_state: LeaderState,
    config: Arc<Config>,
    connection: Addr<dyn Connection<A>>,
    storage: Addr<dyn Storage<A>>,
    commit_state: Addr<Local<CommitStateActor>>,
    awaiting_response: bool,
    leader_id: NodeId,
    node_id: NodeId,
    timer_token: TimerToken,
}

pub(crate) struct ReplicationStreamActor<A: Application> {
    owner: WeakAddr<Local<NodeActor<A>>>,
    mode: ReplicationMode<A>,
    shared: SharedState<A>,
}

impl<A: Application> ReplicationStreamActor<A> {
    pub(crate) fn new(
        node_id: NodeId,
        owner: WeakAddr<Local<NodeActor<A>>>,
        leader_id: NodeId,
        leader_state: LeaderState,
        config: Arc<Config>,
        connection: Addr<dyn Connection<A>>,
        storage: Addr<dyn Storage<A>>,
        commit_state: Addr<Local<CommitStateActor>>,
    ) -> Self {
        Self {
            owner,
            mode: ReplicationMode::Log(LogReplication::new(
                &config,
                leader_state.last_log_index,
                leader_state.last_log_term,
            )),
            shared: SharedState {
                this: WeakAddr::default(),
                node_id,
                leader_state,
                config,
                connection,
                storage,
                awaiting_response: false,
                commit_state,
                leader_id,
                timer_token: TimerToken::default(),
            },
        }
    }
    async fn handle_response(
        &mut self,
        term: Term,
        is_up_to_date: bool,
    ) -> Result<(), ReplicationError> {
        // Regardless of result, record the term
        self.owner
            .call_record_term(term, self.shared.node_id, is_up_to_date)
            .await
            .map_err(|_| ReplicationError::Stopping)?;
        self.shared.awaiting_response = false;
        Ok(())
    }
    async fn handle_mode_change(
        &mut self,
        mut arg: Result<(), ErrorOrChange<A>>,
    ) -> Result<(), ReplicationError> {
        while match arg {
            Err(ErrorOrChange::Change(ChangeMode::Log {
                match_index,
                match_term,
            })) => {
                self.mode = ReplicationMode::Log(LogReplication::new(
                    &self.shared.config,
                    match_index,
                    match_term,
                ));
                Ok(true)
            }
            Err(ErrorOrChange::Change(ChangeMode::Snapshot { snapshot })) => {
                let box_read = self
                    .shared
                    .storage
                    .call_read_snapshot(snapshot.id.clone())
                    .await
                    .map_err(|_| ReplicationError::StorageFailure)?;
                self.mode = ReplicationMode::Snapshot(SnapshotReplication::new(
                    &self.shared.config,
                    snapshot,
                    box_read,
                ));
                Ok(true)
            }
            Err(ErrorOrChange::Error(e)) => Err(e),
            Ok(()) => Ok(false),
        }? {
            arg = self.mode.kickstart(&mut self.shared).await;
        }
        Ok(())
    }
}

impl<A: Application> Actor for ReplicationStreamActor<A> {
    type Error = ReplicationError;

    fn started(&mut self, addr: Addr<Local<Self>>) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        self.shared.this = addr.downgrade();
        Ok(())
    }
}

#[act_zero]
impl<A: Application> ReplicationStream<A> for ReplicationStreamActor<A> {
    // The leader state should have the entry being appended as its last log index
    async fn append_entry(&mut self, leader_state: LeaderState, entry: Arc<Entry<A::LogData>>) {
        self.shared.leader_state = leader_state;
        if let ReplicationMode::Log(log_replication) = &mut self.mode {
            log_replication.append_entry(&mut self.shared, entry).await;
        }
    }
    async fn update_leader_state(&mut self, leader_state: LeaderState) {
        self.shared.leader_state = leader_state;
        if let ReplicationMode::Log(log_replication) = &mut self.mode {
            if !self.shared.awaiting_response {
                log_replication.flush_buffer(&mut self.shared);
            }
        }
    }
}

#[act_zero]
impl<A: Application> PrivateReplicationStream for ReplicationStreamActor<A> {
    async fn timer_tick(&mut self, token: TimerToken) {
        // Ignore spurious wake-ups from cancelled timeouts
        if token != self.shared.timer_token {
            return;
        }

        // It's time to send a heartbeat, even if the buffer is empty
        self.mode.flush_buffer(&mut self.shared);
    }
    async fn await_append_entries_response(
        self: Addr<Local<ReplicationStreamActor<A>>>,
        count: u64,
        receiver: Receiver<AppendEntriesResponse>,
        token: TimerToken,
        deadline: Instant,
    ) {
        // Wait for the RPC response, at least until deadline has expired
        if let Ok(Ok(res)) = timeout_at(deadline, receiver).await {
            self.handle_append_entries_response(count, res);
        }
        // Wait for deadline
        delay_until(deadline).await;
        self.timer_tick(token);
    }
    async fn handle_append_entries_response(
        &mut self,
        count: u64,
        resp: AppendEntriesResponse,
    ) -> Result<(), ReplicationError> {
        // Regardless of result, record the term
        let is_up_to_date = resp.success && self.mode.is_up_to_date(&self.shared);
        self.handle_response(resp.term, is_up_to_date).await?;

        let res = self
            .mode
            .log_replication()
            .handle_append_entries_response(&mut self.shared, count, resp)
            .await;

        // Check if we need to switch replication modes
        self.handle_mode_change(res).await
    }
    async fn await_install_snapshot_response(
        self: Addr<Local<ReplicationStreamActor<A>>>,
        receiver: Receiver<InstallSnapshotResponse>,
        token: TimerToken,
        deadline: Instant,
    ) {
        // Wait for the RPC response, at least until deadline has expired
        if let Ok(Ok(res)) = timeout_at(deadline, receiver).await {
            self.handle_install_snapshot_response(res);
        } else {
            // Wait for deadline
            delay_until(deadline).await;
            self.timer_tick(token);
        }
    }
    async fn handle_install_snapshot_response(
        &mut self,
        resp: InstallSnapshotResponse,
    ) -> Result<(), ReplicationError> {
        self.handle_response(resp.term, false).await?;

        let res = self
            .mode
            .snapshot_replication()
            .handle_install_snapshot_response(&mut self.shared)
            .await;

        // Check if we need to switch replication modes
        self.handle_mode_change(res).await
    }
}
