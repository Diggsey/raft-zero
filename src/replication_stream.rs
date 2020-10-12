use std::sync::Arc;

use thiserror::Error;

use act_zero::runtimes::default::Timer;
use act_zero::timer::Tick;
use act_zero::*;
use async_trait::async_trait;

use crate::commit_state::CommitStateActor;
use crate::config::Config;
use crate::connection::Connection;
use crate::messages::{AppendEntriesResponse, Entry, InstallSnapshotResponse};
use crate::node::NodeActor;
use crate::storage::{Snapshot, Storage};
use crate::types::{DatabaseId, LogIndex, NodeId, Term};
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

#[async_trait]
pub(crate) trait ReplicationStream<A: Application> {
    async fn append_entry(
        &mut self,
        leader_state: LeaderState,
        entry: Arc<Entry<A::LogData>>,
    ) -> ActorResult<()>;
    async fn update_leader_state(&mut self, leader_state: LeaderState) -> ActorResult<()>;
}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("Node stopping")]
    Stopping,
    #[error("Storage actor returned an error")]
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
    this: WeakAddr<ReplicationStreamActor<A>>,
    leader_state: LeaderState,
    config: Arc<Config>,
    connection: Addr<dyn Connection<A>>,
    storage: Addr<dyn Storage<A>>,
    commit_state: Addr<CommitStateActor>,
    awaiting_response: bool,
    database_id: DatabaseId,
    leader_id: NodeId,
    node_id: NodeId,
    timer: Timer,
}

pub(crate) struct ReplicationStreamActor<A: Application> {
    owner: WeakAddr<NodeActor<A>>,
    mode: ReplicationMode<A>,
    shared: SharedState<A>,
}

impl<A: Application> ReplicationStreamActor<A> {
    pub(crate) fn new(
        node_id: NodeId,
        owner: WeakAddr<NodeActor<A>>,
        leader_id: NodeId,
        database_id: DatabaseId,
        leader_state: LeaderState,
        config: Arc<Config>,
        connection: Addr<dyn Connection<A>>,
        storage: Addr<dyn Storage<A>>,
        commit_state: Addr<CommitStateActor>,
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
                database_id,
                leader_state,
                config,
                connection,
                storage,
                awaiting_response: false,
                commit_state,
                leader_id,
                timer: Default::default(),
            },
        }
    }
    async fn handle_response(&mut self, term: Term, is_up_to_date: bool) -> ActorResult<()> {
        // Regardless of result, record the term
        call!(self
            .owner
            .record_term(term, self.shared.node_id, is_up_to_date))
        .await
        .map_err(|_| ReplicationError::Stopping)?;
        self.shared.awaiting_response = false;
        Produces::ok(())
    }
    async fn handle_mode_change(
        &mut self,
        mut arg: Result<(), ErrorOrChange<A>>,
    ) -> ActorResult<()> {
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
                let box_read = call!(self.shared.storage.read_snapshot(snapshot.id.clone()))
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
        Produces::ok(())
    }
}

#[async_trait]
impl<A: Application> Actor for ReplicationStreamActor<A> {
    async fn started(&mut self, addr: Addr<Self>) -> ActorResult<()>
    where
        Self: Sized,
    {
        self.shared.this = addr.downgrade();
        Produces::ok(())
    }
}

#[async_trait]
impl<A: Application> ReplicationStream<A> for ReplicationStreamActor<A> {
    // The leader state should have the entry being appended as its last log index
    async fn append_entry(
        &mut self,
        leader_state: LeaderState,
        entry: Arc<Entry<A::LogData>>,
    ) -> ActorResult<()> {
        self.shared.leader_state = leader_state;
        if let ReplicationMode::Log(log_replication) = &mut self.mode {
            log_replication.append_entry(&mut self.shared, entry).await;
        }
        Produces::ok(())
    }
    async fn update_leader_state(&mut self, leader_state: LeaderState) -> ActorResult<()> {
        self.shared.leader_state = leader_state;
        if let ReplicationMode::Log(log_replication) = &mut self.mode {
            if !self.shared.awaiting_response {
                log_replication.flush_buffer(&mut self.shared);
            }
        }
        Produces::ok(())
    }
}

#[async_trait]
impl<A: Application> Tick for ReplicationStreamActor<A> {
    async fn tick(&mut self) -> ActorResult<()> {
        if self.shared.timer.tick() {
            if self.shared.awaiting_response {
                self.handle_missed_response().await?;
            }
            // It's time to send a heartbeat, even if the buffer is empty
            self.mode.flush_buffer(&mut self.shared);
        }
        Produces::ok(())
    }
}

impl<A: Application> ReplicationStreamActor<A> {
    async fn handle_append_entries_response(
        &mut self,
        count: u64,
        resp: AppendEntriesResponse,
    ) -> ActorResult<()> {
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
    async fn handle_install_snapshot_response(
        &mut self,
        resp: InstallSnapshotResponse,
    ) -> ActorResult<()> {
        self.handle_response(resp.term, false).await?;

        let res = self
            .mode
            .snapshot_replication()
            .handle_install_snapshot_response(&mut self.shared)
            .await;

        // Check if we need to switch replication modes
        self.handle_mode_change(res).await
    }
    async fn handle_missed_response(&mut self) -> ActorResult<()> {
        self.shared.awaiting_response = false;
        call!(self.owner.record_term(Term(0), self.shared.node_id, false))
            .await
            .map_err(|_| ReplicationError::Stopping)?;
        Produces::ok(())
    }
}
