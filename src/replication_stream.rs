use std::sync::Arc;

use tokio::time::{delay_until, timeout_at, Instant};

use act_zero::*;

use crate::commit_state::{CommitStateActor, CommitStateExt};
use crate::config::Config;
use crate::connection::{Connection, ConnectionExt};
use crate::messages::{AppendEntriesRequest, AppendEntriesResponse, Entry};
use crate::node::{NodeActor, PrivateNodeExt};
use crate::seekable_buffer::{SeekDir, SeekableBuffer};
use crate::storage::{Storage, StorageExt};
use crate::timer::TimerToken;
use crate::types::{LogIndex, NodeId, Term};
use crate::Application;

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
    fn await_response(
        &self,
        count: u64,
        receiver: Receiver<AppendEntriesResponse>,
        token: TimerToken,
        deadline: Instant,
    );
    fn handle_response(&self, count: u64, resp: AppendEntriesResponse);
}

pub enum ReplicationError {
    Stopping,
    StorageFailure,
}

pub(crate) struct ReplicationStreamActor<A: Application> {
    this: WeakAddr<Local<Self>>,
    node_id: NodeId,
    owner: WeakAddr<Local<NodeActor<A>>>,
    leader_id: NodeId,
    leader_state: LeaderState,
    prev_log_term: Term,
    buffer: SeekableBuffer<Arc<Entry<A::LogData>>>,
    config: Arc<Config>,
    timer_token: TimerToken,
    awaiting_response: bool,
    connection: Addr<dyn Connection<A>>,
    storage: Addr<dyn Storage<A>>,
    commit_state: Addr<Local<CommitStateActor>>,
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
        let mut buffer = SeekableBuffer::new(config.max_replication_buffer_len as usize);
        buffer.seek(leader_state.last_log_index.0 + 1);

        Self {
            this: WeakAddr::default(),
            node_id,
            owner,
            leader_id,
            prev_log_term: leader_state.last_log_term,
            leader_state,
            buffer,
            config,
            timer_token: TimerToken::default(),
            awaiting_response: false,
            connection,
            storage,
            commit_state,
        }
    }
    fn prev_log_index(&self) -> LogIndex {
        LogIndex(self.buffer.pos() - 1)
    }
    async fn adjust_match_index(
        &mut self,
        new_match_index: LogIndex,
    ) -> Result<(), ReplicationError> {
        match self.buffer.seek(new_match_index.0 + 1) {
            SeekDir::Forward(items) => {
                self.prev_log_term = items.last().expect("At least one item").term;
            }
            SeekDir::Backward(mut gap) => {
                let range = gap.range();
                let log_range = self
                    .storage
                    .call_get_log_range(LogIndex(range.start)..LogIndex(range.end))
                    .await
                    .map_err(|_| ReplicationError::StorageFailure)?;
                gap.fill(range.start, log_range.entries);
                self.prev_log_term = log_range.prev_log_term;
            }
            SeekDir::Still => {}
            SeekDir::Far(buffer) => {
                let range = buffer.calc_fill_range(
                    self.config.max_append_entries_len as usize,
                    self.leader_state.last_log_index.0 + 1,
                );
                let log_range = self
                    .storage
                    .call_get_log_range(LogIndex(range.start)..LogIndex(range.end))
                    .await
                    .map_err(|_| ReplicationError::StorageFailure)?;
                buffer.fill(range.start, log_range.entries);
                self.prev_log_term = log_range.prev_log_term;
            }
        }

        // Update the commit state
        self.commit_state
            .set_match_index(self.node_id, new_match_index, self.prev_log_term);

        let range = self.buffer.calc_fill_range(
            self.config.max_append_entries_len as usize,
            self.leader_state.last_log_index.0 + 1,
        );
        if range.end > range.start {
            let log_range = self
                .storage
                .call_get_log_range(LogIndex(range.start)..LogIndex(range.end))
                .await
                .map_err(|_| ReplicationError::StorageFailure)?;
            self.buffer.fill(range.start, log_range.entries);
        }

        Ok(())
    }
    async fn flush_buffer(&mut self) {
        // Send the first N items from the buffer, without removing them
        let entries: Vec<_> = self
            .buffer
            .iter()
            .take(self.config.max_append_entries_len as usize)
            .cloned()
            .collect();
        let count = entries.len() as u64;

        let res = self.connection.call_append_entries(AppendEntriesRequest {
            leader_id: self.leader_id,
            term: self.leader_state.term,
            leader_commit: self.leader_state.commit_index,
            prev_log_index: self.prev_log_index(),
            prev_log_term: self.prev_log_term,
            entries,
        });
        let deadline = Instant::now() + self.config.heartbeat_interval;
        self.awaiting_response = true;
        self.this
            .await_response(count, res, self.timer_token.inc(), deadline);
    }
}

impl<A: Application> Actor for ReplicationStreamActor<A> {
    type Error = ReplicationError;

    fn started(&mut self, addr: Addr<Local<Self>>) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        self.this = addr.downgrade();
        Ok(())
    }
}

#[act_zero]
impl<A: Application> ReplicationStream<A> for ReplicationStreamActor<A> {
    // The leader state should have the entry being appended as its last log index
    async fn append_entry(&mut self, leader_state: LeaderState, entry: Arc<Entry<A::LogData>>) {
        self.leader_state = leader_state;
        if self
            .buffer
            .fill(self.leader_state.last_log_index.0, Some(entry))
            && !self.awaiting_response
        {
            self.flush_buffer().await;
        }
    }
    async fn update_leader_state(&mut self, leader_state: LeaderState) {
        self.leader_state = leader_state;
        if !self.awaiting_response {
            self.flush_buffer().await;
        }
    }
}

#[act_zero]
impl<A: Application> PrivateReplicationStream for ReplicationStreamActor<A> {
    async fn timer_tick(&mut self, token: TimerToken) {
        // Ignore spurious wake-ups from cancelled timeouts
        if token != self.timer_token {
            return;
        }

        // It's time to send a heartbeat, even if the buffer is empty
        self.flush_buffer().await;
    }
    async fn await_response(
        self: Addr<Local<ReplicationStreamActor<A>>>,
        count: u64,
        receiver: Receiver<AppendEntriesResponse>,
        token: TimerToken,
        deadline: Instant,
    ) {
        // Wait for the RPC response, at least until deadline has expired
        if let Ok(Ok(res)) = timeout_at(deadline, receiver).await {
            self.handle_response(count, res);
        }
        // Wait for deadline
        delay_until(deadline).await;
        self.timer_tick(token);
    }
    async fn handle_response(
        &mut self,
        count: u64,
        resp: AppendEntriesResponse,
    ) -> Result<(), ReplicationError> {
        let is_up_to_date =
            resp.success && self.buffer.end_pos() > self.leader_state.last_log_index.0;

        // Regardless of result, record the term
        self.owner
            .call_record_term(resp.term, self.node_id, is_up_to_date)
            .await
            .map_err(|_| ReplicationError::Stopping)?;
        self.awaiting_response = false;

        if resp.success {
            self.adjust_match_index(self.prev_log_index() + count)
                .await?;
        } else if let Some(conflict) = resp.conflict_opt {
            self.adjust_match_index(conflict.index).await?;
        } else if self.prev_log_index() > LogIndex::ZERO {
            self.adjust_match_index(self.prev_log_index() - 1).await?;
        } else {
            // Can't go back any further, just retry
        }

        // Send the next batch
        if !self.buffer.is_empty() {
            self.flush_buffer().await;
        }
        Ok(())
    }
}
