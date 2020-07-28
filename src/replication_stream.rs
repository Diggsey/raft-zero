use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use tokio::time::{delay_until, timeout_at, Instant};

use act_zero::*;

use crate::commit_state::{CommitStateActor, CommitStateExt};
use crate::config::Config;
use crate::connection::{Connection, ConnectionExt};
use crate::messages::{AppendEntriesRequest, AppendEntriesResponse, Entry};
use crate::node::{NodeActor, PrivateNodeExt};
use crate::storage::{LogRange, Storage, StorageExt};
use crate::timer::TimerToken;
use crate::types::{LogIndex, NodeId, Term};
use crate::Application;

#[derive(Clone)]
pub(crate) struct LeaderState {
    pub term: Term,
    pub commit_index: LogIndex,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[act_zero]
pub(crate) trait ReplicationStream<A: Application> {
    fn append_entry(&self, leader_state: LeaderState, entry: Arc<Entry<A::LogData>>);
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ReplicationMode {
    LineRate,
    Lagging,
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
    mode: ReplicationMode,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    buffer: VecDeque<Arc<Entry<A::LogData>>>,
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
        Self {
            this: WeakAddr::default(),
            node_id,
            owner,
            leader_id,
            mode: ReplicationMode::LineRate,
            prev_log_index: leader_state.last_log_index,
            prev_log_term: leader_state.last_log_term,
            leader_state,
            buffer: VecDeque::new(),
            config,
            timer_token: TimerToken::default(),
            awaiting_response: false,
            connection,
            storage,
            commit_state,
        }
    }
    async fn get_log_range(
        &mut self,
        range: Range<LogIndex>,
    ) -> Result<LogRange<A::LogData>, ReplicationError> {
        self.storage
            .call_get_log_range(range)
            .await
            .map_err(|_| ReplicationError::StorageFailure)
    }
    async fn adjust_match_index(
        &mut self,
        new_match_index: LogIndex,
    ) -> Result<(), ReplicationError> {
        match new_match_index.cmp(&self.prev_log_index) {
            Ordering::Equal => {}
            Ordering::Greater => {
                // We're advancing the match index
                let count = new_match_index - self.prev_log_index;
                self.prev_log_term = self.buffer[count as usize - 1].term;
                self.buffer.drain(0..count as usize);
                let last_buffered_index = self.prev_log_index + (self.buffer.len() as u64);

                // If there are more entries to replicate
                if last_buffered_index < self.leader_state.last_log_index {
                    let desired_buffered_index =
                        self.prev_log_index + self.config.max_append_entries_len;

                    // And our buffer does not contain enough entries to send a max-sized RPC
                    if desired_buffered_index > last_buffered_index {
                        // We need to load more entries
                        let log_range = self
                            .get_log_range(last_buffered_index + 1..desired_buffered_index + 1)
                            .await?;

                        assert_eq!(log_range.prev_log_index, last_buffered_index);
                        self.buffer.extend(log_range.entries);
                    }
                } else {
                    // We caught up
                    self.mode = ReplicationMode::LineRate;
                }

                // Update the commit state
                self.commit_state.set_match_index(
                    self.node_id,
                    self.prev_log_index,
                    self.prev_log_term,
                );
            }
            Ordering::Less => {
                // The match index is regressing
                let count = self.prev_log_index - new_match_index;

                // If we can preserve some entries in our buffer
                if count < self.config.max_replication_buffer_len {
                    let new_len = (self.config.max_replication_buffer_len - count) as usize;
                    if new_len < self.buffer.len() {
                        // We overran the buffer, switch to lagging mode
                        self.buffer.truncate(new_len);
                        self.mode = ReplicationMode::Lagging;
                    }

                    let log_range = self
                        .get_log_range(new_match_index + 1..self.prev_log_index + 1)
                        .await?;

                    assert_eq!(log_range.prev_log_index, new_match_index);

                    // Prepend loaded entries
                    for entry in log_range.entries.into_iter().rev() {
                        self.buffer.push_front(entry);
                    }
                    self.prev_log_index = new_match_index;
                    self.prev_log_term = log_range.prev_log_term;
                } else {
                    // Clear the buffer and switch to lagging mode
                    self.buffer.clear();
                    self.mode = ReplicationMode::Lagging;

                    let desired_buffered_index =
                        new_match_index + self.config.max_append_entries_len;
                    let log_range = self
                        .get_log_range(new_match_index + 1..desired_buffered_index + 1)
                        .await?;

                    assert_eq!(log_range.prev_log_index, new_match_index);

                    // Replace buffer with loaded entries
                    self.buffer.extend(log_range.entries);
                    self.prev_log_index = new_match_index;
                    self.prev_log_term = log_range.prev_log_term;
                }
            }
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
            prev_log_index: self.prev_log_index,
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
        // Send the initial heartbeat
        addr.timer_tick(self.timer_token.inc());
        Ok(())
    }
}

#[act_zero]
impl<A: Application> ReplicationStream<A> for ReplicationStreamActor<A> {
    async fn append_entry(&mut self, leader_state: LeaderState, entry: Arc<Entry<A::LogData>>) {
        self.leader_state = leader_state;

        if let ReplicationMode::LineRate = self.mode {
            if (self.buffer.len() as u64) < self.config.max_replication_buffer_len {
                self.buffer.push_back(entry);
                // If we're idle, immediately send out the new entries
                if !self.awaiting_response {
                    self.flush_buffer().await;
                }
            } else {
                self.mode = ReplicationMode::Lagging;
            }
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
        let is_up_to_date = resp.success && self.mode == ReplicationMode::LineRate;

        // Regardless of result, record the term
        self.owner
            .call_record_term(resp.term, self.node_id, is_up_to_date)
            .await
            .map_err(|_| ReplicationError::Stopping)?;
        self.awaiting_response = false;

        if resp.success {
            self.adjust_match_index(self.prev_log_index + count).await?;
        } else if let Some(conflict) = resp.conflict_opt {
            self.adjust_match_index(conflict.index).await?;
        } else if self.prev_log_index > LogIndex::ZERO {
            self.adjust_match_index(self.prev_log_index - 1).await?;
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
