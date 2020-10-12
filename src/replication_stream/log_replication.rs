use std::ops::Range;
use std::sync::Arc;

use act_zero::*;

use super::{ChangeMode, ErrorOrChange, ReplicationError, SharedState};
use crate::commit_state::CommitState;
use crate::config::Config;
use crate::messages::{AppendEntriesRequest, AppendEntriesResponse, Entry};
use crate::seekable_buffer::{SeekDir, SeekableBuffer};
use crate::storage::{LogRange, LogRangeOrSnapshot, Storage};
use crate::types::{LogIndex, Term};
use crate::Application;

pub(super) struct LogReplication<A: Application> {
    prev_log_term: Term,
    buffer: SeekableBuffer<Arc<Entry<A::LogData>>>,
}

impl<A: Application> LogReplication<A> {
    pub(super) fn new(config: &Config, match_index: LogIndex, match_term: Term) -> Self {
        let mut buffer = SeekableBuffer::new(config.max_replication_buffer_len as usize);
        buffer.seek(match_index.0 + 1);
        Self {
            buffer,
            prev_log_term: match_term,
        }
    }

    pub(super) async fn append_entry(
        &mut self,
        shared: &mut SharedState<A>,
        entry: Arc<Entry<A::LogData>>,
    ) {
        if self
            .buffer
            .fill(shared.leader_state.last_log_index.0, Some(entry))
            && !shared.awaiting_response
        {
            self.flush_buffer(shared);
        }
    }

    fn prev_log_index(&self) -> LogIndex {
        LogIndex(self.buffer.pos() - 1)
    }

    pub(super) fn is_up_to_date(&self, shared: &SharedState<A>) -> bool {
        self.buffer.end_pos() > shared.leader_state.last_log_index.0
    }
    async fn get_log_range(
        storage: &Addr<dyn Storage<A>>,
        range: &Range<u64>,
    ) -> Result<LogRange<A::LogData>, ErrorOrChange<A>> {
        match call!(storage.get_log_range(LogIndex(range.start)..LogIndex(range.end)))
            .await
            .map_err(|_| ErrorOrChange::Error(ReplicationError::StorageFailure))?
        {
            LogRangeOrSnapshot::LogRange(log_range) => Ok(log_range),
            LogRangeOrSnapshot::Snapshot(snapshot) => {
                Err(ErrorOrChange::Change(ChangeMode::Snapshot { snapshot }))
            }
        }
    }

    pub(super) async fn fill_buffer(
        &mut self,
        shared: &mut SharedState<A>,
    ) -> Result<(), ErrorOrChange<A>> {
        // Update the commit state
        send!(shared.commit_state.set_match_index(
            shared.node_id,
            self.prev_log_index(),
            self.prev_log_term
        ));

        let range = self.buffer.calc_fill_range(
            shared.config.max_append_entries_len as usize,
            shared.leader_state.last_log_index.0 + 1,
        );
        if range.end > range.start {
            let log_range = Self::get_log_range(&shared.storage, &range).await?;
            self.buffer.fill(range.start, log_range.entries);
        }
        Ok(())
    }

    async fn adjust_match_index(
        &mut self,
        shared: &mut SharedState<A>,
        new_match_index: LogIndex,
    ) -> Result<(), ErrorOrChange<A>> {
        match self.buffer.seek(new_match_index.0 + 1) {
            SeekDir::Forward(items) => {
                self.prev_log_term = items.last().expect("At least one item").term;
            }
            SeekDir::Backward(mut gap) => {
                let range = gap.range();
                let log_range = Self::get_log_range(&shared.storage, &range).await?;
                gap.fill(range.start, log_range.entries);
                self.prev_log_term = log_range.prev_log_term;
            }
            SeekDir::Still => {}
            SeekDir::Far(buffer) => {
                let range = buffer.calc_fill_range(
                    shared.config.max_append_entries_len as usize,
                    shared.leader_state.last_log_index.0 + 1,
                );
                let log_range = Self::get_log_range(&shared.storage, &range).await?;
                buffer.fill(range.start, log_range.entries);
                self.prev_log_term = log_range.prev_log_term;
            }
        }

        self.fill_buffer(shared).await
    }
    pub(super) fn flush_buffer(&mut self, shared: &mut SharedState<A>) {
        // Send the first N items from the buffer, without removing them
        let entries: Vec<_> = self
            .buffer
            .iter()
            .take(shared.config.max_append_entries_len as usize)
            .cloned()
            .collect();
        let count = entries.len() as u64;

        let res = call!(shared.connection.append_entries(AppendEntriesRequest {
            leader_id: shared.leader_id,
            database_id: shared.database_id,
            term: shared.leader_state.term,
            leader_commit: shared.leader_state.commit_index,
            prev_log_index: self.prev_log_index(),
            prev_log_term: self.prev_log_term,
            entries,
        }));

        shared.awaiting_response = true;
        shared.timer.run_with_timeout_for_weak(
            shared.this.clone(),
            shared.config.heartbeat_interval,
            move |addr| async move {
                if let Ok(resp) = res.await {
                    send!(addr.handle_append_entries_response(count, resp));
                } else {
                    send!(addr.handle_missed_response());
                }
            },
        );
    }
    pub(super) async fn handle_append_entries_response(
        &mut self,
        shared: &mut SharedState<A>,
        count: u64,
        resp: AppendEntriesResponse,
    ) -> Result<(), ErrorOrChange<A>> {
        if resp.success {
            self.adjust_match_index(shared, self.prev_log_index() + count)
                .await?;
        } else if let Some(conflict) = resp.conflict_opt {
            self.adjust_match_index(shared, conflict.index).await?;
        } else if self.prev_log_index() > LogIndex::ZERO {
            self.adjust_match_index(shared, self.prev_log_index() - 1)
                .await?;
        } else {
            // Can't go back any further, just retry
        }

        // Send the next batch
        if !self.buffer.is_empty() {
            self.flush_buffer(shared);
        }
        Ok(())
    }
}
