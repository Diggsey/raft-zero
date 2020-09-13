use tokio::io::AsyncReadExt;
use tokio::time::Instant;

use super::{
    ChangeMode, ErrorOrChange, PrivateReplicationStreamExt, ReplicationError, SharedState,
};
use crate::config::Config;
use crate::connection::ConnectionExt;
use crate::messages::InstallSnapshotRequest;
use crate::storage::{BoxAsyncRead, Snapshot};
use crate::Application;

pub(super) struct SnapshotReplication<A: Application> {
    snapshot: Snapshot<A::SnapshotId>,
    buffer: Vec<u8>,
    source: BoxAsyncRead,
    offset: u64,
}

impl<A: Application> SnapshotReplication<A> {
    pub(super) fn new(
        config: &Config,
        snapshot: Snapshot<A::SnapshotId>,
        source: BoxAsyncRead,
    ) -> Self {
        let mut buffer = Vec::new();
        buffer.resize(config.snapshot_chunk_size as usize, 0);
        Self {
            snapshot,
            buffer,
            source,
            offset: 0,
        }
    }
    pub(super) async fn fill_buffer(&mut self) -> Result<(), ErrorOrChange<A>> {
        let mut offset = 0;
        while offset < self.buffer.len() {
            let bytes_read = self
                .source
                .read(&mut self.buffer[offset..])
                .await
                .map_err(|_| ErrorOrChange::Error(ReplicationError::StorageFailure))?;

            offset += bytes_read;

            if bytes_read == 0 {
                self.buffer.truncate(offset);
                break;
            }
        }
        self.offset += offset as u64;
        Ok(())
    }
    pub(super) fn flush_buffer(&mut self, shared: &mut SharedState<A>) {
        let res = shared
            .connection
            .call_install_snapshot(InstallSnapshotRequest {
                leader_id: shared.leader_id,
                database_id: shared.database_id,
                term: shared.leader_state.term,
                last_included_index: self.snapshot.last_log_index,
                last_included_term: self.snapshot.last_log_term,
                offset: self.offset - self.buffer.len() as u64,
                done: self.buffer.len() < self.buffer.capacity(),
                data: self.buffer.clone(),
            });
        let deadline = Instant::now() + shared.config.heartbeat_interval;
        shared.awaiting_response = true;
        shared
            .this
            .await_install_snapshot_response(res, shared.timer_token.inc(), deadline);
    }
    pub(super) async fn handle_install_snapshot_response(
        &mut self,
        shared: &mut SharedState<A>,
    ) -> Result<(), ErrorOrChange<A>> {
        if self.buffer.len() < self.buffer.capacity() {
            Err(ErrorOrChange::Change(ChangeMode::Log {
                match_index: self.snapshot.last_log_index,
                match_term: self.snapshot.last_log_term,
            }))
        } else {
            self.fill_buffer().await?;
            self.flush_buffer(shared);
            Ok(())
        }
    }
}
