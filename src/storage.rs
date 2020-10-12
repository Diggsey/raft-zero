use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use act_zero::*;
use async_trait::async_trait;

use crate::messages::{Entry, Membership};
use crate::types::{DatabaseId, LogIndex, NodeId, Term};
use crate::{Application, LogData};

#[derive(Debug, Clone)]
pub struct LogState {
    /// The index of the last entry.
    pub last_log_index: LogIndex,
    /// The index of the last log applied to the state machine.
    pub last_log_applied: LogIndex,
    /// The last membership change applied to the state machine
    pub last_membership_applied: Membership,
}

#[derive(Debug)]
pub struct LogRange<D: LogData> {
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Arc<Entry<D>>>,
}

impl<D: LogData> Clone for LogRange<D> {
    fn clone(&self) -> Self {
        Self {
            prev_log_index: self.prev_log_index,
            prev_log_term: self.prev_log_term,
            entries: self.entries.clone(),
        }
    }
}

pub type BoxAsyncRead = Pin<Box<dyn AsyncRead + Send + Sync>>;
pub type BoxAsyncWrite = Pin<Box<dyn AsyncWrite + Send + Sync>>;

#[derive(Debug)]
pub struct Snapshot<S> {
    pub id: S,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug)]
pub enum LogRangeOrSnapshot<D: LogData, S> {
    LogRange(LogRange<D>),
    Snapshot(Snapshot<S>),
}

impl<D: LogData, S> LogRangeOrSnapshot<D, S> {
    pub fn log_range(self) -> Option<LogRange<D>> {
        if let LogRangeOrSnapshot::LogRange(lr) = self {
            Some(lr)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HardState {
    pub database_id: DatabaseId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[async_trait]
pub trait Storage<A: Application>: Actor {
    async fn init(&mut self) -> ActorResult<HardState>;

    async fn get_log_state(&mut self) -> ActorResult<LogState>;
    async fn get_log_range(
        &mut self,
        range: Range<LogIndex>,
    ) -> ActorResult<LogRangeOrSnapshot<A::LogData, A::SnapshotId>>;
    async fn append_entry_to_log(
        &mut self,
        entry: Arc<Entry<A::LogData>>,
    ) -> ActorResult<Result<(), A::LogError>>;
    async fn replicate_to_log(&mut self, range: LogRange<A::LogData>) -> ActorResult<()>;
    async fn apply_to_state_machine(
        &mut self,
        index: LogIndex,
        entry: Arc<Entry<A::LogData>>,
    ) -> ActorResult<A::LogResponse>;
    async fn save_hard_state(&mut self, hs: HardState) -> ActorResult<()>;

    async fn install_snapshot(&mut self, snapshot: Snapshot<A::SnapshotId>) -> ActorResult<()>;
    async fn create_snapshot(&mut self) -> ActorResult<(A::SnapshotId, BoxAsyncWrite)>;
    async fn read_snapshot(&mut self, id: A::SnapshotId) -> ActorResult<BoxAsyncRead>;
}
