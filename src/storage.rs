use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use act_zero::*;

use crate::messages::{Entry, Membership};
use crate::types::{LogIndex, NodeId, Term};
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
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[act_zero]
pub trait Storage<A: Application> {
    fn init(&self, res: Sender<HardState>);

    fn get_log_state(&self, res: Sender<LogState>);
    fn get_log_range(
        &self,
        range: Range<LogIndex>,
        res: Sender<LogRangeOrSnapshot<A::LogData, A::SnapshotId>>,
    );
    fn append_entry_to_log(
        &self,
        entry: Arc<Entry<A::LogData>>,
        res: Sender<Result<(), A::LogError>>,
    );
    fn replicate_to_log(&self, range: LogRange<A::LogData>, res: Sender<()>);
    fn apply_to_state_machine(
        &self,
        index: LogIndex,
        entry: Arc<Entry<A::LogData>>,
        res: Sender<A::LogResponse>,
    );
    fn save_hard_state(&self, hs: HardState, res: Sender<()>);

    fn install_snapshot(&self, snapshot: Snapshot<A::SnapshotId>, res: Sender<()>);
    fn create_snapshot(&self, res: Sender<(A::SnapshotId, BoxAsyncWrite)>);
    fn read_snapshot(&self, id: A::SnapshotId, res: Sender<BoxAsyncRead>);
}
