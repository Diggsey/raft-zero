use std::ops::Range;
use std::sync::Arc;

use act_zero::*;

use crate::messages::{Entry, Membership};
use crate::types::{LogIndex, NodeId, Term};
use crate::{Application, LogData};

pub struct InitialState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    /// The index of the last entry.
    pub last_log_index: LogIndex,
    /// The index of the last log applied to the state machine.
    pub last_log_applied: LogIndex,
    /// The last membership change applied to the state machine
    pub last_membership_applied: Membership,
}

pub struct LogRange<D: LogData> {
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Arc<Entry<D>>>,
}

pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[act_zero]
pub trait Storage<A: Application> {
    fn get_initial_state(&self, res: Sender<Option<InitialState>>);
    fn get_log_range(&self, range: Range<LogIndex>, res: Sender<LogRange<A::LogData>>);
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
}
