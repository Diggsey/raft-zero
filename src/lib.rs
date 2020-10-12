use std::fmt::Debug;
use std::sync::Arc;

use act_zero::*;

pub mod messages;

mod commit_state;
mod config;
mod connection;
mod election_state;
mod node;
mod observer;
mod replication_stream;
mod seekable_buffer;
mod storage;
mod types;

pub use config::{Config, MembershipChangeCond};
pub use connection::Connection;
pub use node::{ClientResult, Node, NodeActor};
pub use observer::{ObservedState, Observer};
pub use storage::{
    BoxAsyncRead, BoxAsyncWrite, HardState, LogRange, LogRangeOrSnapshot, LogState, Snapshot,
    Storage,
};
pub use types::{LogIndex, NodeId, Term};

pub trait LogData: Debug + Send + Sync + 'static {}
pub trait LogResponse: Debug + Send + Sync + 'static {}

pub trait Application: Send + Sync + 'static {
    type LogData: LogData;
    type LogResponse: LogResponse;
    type LogError: Send + Sync + 'static;
    type SnapshotId: Send + Sync + Debug + Clone + 'static;

    fn config(&self) -> Arc<Config>;
    fn storage(&self) -> Addr<dyn Storage<Self>>;
    fn observer(&self) -> Addr<dyn Observer> {
        Addr::default()
    }
    fn establish_connection(&mut self, node_id: NodeId) -> Addr<dyn Connection<Self>>;
}
