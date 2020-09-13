use std::fmt::Debug;
use std::sync::Arc;

use futures::task::{FutureObj, Spawn, SpawnError};

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
mod timer;
mod types;

pub use config::{Config, MembershipChangeCond};
pub use connection::{Connection, ConnectionExt, ConnectionImpl};
pub use node::{ClientResult, Node, NodeActor, NodeExt};
pub use observer::{ObservedState, Observer, ObserverExt, ObserverImpl};
pub use storage::{
    BoxAsyncRead, BoxAsyncWrite, HardState, LogRange, LogRangeOrSnapshot, LogState, Snapshot,
    Storage, StorageExt, StorageImpl,
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

struct TokioSpawn;

impl Spawn for TokioSpawn {
    fn spawn_obj(&self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        tokio::spawn(future);
        Ok(())
    }
}

fn spawn_actor<A: Actor>(actor: A) -> Addr<Local<A>> {
    spawn(&TokioSpawn, actor).expect("TokioSpawn to be infallible")
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
