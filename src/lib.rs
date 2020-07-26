use std::fmt::Debug;

use futures::task::{FutureObj, Spawn, SpawnError};

use act_zero::*;

pub mod messages;

pub mod types;
use types::*;

mod commit_state;
mod config;
mod connection;
mod node;
mod replication_stream;
mod storage;
mod timer;

pub trait LogData: Debug + Send + Sync + 'static {}
pub trait LogResponse: Debug + Send + Sync + 'static {}

pub trait Application: Send + Sync + 'static {
    type LogData: LogData;
    type LogResponse: LogResponse;
    type LogError: Send + Sync + 'static;

    fn establish_connection(&mut self, node_id: NodeId) -> Addr<dyn connection::Connection<Self>>;
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
