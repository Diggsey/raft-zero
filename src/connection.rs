use act_zero::*;
use async_trait::async_trait;

use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, VoteRequest, VoteResponse,
};
use crate::Application;

#[async_trait]
pub trait Connection<A: Application>: Actor {
    async fn request_vote(&mut self, req: VoteRequest) -> ActorResult<VoteResponse>;
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<A>,
    ) -> ActorResult<AppendEntriesResponse>;
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> ActorResult<InstallSnapshotResponse>;
    async fn request_pre_vote(&mut self, req: PreVoteRequest) -> ActorResult<PreVoteResponse>;
}
