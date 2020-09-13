use act_zero::*;

use crate::messages::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, VoteRequest, VoteResponse,
};
use crate::Application;

#[act_zero]
pub trait Connection<A: Application> {
    fn request_vote(&self, req: VoteRequest, res: Sender<VoteResponse>);
    fn append_entries(&self, req: AppendEntriesRequest<A>, res: Sender<AppendEntriesResponse>);
    fn install_snapshot(&self, req: InstallSnapshotRequest, res: Sender<InstallSnapshotResponse>);
    fn request_pre_vote(&self, req: PreVoteRequest, res: Sender<PreVoteResponse>);
}
