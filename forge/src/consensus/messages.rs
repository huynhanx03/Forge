use crate::core::domain::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u32,
    pub last_log_index: i64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u32,
    pub prev_log_index: i64,
    pub prev_log_term: u64,
    pub entries: Vec<RecordBatch>,
    pub leader_commit: i64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: i64,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshot {
    pub term: u64,
    pub leader_id: u32,
    pub last_included_index: i64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}
