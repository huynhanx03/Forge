use rand::RngExt;
use std::time::{Duration, Instant};

use crate::{
    adapters::driven::storage::log::PartitionLog,
    consensus::{
        messages::{
            AppendEntries, AppendEntriesResponse,
            RequestVote, RequestVoteResponse,
            InstallSnapshot, InstallSnapshotResponse,
        },
        state::{PersistentState, Role},
    },
    core::domain::record_batch::RecordBatch,
    shared::collections::{FlatMap, FlatSet},
};

pub struct Node {
    pub id: u32,
    pub peers: Vec<u32>,
    pub log_store: PartitionLog,

    pub role: Role,
    pub persistent_state: PersistentState,

    pub commit_index: i64,
    pub last_applied: i64,

    pub election_timeout: Duration,
    pub last_heartbeat: Instant,
}

impl Node {
    pub fn new(id: u32, peers: Vec<u32>, log_store: PartitionLog) -> Self {
        Self {
            id,
            peers,
            log_store,
            role: Role::Follower,
            persistent_state: PersistentState::new(),
            commit_index: -1,
            last_applied: -1,
            election_timeout: Self::generate_election_timeout(),
            last_heartbeat: Instant::now(),
        }
    }

    fn generate_election_timeout() -> Duration {
        let mut rng = rand::rng();
        let timeout_ms = rng.random_range(150..=300);
        Duration::from_millis(timeout_ms)
    }

    pub fn tick(&mut self) {
        if matches!(self.role, Role::Leader { .. }) {
            tracing::info!("Node {} is in leader role", self.id);
        } else {
            if self.last_heartbeat.elapsed() >= self.election_timeout {
                tracing::info!("Node {} election timeout elapsed", self.id);
                self.start_election();
            }
        }
    }

    fn start_election(&mut self) {
        let mut votes = FlatSet::new();
        votes.insert(self.id);
        self.role = Role::Candidate {
            votes_received: votes,
        };
        self.persistent_state.current_term += 1;
        self.persistent_state.voted_for = Some(self.id);

        self.last_heartbeat = Instant::now();
        self.election_timeout = Self::generate_election_timeout();

        tracing::info!(
            "Node {} started election with term {}",
            self.id,
            self.persistent_state.current_term
        );
    }

    pub fn handle_request_vote(&mut self, request: RequestVote) -> RequestVoteResponse {
        if request.term < self.persistent_state.current_term {
            tracing::info!(
                "Node {} rejected vote request from {} because term {} is lower than current term {}",
                self.id,
                request.candidate_id,
                request.term,
                self.persistent_state.current_term
            );
            return RequestVoteResponse {
                term: self.persistent_state.current_term,
                vote_granted: false,
            };
        }

        if request.term > self.persistent_state.current_term {
            tracing::info!(
                "Node {} saw term {} > {}. Stepping down to Follower.",
                self.id,
                request.term,
                self.persistent_state.current_term
            );
            self.role = Role::Follower;
            self.persistent_state.current_term = request.term;
            self.persistent_state.voted_for = None;
        }

        let can_vote = match self.persistent_state.voted_for {
            Some(voted_for) => voted_for == request.candidate_id,
            None => true,
        };

        let my_last_log_index: i64 = self.log_store.get_last_log_index();
        let my_last_log_term: u64 = self.log_store.get_last_log_term();

        let is_log_up_to_date = if request.last_log_term != my_last_log_term {
            request.last_log_term > my_last_log_term
        } else {
            request.last_log_index >= my_last_log_index
        };

        if can_vote && is_log_up_to_date {
            self.role = Role::Follower;
            self.persistent_state.voted_for = Some(request.candidate_id);
            self.last_heartbeat = Instant::now();
            self.election_timeout = Self::generate_election_timeout();

            tracing::info!("Node {} voted for {}", self.id, request.candidate_id);

            return RequestVoteResponse {
                term: self.persistent_state.current_term,
                vote_granted: true,
            };
        }

        tracing::info!(
            "Node {} rejected vote request from {} because log is not up-to-date or already voted",
            self.id,
            request.candidate_id
        );
        RequestVoteResponse {
            term: self.persistent_state.current_term,
            vote_granted: false,
        }
    }

    pub async fn handle_append_entries(&mut self, request: AppendEntries) -> AppendEntriesResponse {
        if request.term < self.persistent_state.current_term {
            tracing::info!(
                "Node {} rejected append entries request from {} because term {} is lower than current term {}",
                self.id,
                request.leader_id,
                request.term,
                self.persistent_state.current_term
            );
            return AppendEntriesResponse {
                term: self.persistent_state.current_term,
                success: false,
                match_index: -1,
            };
        }

        self.role = Role::Follower;
        self.persistent_state.current_term = request.term;
        self.persistent_state.voted_for = Some(request.leader_id);
        self.last_heartbeat = Instant::now();
        self.election_timeout = Self::generate_election_timeout();

        let my_last_log_index: i64 = self.log_store.get_last_log_index();

        if request.prev_log_index >= 0 {
            let has_matching_log = match self
                .log_store
                .get_term_at_index(request.prev_log_index)
                .await
            {
                Ok(Some(term)) => term == request.prev_log_term,
                _ => false,
            };

            if !has_matching_log {
                tracing::warn!(
                    "Node {} rejected AppendEntries: log mismatch at prev_log_index {}",
                    self.id,
                    request.prev_log_index
                );
                return AppendEntriesResponse {
                    term: self.persistent_state.current_term,
                    success: false,
                    match_index: my_last_log_index,
                };
            }
        }

        let mut latest_match_index = request.prev_log_index;
        let mut new_entries_start = None;

        for (index, batch) in request.entries.iter().enumerate() {
            let batch_index = batch.base_offset;

            match self.log_store.get_term_at_index(batch_index).await {
                Ok(Some(existing_term)) => {
                    if existing_term != batch.partition_leader_epoch as u64 {
                        tracing::warn!(
                            "Node {} detected log conflict at index {}. Truncating log.",
                            self.id,
                            batch_index
                        );

                        if let Err(e) = self.log_store.truncate_from_index(batch_index).await {
                            tracing::error!("Error truncating disk log: {}", e);
                            return AppendEntriesResponse {
                                term: self.persistent_state.current_term,
                                success: false,
                                match_index: my_last_log_index,
                            };
                        }

                        new_entries_start = Some(index);
                        break;
                    }
                    latest_match_index = batch.base_offset + batch.records_count as i64 - 1;
                }
                _ => {
                    new_entries_start = Some(index);
                    break;
                }
            }
        }

        if let Some(start_idx) = new_entries_start {
            for batch in request.entries.into_iter().skip(start_idx) {
                let _ = self.log_store.append(&batch).await;
                latest_match_index = batch.base_offset + batch.records_count as i64 - 1;
            }
        }

        if request.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(request.leader_commit, latest_match_index);
        }

        tracing::info!(
            "Node {} successfully appended entries from leader {}. Match index: {}",
            self.id,
            request.leader_id,
            latest_match_index
        );

        AppendEntriesResponse {
            term: self.persistent_state.current_term,
            success: true,
            match_index: latest_match_index,
        }
    }

    pub fn handle_request_vote_response(&mut self, response: RequestVoteResponse, from_id: u32) {
        if response.term > self.persistent_state.current_term {
            self.role = Role::Follower;
            self.persistent_state.current_term = response.term;
            self.persistent_state.voted_for = None;
            return;
        }

        if response.term == self.persistent_state.current_term && response.vote_granted {
            let mut won_election = false;

            if let Role::Candidate {
                ref mut votes_received,
            } = self.role
            {
                votes_received.insert(from_id);
                let majority = (self.peers.len() + 1) >> 1 + 1;
                if votes_received.len() >= majority {
                    won_election = true;
                }
            }

            if won_election {
                tracing::info!(
                    "Node {} won election for term {}! Becoming Leader.",
                    self.id,
                    self.persistent_state.current_term
                );
                self.become_leader();
            }
        }
    }

    fn become_leader(&mut self) {
        let last_log_index = self.log_store.get_last_log_index();
        let mut next_index = FlatMap::new();
        let mut match_index = FlatMap::new();

        for peer_id in &self.peers {
            next_index.insert(*peer_id, last_log_index + 1);
            match_index.insert(*peer_id, -1);
        }

        self.role = Role::Leader {
            next_index,
            match_index,
        };
    }

    pub async fn handle_append_entries_response(
        &mut self,
        response: AppendEntriesResponse,
        peer_id: u32,
    ) {
        if response.term > self.persistent_state.current_term {
            self.role = Role::Follower;
            self.persistent_state.current_term = response.term;
            self.persistent_state.voted_for = None;
            return;
        }

        if let Role::Leader {
            ref mut next_index,
            ref mut match_index,
        } = self.role
        {
            if response.success {
                match_index.insert(peer_id, response.match_index);
                next_index.insert(peer_id, response.match_index + 1);
            } else {
                if let Some(n_idx) = next_index.get_mut(&peer_id) {
                    if *n_idx > 0 {
                        *n_idx -= 1;
                    }
                }
            }
        }

        if response.success && matches!(self.role, Role::Leader { .. }) {
            self.advance_commit_index().await;
        }
    }

    async fn advance_commit_index(&mut self) {
        let new_commit_index = if let Role::Leader {
            ref match_index, ..
        } = self.role
        {
            let mut match_indexes: Vec<i64> = match_index.values().copied().collect();
            match_indexes.push(self.log_store.get_last_log_index());
            match_indexes.sort_unstable();

            let majority = (self.peers.len() + 1) >> 1 + 1;
            match_indexes[match_indexes.len() - majority]
        } else {
            return;
        };

        if new_commit_index > self.commit_index {
            if let Ok(Some(term)) = self.log_store.get_term_at_index(new_commit_index).await {
                if term == self.persistent_state.current_term {
                    self.commit_index = new_commit_index;
                    tracing::info!(
                        "Leader {} advanced commit_index to {}",
                        self.id,
                        self.commit_index
                    );
                }
            }
        }
    }

    pub async fn client_append_local(&mut self, batch: RecordBatch) -> Result<i64, String> {
        if !matches!(self.role, Role::Leader { .. }) {
            return Err("Node is not Leader".into());
        }

        let mut final_batch = batch;
        final_batch.partition_leader_epoch = self.persistent_state.current_term as i32;

        let next_offset = self.log_store.get_last_log_index() + 1;
        final_batch.base_offset = next_offset;

        self.log_store
            .append(&final_batch)
            .await
            .map_err(|e| e.to_string())?;

        let last_log_index = self.log_store.get_last_log_index();

        tracing::info!(
            "Leader {} locally appended entry at index {}",
            self.id,
            last_log_index
        );

        Ok(last_log_index)
    }

    pub async fn handle_install_snapshot(
        &mut self,
        request: InstallSnapshot,
    ) -> InstallSnapshotResponse {
        if request.term < self.persistent_state.current_term {
            return InstallSnapshotResponse {
                term: self.persistent_state.current_term,
            };
        }

        self.role = Role::Follower;
        self.persistent_state.current_term = request.term;
        self.persistent_state.voted_for = Some(request.leader_id);
        self.last_heartbeat = Instant::now();
        self.election_timeout = Self::generate_election_timeout();

        let my_last_log_index = self.log_store.get_last_log_index();

        if request.last_included_index > my_last_log_index {
            if let Err(e) = self.log_store.truncate_from_index(0).await {
                tracing::error!("Error truncating log for snapshot: {}", e);
            }
        } else {
            let _ = self
                .log_store
                .truncate_prefix(request.last_included_index)
                .await;
        }

        if request.last_included_index > self.commit_index {
            self.commit_index = request.last_included_index;
            self.last_applied = request.last_included_index;
        }

        tracing::info!(
            "Node {} successfully installed snapshot from leader {}. Last included index: {}",
            self.id,
            request.leader_id,
            request.last_included_index
        );

        InstallSnapshotResponse {
            term: self.persistent_state.current_term,
        }
    }

    pub fn handle_install_snapshot_response(
        &mut self,
        response: InstallSnapshotResponse,
        peer_id: u32,
        snapshot_last_index: i64,
    ) {
        if response.term > self.persistent_state.current_term {
            self.role = Role::Follower;
            self.persistent_state.current_term = response.term;
            self.persistent_state.voted_for = None;
            return;
        }

        if let Role::Leader {
            ref mut next_index,
            ref mut match_index,
        } = self.role
        {
            let current_match = match_index.get(&peer_id).copied().unwrap_or(-1);
            if snapshot_last_index > current_match {
                match_index.insert(peer_id, snapshot_last_index);
                next_index.insert(peer_id, snapshot_last_index + 1);
                tracing::info!(
                    "Leader {} updated peer {} match_index to {} after snapshot install",
                    self.id,
                    peer_id,
                    snapshot_last_index
                );
            }
        }
    }
}
