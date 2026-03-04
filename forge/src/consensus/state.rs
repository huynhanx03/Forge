use crate::shared::collections::{FlatMap, FlatSet};

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Follower,
    Candidate {
        votes_received: FlatSet<u32>,
    },
    Leader {
        /// Optimistic pointer: The index of the next log entry to send to each follower.
        /// Used for probing missing entries via AppendEntries. Decrements on failure.
        next_index: FlatMap<u32, i64>,

        /// Pessimistic pointer: The highest log entry index known to be replicated on each follower.
        /// Used to calculate the cluster's majority and advance the commit_index. Never decreases.
        match_index: FlatMap<u32, i64>,
    },
}

#[derive(Debug, Clone)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<u32>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}
