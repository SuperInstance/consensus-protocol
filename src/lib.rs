#![allow(dead_code)]
//! # Consensus Protocol
//!
//! A library implementing distributed consensus algorithms for fault-tolerant
//! distributed systems. Provides Raft-style leader election, log replication,
//! and commit index tracking.
//!
//! ## Overview
//!
//! In distributed systems, multiple nodes must agree on shared state despite
//! failures. This crate implements core Raft consensus protocol components:
//!
//! - **Leader Election**: Timeout-based election with term numbers
//! - **Log Replication**: Append-only log entries replicated across nodes
//! - **Commit Index**: Track which entries are safely committed
//!
//! ## Example
//!
//! ```
//! use consensus_protocol::{Vote, ConsensusState, RaftLeaderElection, LogReplication, CommitIndex};
//!
//! let mut state = ConsensusState::new("node-1");
//! let mut election = RaftLeaderElection::new("node-1", 150);
//! let mut replication = LogReplication::new("node-1");
//! let mut commit = CommitIndex::new();
//!
//! // Simulate election
//! let vote = Vote::new("node-1", "node-1", 1);
//! election.start_election(&mut state);
//!
//! // Append and replicate entries
//! replication.append(b"cmd-1".to_vec());
//! replication.append(b"cmd-2".to_vec());
//! assert_eq!(replication.log_len(), 2);
//! ```

use std::collections::HashMap;

/// A single vote cast during a leader election round.
///
/// Each vote records which agent cast it, who they voted for, and the term
/// number of the election. In Raft, each node may vote at most once per term.
#[derive(Debug, Clone, PartialEq)]
pub struct Vote {
    /// The ID of the agent casting this vote.
    pub voter_id: String,
    /// The candidate ID being voted for.
    pub candidate_id: String,
    /// The term (election round) this vote belongs to.
    pub term: u64,
}

impl Vote {
    /// Create a new vote.
    ///
    /// # Arguments
    /// * `voter_id` - ID of the voting agent
    /// * `candidate_id` - ID of the candidate being voted for
    /// * `term` - Election term number
    pub fn new(voter_id: &str, candidate_id: &str, term: u64) -> Self {
        Self {
            voter_id: voter_id.to_string(),
            candidate_id: candidate_id.to_string(),
            term,
        }
    }
}

/// A single log entry in the replicated log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    /// The term in which this entry was created.
    pub term: u64,
    /// The command or data payload.
    pub data: Vec<u8>,
    /// Monotonic index of this entry in the log.
    pub index: usize,
}

impl LogEntry {
    /// Create a new log entry.
    pub fn new(term: u64, data: Vec<u8>, index: usize) -> Self {
        Self { term, data, index }
    }
}

/// The persistent state of a consensus participant.
///
/// Tracks the current term, who this node voted for in the current term,
/// and the complete log of entries. This state must survive node restarts
/// in a real implementation.
#[derive(Debug, Clone)]
pub struct ConsensusState {
    /// The node's own identifier.
    pub node_id: String,
    /// Current term number (monotonically increasing).
    pub current_term: u64,
    /// Who this node voted for in the current term (None = no vote yet).
    pub voted_for: Option<String>,
    /// The replicated log entries.
    pub log: Vec<LogEntry>,
}

impl ConsensusState {
    /// Create a new consensus state for the given node.
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }

    /// Advance to a new term. Resets voted_for.
    pub fn advance_term(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
    }

    /// Cast a vote in the current term. Returns false if already voted.
    pub fn cast_vote(&mut self, candidate_id: &str) -> bool {
        if self.voted_for.is_some() {
            return false;
        }
        self.voted_for = Some(candidate_id.to_string());
        true
    }

    /// Get the index of the last log entry (0 if empty).
    pub fn last_log_index(&self) -> usize {
        self.log.len()
    }

    /// Get the term of the last log entry (0 if empty).
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    /// Append an entry to the log.
    pub fn append_entry(&mut self, entry: LogEntry) {
        self.log.push(entry);
    }

    /// Truncate log from the given index onward and append new entries.
    /// Used when a leader overwrites conflicting entries.
    pub fn truncate_and_append(&mut self, from_index: usize, entries: Vec<LogEntry>) {
        self.log.truncate(from_index);
        self.log.extend(entries);
    }
}

/// Raft-style leader election via randomized timeouts.
///
/// In Raft, nodes start as followers. If they don't hear from a leader
/// within the election timeout, they become candidates and request votes.
/// A candidate becomes leader upon receiving a majority of votes.
#[derive(Debug)]
pub struct RaftLeaderElection {
    /// This node's ID.
    node_id: String,
    /// Election timeout in milliseconds.
    election_timeout_ms: u64,
    /// Peers in the cluster (node_id → their latest known term).
    peers: HashMap<String, u64>,
    /// Votes received in the current election: voter → candidate.
    votes_received: HashMap<String, String>,
}

impl RaftLeaderElection {
    /// Create a new election manager.
    ///
    /// # Arguments
    /// * `node_id` - This node's identifier
    /// * `election_timeout_ms` - Timeout before triggering an election
    pub fn new(node_id: &str, election_timeout_ms: u64) -> Self {
        Self {
            node_id: node_id.to_string(),
            election_timeout_ms,
            peers: HashMap::new(),
            votes_received: HashMap::new(),
        }
    }

    /// Add a peer to the cluster.
    pub fn add_peer(&mut self, peer_id: &str) {
        self.peers.insert(peer_id.to_string(), 0);
    }

    /// Get the number of peers (excluding self).
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Get the cluster size (self + peers).
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Get the election timeout.
    pub fn election_timeout(&self) -> u64 {
        self.election_timeout_ms
    }

    /// Start a new election. Advances term, votes for self.
    /// Returns the vote request to send to peers.
    pub fn start_election(&mut self, state: &mut ConsensusState) -> Vote {
        state.advance_term(state.current_term + 1);
        state.cast_vote(&self.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.node_id.clone(), self.node_id.clone());
        Vote::new(&self.node_id, &self.node_id, state.current_term)
    }

    /// Receive a vote from a peer. Returns true if this node has won the election.
    pub fn receive_vote(&mut self, vote: Vote) -> bool {
        if vote.candidate_id == self.node_id {
            self.votes_received.insert(vote.voter_id.clone(), vote.candidate_id);
        }
        // Majority: more than half of cluster_size
        let needed = self.cluster_size() / 2 + 1;
        self.votes_received.len() >= needed
    }

    /// Reset election state (e.g., when a valid leader heartbeat is received).
    pub fn reset(&mut self) {
        self.votes_received.clear();
    }

    /// Check if a vote request should be granted.
    /// A node grants its vote if:
    /// 1. The candidate's term is at least as high
    /// 2. The candidate's log is at least as up-to-date
    /// 3. This node hasn't already voted in this term
    pub fn should_grant_vote(
        &self,
        state: &ConsensusState,
        candidate_term: u64,
        candidate_last_log_index: usize,
        candidate_last_log_term: u64,
    ) -> bool {
        if candidate_term < state.current_term {
            return false;
        }
        if state.voted_for.is_some() {
            return false;
        }
        // Log completeness check: candidate's log must be at least as up-to-date
        if candidate_last_log_term < state.last_log_term() {
            return false;
        }
        if candidate_last_log_term == state.last_log_term()
            && candidate_last_log_index < state.last_log_index()
        {
            return false;
        }
        true
    }
}

/// Log replication engine.
///
/// The leader replicates log entries to all followers. Each follower's
/// `next_index` tracks what the leader should send next, and `match_index`
/// tracks the highest log index known to be replicated on each follower.
#[derive(Debug)]
pub struct LogReplication {
    /// This node's ID.
    node_id: String,
    /// Local log entries.
    log: Vec<LogEntry>,
    /// For each follower, the next log index to send.
    next_index: HashMap<String, usize>,
    /// For each follower, the highest index known to be replicated.
    match_index: HashMap<String, usize>,
    /// Current term.
    current_term: u64,
}

impl LogReplication {
    /// Create a new log replication engine.
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            log: Vec::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            current_term: 0,
        }
    }

    /// Set the current term.
    pub fn set_term(&mut self, term: u64) {
        self.current_term = term;
    }

    /// Get the current term.
    pub fn term(&self) -> u64 {
        self.current_term
    }

    /// Append a new entry to the local log.
    pub fn append(&mut self, data: Vec<u8>) -> &LogEntry {
        let index = self.log.len() + 1;
        let entry = LogEntry::new(self.current_term, data, index);
        self.log.push(entry);
        self.log.last().unwrap()
    }

    /// Get the number of log entries.
    pub fn log_len(&self) -> usize {
        self.log.len()
    }

    /// Get a reference to the log.
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    /// Add a follower for replication tracking.
    pub fn add_follower(&mut self, follower_id: &str) {
        let next = 1; // start from beginning
        self.next_index.insert(follower_id.to_string(), next);
        self.match_index.insert(follower_id.to_string(), 0);
    }

    /// Get entries to send to a follower starting from their next_index.
    pub fn entries_for(&self, follower_id: &str) -> Vec<LogEntry> {
        let start = self.next_index.get(follower_id).copied().unwrap_or(1);
        self.log.iter().filter(|e| e.index >= start).cloned().collect()
    }

    /// Confirm that a follower has replicated up to the given index.
    pub fn confirm_replication(&mut self, follower_id: &str, index: usize) {
        if let Some(ni) = self.next_index.get_mut(follower_id) {
            *ni = index + 1;
        }
        if let Some(mi) = self.match_index.get_mut(follower_id) {
            *mi = (*mi).max(index);
        }
    }

    /// Count how many nodes (including self) have replicated up to the given index.
    pub fn replication_count(&self, index: usize) -> usize {
        let count = self.match_index.values().filter(|&&mi| mi >= index).count();
        count + 1 // +1 for self (the leader)
    }

    /// Apply entries from a leader (follower side).
    /// Returns the number of new entries appended.
    pub fn apply_entries(
        &mut self,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
    ) -> Result<usize, String> {
        // Check consistency: entry at prev_log_index must match prev_log_term
        if prev_log_index > 0 {
            match self.log.get(prev_log_index - 1) {
                Some(e) if e.term == prev_log_term => {}
                Some(_) => return Err("log term mismatch".to_string()),
                None => return Err("log missing previous entry".to_string()),
            }
        }
        let start = prev_log_index;
        let new_count = entries.len();
        self.log.truncate(start);
        self.log.extend(entries);
        Ok(new_count)
    }
}

/// Tracks which log entries have been committed (safely replicated to a majority).
///
/// An entry is considered committed once it has been replicated on a majority
/// of nodes. Once committed, an entry will never be lost (assuming a majority
/// of nodes remain operational).
#[derive(Debug)]
pub struct CommitIndex {
    /// The highest committed log index.
    committed: usize,
    /// The highest index applied to the state machine.
    last_applied: usize,
}

impl CommitIndex {
    /// Create a new commit index tracker.
    pub fn new() -> Self {
        Self {
            committed: 0,
            last_applied: 0,
        }
    }

    /// Get the current committed index.
    pub fn committed(&self) -> usize {
        self.committed
    }

    /// Get the last applied index.
    pub fn last_applied(&self) -> usize {
        self.last_applied
    }

    /// Advance the commit index. Returns the number of newly committed entries.
    /// Only advances if the new index is higher and a majority has replicated.
    pub fn advance_to(&mut self, index: usize, cluster_size: usize, replication_count: usize) -> usize {
        let majority = cluster_size / 2 + 1;
        if replication_count >= majority && index > self.committed {
            let newly = index - self.committed;
            self.committed = index;
            newly
        } else {
            0
        }
    }

    /// Apply committed entries up to the committed index.
    /// Returns the number of newly applied entries.
    pub fn apply_committed(&mut self) -> usize {
        if self.committed > self.last_applied {
            let count = self.committed - self.last_applied;
            self.last_applied = self.committed;
            count
        } else {
            0
        }
    }

    /// Check if a specific index has been committed.
    pub fn is_committed(&self, index: usize) -> bool {
        index <= self.committed
    }

    /// Check if a specific index has been applied.
    pub fn is_applied(&self, index: usize) -> bool {
        index <= self.last_applied
    }
}

impl Default for CommitIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_creation() {
        let v = Vote::new("a", "b", 3);
        assert_eq!(v.voter_id, "a");
        assert_eq!(v.candidate_id, "b");
        assert_eq!(v.term, 3);
    }

    #[test]
    fn test_vote_equality() {
        let v1 = Vote::new("x", "y", 1);
        let v2 = Vote::new("x", "y", 1);
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_consensus_state_new() {
        let s = ConsensusState::new("n1");
        assert_eq!(s.node_id, "n1");
        assert_eq!(s.current_term, 0);
        assert!(s.voted_for.is_none());
        assert!(s.log.is_empty());
    }

    #[test]
    fn test_advance_term() {
        let mut s = ConsensusState::new("n1");
        s.cast_vote("n2");
        s.advance_term(5);
        assert_eq!(s.current_term, 5);
        assert!(s.voted_for.is_none());
    }

    #[test]
    fn test_advance_term_ignores_lower() {
        let mut s = ConsensusState::new("n1");
        s.advance_term(10);
        s.advance_term(5);
        assert_eq!(s.current_term, 10);
    }

    #[test]
    fn test_cast_vote_once() {
        let mut s = ConsensusState::new("n1");
        assert!(s.cast_vote("n2"));
        assert!(!s.cast_vote("n3")); // already voted
        assert_eq!(s.voted_for, Some("n2".to_string()));
    }

    #[test]
    fn test_log_append_and_last() {
        let mut s = ConsensusState::new("n1");
        assert_eq!(s.last_log_index(), 0);
        assert_eq!(s.last_log_term(), 0);
        s.append_entry(LogEntry::new(1, vec![1], 1));
        s.append_entry(LogEntry::new(2, vec![2], 2));
        assert_eq!(s.last_log_index(), 2);
        assert_eq!(s.last_log_term(), 2);
    }

    #[test]
    fn test_truncate_and_append() {
        let mut s = ConsensusState::new("n1");
        s.append_entry(LogEntry::new(1, vec![1], 1));
        s.append_entry(LogEntry::new(1, vec![2], 2));
        s.append_entry(LogEntry::new(2, vec![3], 3));
        s.truncate_and_append(0, vec![LogEntry::new(3, vec![9], 1)]);
        assert_eq!(s.log.len(), 1);
        assert_eq!(s.log[0].data, vec![9]);
    }

    #[test]
    fn test_election_start() {
        let mut election = RaftLeaderElection::new("n1", 150);
        election.add_peer("n2");
        election.add_peer("n3");
        let mut state = ConsensusState::new("n1");
        let vote = election.start_election(&mut state);
        assert_eq!(vote.voter_id, "n1");
        assert_eq!(vote.candidate_id, "n1");
        assert_eq!(vote.term, 1);
        assert_eq!(state.current_term, 1);
        assert_eq!(state.voted_for, Some("n1".to_string()));
    }

    #[test]
    fn test_election_win_with_majority() {
        let mut election = RaftLeaderElection::new("n1", 150);
        election.add_peer("n2");
        election.add_peer("n3");
        let mut state = ConsensusState::new("n1");
        election.start_election(&mut state);
        // Self vote is implicit
        assert!(election.receive_vote(Vote::new("n2", "n1", 1)));
    }

    #[test]
    fn test_election_no_win_without_majority() {
        let mut election = RaftLeaderElection::new("n1", 150);
        election.add_peer("n2");
        election.add_peer("n3");
        election.add_peer("n4");
        election.add_peer("n5");
        let mut state = ConsensusState::new("n1");
        election.start_election(&mut state);
        // Need 3 of 5; only self + n2 = 2 (but self isn't counted in receive_vote)
        assert!(!election.receive_vote(Vote::new("n2", "n1", 1)));
    }

    #[test]
    fn test_should_grant_vote() {
        let election = RaftLeaderElection::new("n2", 150);
        let state = ConsensusState::new("n2");
        assert!(election.should_grant_vote(&state, 1, 0, 0));
    }

    #[test]
    fn test_should_deny_vote_lower_term() {
        let election = RaftLeaderElection::new("n2", 150);
        let mut state = ConsensusState::new("n2");
        state.advance_term(5);
        assert!(!election.should_grant_vote(&state, 3, 0, 0));
    }

    #[test]
    fn test_should_deny_vote_already_voted() {
        let election = RaftLeaderElection::new("n2", 150);
        let mut state = ConsensusState::new("n2");
        state.cast_vote("n3");
        assert!(!election.should_grant_vote(&state, 1, 0, 0));
    }

    #[test]
    fn test_log_replication_append() {
        let mut r = LogReplication::new("n1");
        r.set_term(1);
        r.append(b"set x=1".to_vec());
        r.append(b"set y=2".to_vec());
        assert_eq!(r.log_len(), 2);
        assert_eq!(r.log()[0].term, 1);
        assert_eq!(r.log()[1].index, 2);
    }

    #[test]
    fn test_log_replication_to_follower() {
        let mut r = LogReplication::new("n1");
        r.set_term(1);
        r.append(b"cmd1".to_vec());
        r.append(b"cmd2".to_vec());
        r.add_follower("n2");
        let entries = r.entries_for("n2");
        assert_eq!(entries.len(), 2);
        r.confirm_replication("n2", 2);
        assert_eq!(r.replication_count(2), 2); // self + n2
    }

    #[test]
    fn test_apply_entries_follower() {
        let mut r = LogReplication::new("n2");
        let entries = vec![
            LogEntry::new(1, b"a".to_vec(), 1),
            LogEntry::new(1, b"b".to_vec(), 2),
        ];
        let count = r.apply_entries(0, 0, entries).unwrap();
        assert_eq!(count, 2);
        assert_eq!(r.log_len(), 2);
    }

    #[test]
    fn test_apply_entries_mismatch() {
        let mut r = LogReplication::new("n2");
        r.set_term(2);
        r.append(b"x".to_vec());
        let entries = vec![LogEntry::new(1, b"a".to_vec(), 1)];
        let result = r.apply_entries(1, 1, entries);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_index_advance() {
        let mut ci = CommitIndex::new();
        assert_eq!(ci.committed(), 0);
        let newly = ci.advance_to(3, 3, 2); // cluster=3, repl=2, majority=2
        assert_eq!(newly, 3);
        assert_eq!(ci.committed(), 3);
    }

    #[test]
    fn test_commit_index_no_advance_without_majority() {
        let mut ci = CommitIndex::new();
        let newly = ci.advance_to(3, 5, 2); // cluster=5, repl=2, majority=3
        assert_eq!(newly, 0);
        assert_eq!(ci.committed(), 0);
    }

    #[test]
    fn test_commit_apply() {
        let mut ci = CommitIndex::new();
        ci.advance_to(5, 3, 2);
        let applied = ci.apply_committed();
        assert_eq!(applied, 5);
        assert_eq!(ci.last_applied(), 5);
        assert!(ci.is_committed(3));
        assert!(ci.is_applied(3));
        assert!(!ci.is_applied(6));
    }
}
