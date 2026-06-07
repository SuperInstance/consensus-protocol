# Consensus Protocol

A Rust library implementing distributed consensus algorithms for fault-tolerant distributed systems. Provides Raft-style leader election, log replication, and commit index tracking.

## Why This Matters

In distributed systems, multiple nodes must agree on shared state despite network partitions, node failures, and message delays. Without consensus, distributed databases, coordination services (like etcd/Consul), and replicated state machines would be impossible to build correctly.

The **Raft consensus algorithm** (Ongaro & Ousterhout, 2014) decomposes consensus into three subproblems:

1. **Leader Election** — Select a leader when the current leader fails
2. **Log Replication** — The leader replicates its log to all followers
3. **Safety** — If any log entry is committed, no other entry with the same index will ever be committed

This crate provides clean, testable implementations of all three components.

## Architecture

### Vote

A `Vote` represents a single vote cast during a leader election round. Each vote records:
- `voter_id` — which node cast the vote
- `candidate_id` — who they voted for
- `term` — the election round (monotonically increasing)

In Raft, each node votes at most once per term, ensuring election safety (at most one leader per term).

### ConsensusState

`ConsensusState` holds the persistent state that must survive node restarts:
- `current_term` — the latest term the node has seen
- `voted_for` — who this node voted for in the current term (None if no vote)
- `log` — the complete replicated log

### RaftLeaderElection

`RaftLeaderElection` manages timeout-based leader election with randomized timeouts to prevent split votes. When a follower doesn't hear from a leader within the election timeout, it:

1. Increments its term
2. Transitions to candidate
3. Votes for itself
4. Requests votes from all peers

A candidate wins when it receives votes from a strict majority of the cluster.

### LogReplication

`LogReplication` manages the append-only log that is replicated from leader to followers. The leader tracks:
- `next_index` — the next log index to send to each follower
- `match_index` — the highest index known to be replicated on each follower

Followers validate log consistency by checking that the term at `prev_log_index` matches `prev_log_term`.

### CommitIndex

`CommitIndex` tracks which entries are safely committed (replicated on a majority). An entry is committed when the leader determines that a majority of nodes have stored it. Committed entries are never lost and can be safely applied to the state machine.

## Usage

```rust
use consensus_protocol::{Vote, ConsensusState, RaftLeaderElection, LogReplication, CommitIndex};

// Set up a 3-node cluster
let mut state = ConsensusState::new("node-1");
let mut election = RaftLeaderElection::new("node-1", 150); // 150ms timeout
election.add_peer("node-2");
election.add_peer("node-3");

// Start an election
let vote_request = election.start_election(&mut state);
assert_eq!(vote_request.term, 1);

// Receive votes and win election
let won = election.receive_vote(Vote::new("node-2", "node-1", 1));
assert!(won); // 2 of 3 = majority

// As leader, replicate log entries
let mut replication = LogReplication::new("node-1");
replication.set_term(1);
replication.append(b"SET x = 42".to_vec());
replication.append(b"SET y = 99".to_vec());
replication.add_follower("node-2");
replication.add_follower("node-3");

// Send entries and confirm replication
let entries = replication.entries_for("node-2");
replication.confirm_replication("node-2", 2);

// Commit entries
let mut commit = CommitIndex::new();
let newly_committed = commit.advance_to(2, 3, 2);
assert_eq!(newly_committed, 2);
```

## Mathematical Background

### Election Safety Proof

In Raft, at most one leader can be elected per term. Proof by contradiction:

- Assume nodes A and B both become leaders in term T
- Both must have received a majority of votes in term T
- Since a node votes at most once per term, the two majorities must overlap
- At least one node voted for both A and B — contradiction ∎

### Log Matching Property

If two entries in different logs have the same index and term, then:
1. They store the same command
2. All preceding entries are identical

This is maintained by the consistency check: followers reject AppendEntries with mismatched `(prev_log_index, prev_log_term)`.

### Leader Completeness

If a log entry is committed in term T, then all leaders for terms > T will contain that entry. This follows from the voting restriction: a node only grants its vote if the candidate's log is at least as up-to-date as its own.

## Performance Characteristics

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| Append entry | O(1) amortized | O(n) for n entries |
| Election | O(1) per vote | O(p) for p peers |
| Commit check | O(f) for f followers | O(f) |
| Log truncation | O(k) for k new entries | O(n) |

## Safety Guarantees

- **Election Safety**: At most one leader per term
- **Leader Append-Only**: Leaders never overwrite their own log
- **Log Matching**: Matching entries imply matching prefixes
- **Leader Completeness**: Committed entries appear in all future leaders
- **State Machine Safety**: If a node applies entry at index i, no other node applies a different entry at index i

## Safety Properties

- **Election Safety**: At most one leader per term
- **Leader Append-Only**: Leaders never overwrite their own log
- **Log Matching**: Matching entries imply matching prefixes
- **Leader Completeness**: Committed entries appear in all future leaders
- **State Machine Safety**: If a node applies entry at index i, no other node applies a different entry at index i

## Implementation Notes

- All structures are `Send + Sync` safe (no interior mutability with `RefCell`)
- The election timeout should be randomized in production to avoid split votes
- Log compaction (snapshotting) is not implemented but can be layered on top
- This is a library, not a full Raft implementation — it provides building blocks

## References

- Ongaro, D., & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm*. USENIX ATC.
- Howard, H., Malkhi, D., & Spiegelman, A. (2016). *Flexible Paxos: Quorum Intersection Revisited*. PODC.

## License

MIT
