// Snapshot catch-up chaos test plus the snapshot-store introspection
// helpers specific to it. The scenario skips under -short.
package honeybadger

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// chaosSnapshotMeta is the parsed identity of one stored snapshot. The
// ID's timestamp is deliberately not compared: a received snapshot is
// stored under a fresh local ID with the sender's term and index but a new
// local timestamp.
type chaosSnapshotMeta struct {
	term  uint64
	index uint64
}

// chaosSnapshotIDs lists the snapshots stored by a node as
// snapshot-id -> {term, index}, by reading its FileSnapshotStore directory
// at <DataDir>/raft/snapshots (Open passes <DataDir>/raft as the store
// base and Raft appends "snapshots"). Snapshot IDs have the form
// "<term>-<index>-<unixmilli>"; directories still being written carry a
// ".tmp" suffix and are skipped.
func chaosSnapshotIDs(dataDir string) map[string]chaosSnapshotMeta {
	out := map[string]chaosSnapshotMeta{}
	entries, err := os.ReadDir(filepath.Join(dataDir, "raft", "snapshots"))
	if err != nil {
		return out
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		var meta chaosSnapshotMeta
		var ts uint64
		if _, err := fmt.Sscanf(e.Name(), "%d-%d-%d", &meta.term, &meta.index, &ts); err == nil {
			out[e.Name()] = meta
		}
	}
	return out
}

// chaosLastSnapshotTermIndex returns the node's last snapshot term and
// index from RawRaftStats, failing the test when either stat is missing or
// malformed — a silently zeroed value would make the snapshot proof
// meaningless.
func chaosLastSnapshotTermIndex(t *testing.T, db *DB) (term, index uint64) {
	t.Helper()
	stats := db.RawRaftStats()
	index, err := strconv.ParseUint(stats["last_snapshot_index"], 10, 64)
	if err != nil {
		t.Fatalf("chaos: parse last_snapshot_index %q: %v", stats["last_snapshot_index"], err)
	}
	term, err = strconv.ParseUint(stats["last_snapshot_term"], 10, 64)
	if err != nil {
		t.Fatalf("chaos: parse last_snapshot_term %q: %v", stats["last_snapshot_term"], err)
	}
	return term, index
}

// chaosLastSuccessfulCommandIndex returns the node's Status.AppliedIndex:
// the most recent command index whose Badger transaction succeeded.
func chaosLastSuccessfulCommandIndex(t *testing.T, n *chaosNode) uint64 {
	t.Helper()
	st, err := n.db.Status()
	if err != nil {
		t.Fatalf("chaos: status of %q: %v", n.cfg.NodeID, err)
	}
	return st.AppliedIndex
}

// TestChaosSnapshotCatchUp exercises follower catch-up through Raft
// snapshots. A follower is closed, the leader commits several hundred more
// entries — comfortably more than the configured trailing-log retention
// (DB sets TrailingLogs = SnapshotThreshold = 64) — and Snapshot() then
// compacts the log deterministically. When the follower rejoins, ordinary
// AppendEntries can no longer reach back to it, so the leader streams an
// InstallSnapshot that replaces the follower's Badger DB via FSM Restore.
// The test proves the snapshot path was used by requiring the follower's
// snapshot store to contain a snapshot at the leader's snapshot term and
// index.
func TestChaosSnapshotCatchUp(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos: skipping long-running scenario in -short mode")
	}
	const threshold = 64
	nodes := chaosCluster(t, "snap", threshold)

	expected := map[string]string{}
	writeKeys := func(prefix string, count int) {
		for i := 0; i < count; i++ {
			k := fmt.Sprintf("%sk%05d", prefix, i)
			v := fmt.Sprintf("%sval-%05d", prefix, i)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Set(k, v)
			})
			expected[k] = v
		}
	}

	// Establish a converged baseline, then take one follower down.
	writeKeys("snap/a/", 100)
	chaosWaitConverged(t, 60*time.Second, "snap/", expected, nodes...)

	restartedFollower := nodes[1]
	if chaosIsLeader(restartedFollower.db) {
		restartedFollower = nodes[2]
	}
	onlineFollower := nodes[2]
	if onlineFollower == restartedFollower {
		onlineFollower = nodes[1]
	}

	appliedBefore := chaosLastSuccessfulCommandIndex(t, restartedFollower)
	if err := restartedFollower.db.Close(); err != nil {
		t.Fatalf("chaos: close follower %q: %v", restartedFollower.cfg.NodeID, err)
	}

	// Commit a few hundred entries while the follower is down: more than
	// the trailing-log retention, so compaction below drops entries the
	// follower still needs.
	writeKeys("snap/b/", 300)

	// Compact the leader's log deterministically: when Snapshot returns,
	// the snapshot is persisted and logs up to
	// min(snapshotIndex, lastLog-TrailingLogs) are deleted.
	leader := chaosLeader(t, 30*time.Second, nodes[0], onlineFollower)
	if err := leader.db.Snapshot(); err != nil {
		t.Fatalf("chaos: leader snapshot: %v", err)
	}

	// Prove ordinary log replication cannot catch the follower up: after
	// compaction the leader's oldest retained log is no earlier than
	// snapshotIndex-TrailingLogs (raft's compactLogsWithTrailing), so it
	// suffices that the snapshot index lies more than TrailingLogs past
	// the follower's applied index when it was stopped. The sample is the
	// latest successful command index, which equals the last applied log
	// index here: this test injects no FSM failures and samples before any
	// restore.
	leaderSnapTerm, leaderSnapIdx := chaosLastSnapshotTermIndex(t, leader.db)
	if leaderSnapIdx <= threshold+appliedBefore {
		t.Fatalf("chaos: test setup failed to force the snapshot path: leader snapshot index=%d, follower applied=%d (need snapshot index > %d+%d)",
			leaderSnapIdx, appliedBefore, threshold, appliedBefore)
	}

	// The two survivors must hold the full state before the follower returns.
	chaosWaitConverged(t, 60*time.Second, "snap/", expected, nodes[0], onlineFollower)

	// Restart the follower: only InstallSnapshot can catch it up now.
	chaosReopen(t, restartedFollower)
	if _, err := restartedFollower.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: follower %q after snapshot restart: %v", restartedFollower.cfg.NodeID, err)
	}
	chaosWaitConverged(t, 60*time.Second, "snap/", expected, nodes...)

	// Prove the InstallSnapshot -> FSM Restore path was actually taken: the
	// follower's snapshot store must now hold a snapshot at the leader's
	// snapshot term and index (the timestamp is never compared — a received
	// snapshot is stored under a fresh local ID). The follower cannot have
	// taken that snapshot itself: its own snapshot check only starts
	// ticking 120-240s after its restart, seconds ago. Combined with the
	// compaction precondition above, receiving the leader's snapshot via
	// InstallSnapshot is the only way it could hold one.
	leaderSnaps := chaosSnapshotIDs(leader.cfg.DataDir)
	followerSnaps := chaosSnapshotIDs(restartedFollower.cfg.DataDir)
	found := ""
	for id, meta := range followerSnaps {
		if meta.term == leaderSnapTerm && meta.index == leaderSnapIdx {
			found = id
			break
		}
	}
	if found == "" {
		t.Fatalf("chaos: follower converged without receiving the leader's snapshot "+
			"(leader snapshots=%v, follower snapshots=%v): InstallSnapshot/Restore path not exercised",
			leaderSnaps, followerSnaps)
	}
	t.Logf("follower restored from leader snapshot term %d index %d (local snapshot %s) and converged on %d keys",
		leaderSnapTerm, leaderSnapIdx, found, len(expected))
}
