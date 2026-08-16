// chaos_test.go contains aggressive multi-node integration tests for the
// honeybadger package: concurrent write storms (including a cluster
// membership change in the middle of the storm), follower restart catch-up
// via log replication, follower catch-up via InstallSnapshot -> FSM Restore
// (snapshot taken with a very low SnapshotThreshold while the follower is
// down), and a mixed Set/Delete/Batch/TTL storm with concurrent follower
// reads. All tests use real TCP transports on 127.0.0.1 with dynamically
// allocated ports and t.TempDir() data directories.
//
// All helpers in this file carry a "chaos" prefix so they cannot collide
// with helpers defined in the other internal test files.
package honeybadger

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// chaosPoll is the interval between condition re-checks in poll loops.
	chaosPoll = 120 * time.Millisecond
	// chaosConvergeTO bounds a single "all nodes converge" wait.
	chaosConvergeTO = 90 * time.Second
)

// chaosLocal is the read mode used by all convergence checks: deliberately
// local (eventually consistent), on any node.
var chaosLocal = ReadOptions{Mode: ReadLocal}

// chaosNode couples an open DB with the Config it was opened from so tests
// can close and re-open it in place (simulating a process restart).
type chaosNode struct {
	db         *DB
	cfg        Config
	newCluster bool
}

// chaosFreePort returns a currently-free TCP port on 127.0.0.1. There is an
// inherent race between releasing the port and the Raft transport binding
// it, so callers retry Open on failure.
func chaosFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("chaos: allocate free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// chaosIsLeader reports whether the node currently believes it is leader.
func chaosIsLeader(db *DB) bool {
	st, err := db.Status()
	return err == nil && st.State == StateLeader
}

// chaosOpen opens a node in a fresh t.TempDir(), retrying with a new port
// if the bind loses the free-port race. The node is closed at test cleanup.
// newCluster must be true for exactly the first node of a new cluster;
// plain Open never bootstraps.
func chaosOpen(t *testing.T, id string, newCluster bool, snapThreshold uint64) *chaosNode {
	t.Helper()
	dir := t.TempDir()
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		cfg := Config{
			NodeID:   id,
			RaftBind: fmt.Sprintf("127.0.0.1:%d", chaosFreePort(t)),
			DataDir:  dir,
			Advanced: AdvancedConfig{
				ApplyTimeout:      10 * time.Second,
				SnapshotThreshold: snapThreshold,
			},
		}
		var db *DB
		var err error
		if newCluster {
			db, err = Open(cfg, NewCluster())
		} else {
			db, err = Open(cfg)
		}
		if err == nil {
			n := &chaosNode{db: db, cfg: cfg, newCluster: newCluster}
			t.Cleanup(func() { n.db.Close() })
			return n
		}
		lastErr = err
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("chaos: open node %q after retries: %v", id, lastErr)
	return nil
}

// chaosReopen re-opens a node from its original Config (same NodeID,
// RaftBind and DataDir) after it was closed, simulating a process restart.
// The same RaftBind must be reused because the cluster configuration
// records the server's address.
func chaosReopen(t *testing.T, n *chaosNode) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		var db *DB
		var err error
		if n.newCluster {
			db, err = Open(n.cfg, NewCluster())
		} else {
			db, err = Open(n.cfg)
		}
		if err == nil {
			n.db = db
			return
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("chaos: reopen node %q: %v", n.cfg.NodeID, lastErr)
}

// chaosWaitFor polls cond until it holds or the timeout expires.
func chaosWaitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("chaos: timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(chaosPoll)
	}
}

// chaosLeader returns the first node that currently reports itself leader,
// polling until one emerges. Must only be called from the main test
// goroutine (it fails the test on timeout).
func chaosLeader(t *testing.T, timeout time.Duration, nodes ...*chaosNode) *chaosNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, n := range nodes {
			if n.db != nil && chaosIsLeader(n.db) {
				return n
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("chaos: no leader emerged among %d nodes within %s", len(nodes), timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// chaosApplyOnLeader runs fn on the current leader, re-resolving the leader
// and retrying while fn returns ErrNotLeader. Any other error fails the
// test. Main test goroutine only.
func chaosApplyOnLeader(t *testing.T, timeout time.Duration, nodes []*chaosNode, fn func(*DB) error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ldr := chaosLeader(t, 30*time.Second, nodes...)
		err := fn(ldr.db)
		if err == nil {
			return
		}
		if errors.Is(err, ErrNotLeader) && time.Now().Before(deadline) {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		t.Fatalf("chaos: operation failed on leader %q: %v", ldr.cfg.NodeID, err)
	}
}

// chaosSetOnAnyLeader is a goroutine-safe variant of chaosApplyOnLeader for
// write storms: it returns errors instead of failing the test.
func chaosSetOnAnyLeader(nodes []*chaosNode, key, value string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		for _, n := range nodes {
			if n.db != nil && chaosIsLeader(n.db) {
				err := n.db.Set(key, value)
				if err == nil || !errors.Is(err, ErrNotLeader) {
					return err
				}
			}
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("chaos: no leader available to write key %q", key)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// chaosJoin adds joiner to the cluster via the current leader.
func chaosJoin(t *testing.T, members []*chaosNode, joiner *chaosNode) {
	t.Helper()
	chaosApplyOnLeader(t, 60*time.Second, members, func(db *DB) error {
		return db.AddVoter(Node{ID: joiner.cfg.NodeID, RaftAddr: joiner.cfg.RaftBind})
	})
}

// chaosWaitAllLeaders blocks until every node knows a cluster leader.
func chaosWaitAllLeaders(t *testing.T, nodes ...*chaosNode) {
	t.Helper()
	for _, n := range nodes {
		if _, err := n.db.WaitForLeader(45 * time.Second); err != nil {
			t.Fatalf("chaos: node %q: %v", n.cfg.NodeID, err)
		}
	}
}

// chaosCluster bootstraps a 3-node cluster and returns it fully formed.
func chaosCluster(t *testing.T, prefix string, snapThreshold uint64) []*chaosNode {
	t.Helper()
	n1 := chaosOpen(t, prefix+"-1", true, snapThreshold)
	n2 := chaosOpen(t, prefix+"-2", false, snapThreshold)
	n3 := chaosOpen(t, prefix+"-3", false, snapThreshold)
	nodes := []*chaosNode{n1, n2, n3}
	// n1 has already completed its first election: Open with NewCluster
	// waited for it.
	chaosJoin(t, nodes, n2)
	chaosJoin(t, nodes, n3)
	chaosWaitAllLeaders(t, nodes...)
	return nodes
}

// chaosScan reads the full keyspace below prefix from one node into a map,
// via a deliberately local, unlimited scan.
func chaosScan(db *DB, prefix string) (map[string]string, error) {
	entries, err := db.ScanPrefixBytes([]byte(prefix), ScanOptions{
		Read:      chaosLocal,
		Unlimited: true,
	})
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[string(e.Key)] = string(e.Value)
	}
	return m, nil
}

// chaosMapsEqual reports whether got contains exactly want.
func chaosMapsEqual(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, wv := range want {
		if gv, ok := got[k]; !ok || gv != wv {
			return false
		}
	}
	return true
}

// chaosMapDiff samples up to 3 missing, extra and wrong-value keys for
// diagnostics.
func chaosMapDiff(got, want map[string]string) (missing, extra, wrong []string) {
	for k, wv := range want {
		gv, ok := got[k]
		switch {
		case !ok:
			if len(missing) < 3 {
				missing = append(missing, k)
			}
		case gv != wv:
			if len(wrong) < 3 {
				wrong = append(wrong, fmt.Sprintf("%s got=%q want=%q", k, gv, wv))
			}
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok && len(extra) < 3 {
			extra = append(extra, k)
		}
	}
	return missing, extra, wrong
}

// chaosWaitConverged polls until every node serves exactly the expected
// key/value state below prefix.
func chaosWaitConverged(t *testing.T, timeout time.Duration, prefix string, expected map[string]string, nodes ...*chaosNode) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var detail string
	for {
		allMatch := true
		detail = ""
		for _, n := range nodes {
			got, err := chaosScan(n.db, prefix)
			switch {
			case err != nil:
				allMatch = false
				detail += fmt.Sprintf(" node %q scan error: %v;", n.cfg.NodeID, err)
			case !chaosMapsEqual(got, expected):
				allMatch = false
				missing, extra, wrong := chaosMapDiff(got, expected)
				detail += fmt.Sprintf(" node %q has %d keys, want %d (missing e.g. %v, extra e.g. %v, wrong e.g. %v);",
					n.cfg.NodeID, len(got), len(expected), missing, extra, wrong)
			}
		}
		if allMatch {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("chaos: nodes did not converge within %s:%s", timeout, detail)
		}
		time.Sleep(chaosPoll)
	}
}

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

// TestChaosWriteStormJoinDuring hammers the leader with 8 goroutines x 50
// disjoint Sets and joins a 4th node in the middle of the storm. Afterwards
// all 4 nodes must serve byte-identical values for all 400 keys, proving
// the late joiner also received every entry committed before it joined.
func TestChaosWriteStormJoinDuring(t *testing.T) {
	nodes := chaosCluster(t, "storm", 0)

	const writers, perWriter = 8, 50
	expected := make(map[string]string, writers*perWriter)
	for w := 0; w < writers; w++ {
		for k := 0; k < perWriter; k++ {
			key := fmt.Sprintf("storm/w%d/k%03d", w, k)
			expected[key] = fmt.Sprintf("storm-val-%d-%03d", w, k)
		}
	}

	writeErrs := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for k := 0; k < perWriter; k++ {
				key := fmt.Sprintf("storm/w%d/k%03d", w, k)
				val := fmt.Sprintf("storm-val-%d-%03d", w, k)
				if err := chaosSetOnAnyLeader(nodes, key, val); err != nil && writeErrs[w] == nil {
					writeErrs[w] = err
				}
			}
		}(w)
	}
	close(start)

	// Join a 4th node while the storm is in flight.
	n4 := chaosOpen(t, "storm-4", false, 0)
	chaosJoin(t, nodes, n4)
	all := []*chaosNode{nodes[0], nodes[1], nodes[2], n4}

	wg.Wait()
	for w, err := range writeErrs {
		if err != nil {
			t.Fatalf("chaos: storm writer %d failed: %v", w, err)
		}
	}
	if _, err := n4.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: late-joining node %q: %v", n4.cfg.NodeID, err)
	}

	chaosWaitConverged(t, chaosConvergeTO, "storm/", expected, all...)
}

// TestChaosFollowerRestartCatchUp closes a follower cleanly, commits more
// data on the leader while it is down, then re-opens it with the same
// NodeID/DataDir/RaftBind and verifies it catches up via log replication
// and serves the union of everything committed before and after its
// shutdown.
func TestChaosFollowerRestartCatchUp(t *testing.T) {
	nodes := chaosCluster(t, "restart", 0)

	expected := map[string]string{}
	mutsA := make([]Mutation, 0, 60)
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("restart/a/k%02d", i)
		v := fmt.Sprintf("restart-val-a-%02d", i)
		mutsA = append(mutsA, SetOp(k, v))
		expected[k] = v
	}
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error { return db.Batch(mutsA...) })
	chaosWaitConverged(t, 60*time.Second, "restart/", expected, nodes...)

	// Pick a follower and shut it down cleanly.
	follower := nodes[1]
	if chaosIsLeader(follower.db) {
		follower = nodes[2]
	}
	if err := follower.db.Close(); err != nil {
		t.Fatalf("chaos: close follower %q: %v", follower.cfg.NodeID, err)
	}

	// Batch B on the leader while the follower is down: 60 new keys plus
	// deletes of 5 batch-A keys, so the catch-up must apply both sets and
	// deletes.
	mutsB := make([]Mutation, 0, 65)
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("restart/b/k%02d", i)
		v := fmt.Sprintf("restart-val-b-%02d", i)
		mutsB = append(mutsB, SetOp(k, v))
		expected[k] = v
	}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("restart/a/k%02d", i)
		mutsB = append(mutsB, DeleteOp(k))
		delete(expected, k)
	}
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error { return db.Batch(mutsB...) })

	// The two survivors must converge on their own (quorum never broke).
	survivors := []*chaosNode{nodes[0], nodes[2]}
	if survivors[1] == follower {
		survivors[1] = nodes[1]
	}
	chaosWaitConverged(t, 60*time.Second, "restart/", expected, survivors...)

	// Restart the follower; it must catch up and serve batch A + B.
	chaosReopen(t, follower)
	if _, err := follower.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: restarted follower %q: %v", follower.cfg.NodeID, err)
	}
	chaosWaitConverged(t, chaosConvergeTO, "restart/", expected, nodes...)
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

// TestChaosMixedOpStorm interleaves Set/Delete/Batch/TTL operations on the
// leader while readers hammer the followers, then compares the full
// keyspace of every node against an in-test model of the expected state.
// TTLs are 120s so no key can expire before the comparison completes.
func TestChaosMixedOpStorm(t *testing.T) {
	nodes := chaosCluster(t, "mixed", 0)

	rng := rand.New(rand.NewSource(20260815))
	const keyspace, numOps = 150, 600
	keyFn := func(i int) string { return fmt.Sprintf("mixed/k%03d", i) }
	model := make(map[string]string, keyspace)

	// Concurrent readers on the followers for the duration of the storm:
	// local Get must only ever return a value or ErrKeyNotFound.
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	var readErrCount atomic.Int32
	firstErr := make(chan string, 1)
	for ni, n := range []*chaosNode{nodes[1], nodes[2]} {
		for r := 0; r < 2; r++ {
			readerWG.Add(1)
			go func(n *chaosNode, seed int64) {
				defer readerWG.Done()
				rr := rand.New(rand.NewSource(seed))
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, err := n.db.GetWithOptions(keyFn(rr.Intn(keyspace)), chaosLocal)
					if err != nil && !errors.Is(err, ErrKeyNotFound) {
						readErrCount.Add(1)
						select {
						case firstErr <- fmt.Sprintf("node %q: %v", n.cfg.NodeID, err):
						default:
						}
					}
				}
			}(n, int64(ni*10+r+1))
		}
	}

	for op := 0; op < numOps; op++ {
		switch roll := rng.Float64(); {
		case roll < 0.45: // Set
			k := keyFn(rng.Intn(keyspace))
			v := fmt.Sprintf("mixed-val-op%04d", op)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Set(k, v)
			})
			model[k] = v
		case roll < 0.65: // Delete (possibly of a missing key: not an error)
			k := keyFn(rng.Intn(keyspace))
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Delete(k)
			})
			delete(model, k)
		case roll < 0.85: // Batch: 3-8 sets + 1-3 deletes over distinct keys
			nSets := 3 + rng.Intn(6)
			nDels := 1 + rng.Intn(3)
			picked := map[int]bool{}
			pick := func() int {
				for {
					i := rng.Intn(keyspace)
					if !picked[i] {
						picked[i] = true
						return i
					}
				}
			}
			muts := make([]Mutation, 0, nSets+nDels)
			sets := make([][2]string, 0, nSets)
			for i := 0; i < nSets; i++ {
				k := keyFn(pick())
				v := fmt.Sprintf("mixed-val-op%04d-%d", op, i)
				muts = append(muts, SetOp(k, v))
				sets = append(sets, [2]string{k, v})
			}
			dels := make([]string, 0, nDels)
			for i := 0; i < nDels; i++ {
				k := keyFn(pick())
				muts = append(muts, DeleteOp(k))
				dels = append(dels, k)
			}
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Batch(muts...)
			})
			// Mirror the FSM's apply order: sets first, then deletes.
			for _, s := range sets {
				model[s[0]] = s[1]
			}
			for _, d := range dels {
				delete(model, d)
			}
		default: // Set with TTL, long enough to outlive the comparison
			k := keyFn(rng.Intn(keyspace))
			v := fmt.Sprintf("mixed-val-op%04d-ttl", op)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Set(k, v, WithTTL(120*time.Second))
			})
			model[k] = v
		}
	}

	close(stop)
	readerWG.Wait()
	if readErrCount.Load() > 0 {
		t.Fatalf("chaos: followers served %d unexpected read errors during the storm, first: %s",
			readErrCount.Load(), <-firstErr)
	}

	// Final full-keyspace comparison across all nodes.
	chaosWaitConverged(t, chaosConvergeTO, "mixed/", model, nodes...)
}

// TestChaosTTLAcrossRestart verifies that TTLs replicate as absolute
// expiries: a follower restarted inside the TTL window must serve the keys
// while they are unexpired and must report ErrKeyNotFound once the original
// expiry passes — the restart must not extend the lease (replaying the log
// used to restamp a fresh TTL per apply). The restart is deliberately
// positioned late in the TTL window so a restart-extended lease would
// outlive the expiry deadline by a wide, unflaky margin.
func TestChaosTTLAcrossRestart(t *testing.T) {
	nodes := chaosCluster(t, "ttl", 0)

	const ttl = 30 * time.Second
	ttlKeys := map[string]string{
		"ttl/k1": "ttl-val-1",
		"ttl/k2": "ttl-val-2",
		"ttl/k3": "ttl-val-3",
		"ttl/k4": "ttl-val-4",
	}
	controls := map[string]string{
		"ttl/ctl-a": "ctl-a",
		"ttl/ctl-b": "ctl-b",
	}
	expected := map[string]string{}
	for k, v := range ttlKeys {
		expected[k] = v
	}
	for k, v := range controls {
		expected[k] = v
	}

	// Anchor the TTL window before the first TTL write: each write stamps
	// its absolute expiry on the leader at submit time (now+ttl, truncated
	// to Badger's one-second granularity), so every TTL key expires
	// between ~29s and ~31s after writtenAt.
	writtenAt := time.Now()

	// Two TTL Sets plus a Batch mixing TTL pairs and a persistent pair,
	// plus one persistent control Set.
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
		return db.Set("ttl/k1", "ttl-val-1", WithTTL(ttl))
	})
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
		return db.Set("ttl/k2", "ttl-val-2", WithTTL(ttl))
	})
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
		return db.Batch(
			SetOp("ttl/k3", "ttl-val-3", WithTTL(ttl)),
			SetOp("ttl/k4", "ttl-val-4", WithTTL(ttl)),
			SetOp("ttl/ctl-b", "ctl-b"),
		)
	})
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
		return db.Set("ttl/ctl-a", "ctl-a")
	})
	chaosWaitConverged(t, 15*time.Second, "ttl/", expected, nodes...)

	// Pick a follower and position its restart late in the TTL window:
	// closing at ~writtenAt+15s means log replay lands ~17-20s in, leaving
	// ~10s to confirm the keys are still served, and a restart-extended
	// lease would expire at ~writtenAt+50s, far past the expiry deadline.
	follower := nodes[1]
	if chaosIsLeader(follower.db) {
		follower = nodes[2]
	}
	if d := time.Until(writtenAt.Add(15 * time.Second)); d > 0 {
		time.Sleep(d)
	}
	if err := follower.db.Close(); err != nil {
		t.Fatalf("chaos: close follower %q: %v", follower.cfg.NodeID, err)
	}
	chaosReopen(t, follower)
	if _, err := follower.db.WaitForLeader(30 * time.Second); err != nil {
		t.Fatalf("chaos: restarted follower %q: %v", follower.cfg.NodeID, err)
	}

	// (a) While still unexpired, the restarted follower must converge and
	// serve every TTL key and both controls.
	chaosWaitFor(t, time.Until(writtenAt.Add(28*time.Second)),
		"restarted follower to replay and serve TTL keys before their expiry", func() bool {
			for k, v := range expected {
				got, err := follower.db.GetWithOptions(k, chaosLocal)
				if err != nil || got != v {
					return false
				}
			}
			return true
		})

	// (b) After the original expiry (~writtenAt+30s) every node, including
	// the restarted one, must report ErrKeyNotFound for all TTL keys, and
	// the persistent controls must survive.
	expired := map[string]time.Time{}
	deadline := writtenAt.Add(45 * time.Second)
	var detail string
	for {
		allGone := true
		detail = ""
		for _, n := range nodes {
			nodeGone := true
			for k := range ttlKeys {
				_, err := n.db.GetWithOptions(k, chaosLocal)
				switch {
				case err == nil:
					nodeGone = false
					detail += fmt.Sprintf(" node %q still serves %q;", n.cfg.NodeID, k)
				case !errors.Is(err, ErrKeyNotFound):
					t.Fatalf("chaos: node %q Get(%q) returned unexpected error: %v", n.cfg.NodeID, k, err)
				}
			}
			if nodeGone {
				if _, seen := expired[n.cfg.NodeID]; !seen {
					expired[n.cfg.NodeID] = time.Now()
				}
			} else {
				allGone = false
			}
		}
		if allGone {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("chaos: TTL keys did not expire on schedule within %s of the write%s", deadline.Sub(writtenAt), detail)
		}
		time.Sleep(500 * time.Millisecond)
	}
	for id, at := range expired {
		t.Logf("node %s reported all TTL keys expired %s after the write (ttl=%s)", id, at.Sub(writtenAt).Round(time.Second), ttl)
	}

	// Persistent keys must be intact everywhere after the TTL storm.
	for _, n := range nodes {
		for k, v := range controls {
			got, err := n.db.GetWithOptions(k, chaosLocal)
			if err != nil || got != v {
				t.Fatalf("chaos: node %q lost persistent key %q: got %q, err %v", n.cfg.NodeID, k, got, err)
			}
		}
	}
}
