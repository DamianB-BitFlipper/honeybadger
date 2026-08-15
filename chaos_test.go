// chaos_test.go contains aggressive multi-node integration tests for the
// honeybadger package: concurrent write storms (including a cluster
// membership change in the middle of the storm), follower restart catch-up
// via log replication, follower catch-up via InstallSnapshot -> FSM Restore
// (snapshot taken with a very low SnapshotThreshold while the follower is
// down), and a mixed Set/Delete/Batch/SetWithTTL storm with concurrent
// follower reads. All tests use real TCP transports on 127.0.0.1 with
// dynamically allocated ports and t.TempDir() data directories.
//
// All helpers in this file carry a "chaos" prefix so they cannot collide
// with helpers defined in honeybadger_test.go.
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

// chaosNode couples an open DB with the Config it was opened from so tests
// can close and re-open it in place (simulating a process restart).
type chaosNode struct {
	db  *DB
	cfg Config
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

// chaosOpen opens a node in a fresh t.TempDir(), retrying with a new port if
// the bind loses the free-port race. The node is closed at test cleanup.
func chaosOpen(t *testing.T, id string, bootstrap bool, snapThreshold uint64) *chaosNode {
	t.Helper()
	dir := t.TempDir()
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		cfg := Config{
			NodeID:            id,
			RaftBind:          fmt.Sprintf("127.0.0.1:%d", chaosFreePort(t)),
			DataDir:           dir,
			Bootstrap:         bootstrap,
			ApplyTimeout:      10 * time.Second,
			SnapshotThreshold: snapThreshold,
		}
		db, err := Open(cfg)
		if err == nil {
			n := &chaosNode{db: db, cfg: cfg}
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
		db, err := Open(n.cfg)
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
			if n.db != nil && n.db.IsLeader() {
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
func chaosSetOnAnyLeader(nodes []*chaosNode, key, value []byte) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		for _, n := range nodes {
			if n.db != nil && n.db.IsLeader() {
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
		return db.Join(joiner.cfg.NodeID, joiner.cfg.RaftBind)
	})
}

// chaosWaitAllLeaders blocks until every node knows a cluster leader.
func chaosWaitAllLeaders(t *testing.T, nodes ...*chaosNode) {
	t.Helper()
	for _, n := range nodes {
		if err := n.db.WaitForLeader(45 * time.Second); err != nil {
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
	if err := n1.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: bootstrap node %q: %v", n1.cfg.NodeID, err)
	}
	chaosJoin(t, nodes, n2)
	chaosJoin(t, nodes, n3)
	chaosWaitAllLeaders(t, nodes...)
	return nodes
}

// chaosScan reads the full keyspace below prefix from one node into a map.
func chaosScan(db *DB, prefix string) (map[string]string, error) {
	pairs, err := db.PrefixScan([]byte(prefix), 0)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		m[string(p.Key)] = string(p.Value)
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

// chaosSnapshotIDs lists the snapshots stored by a node as
// snapshot-id -> raft-index, by reading its FileSnapshotStore directory.
// Snapshot IDs have the form "<term>-<index>-<unixmilli>"; directories
// still being written carry a ".tmp" suffix and are skipped.
//
// The store root is DataDir/raft/snapshots. Both that directory and a
// doubly-nested DataDir/raft/snapshots/snapshots are scanned: raft's
// NewFileSnapshotStore appends its own "snapshots" element to whatever
// base directory it is given, so the effective layout depends on the base
// the implementation passes. Accepting both keeps this helper correct
// across either choice.
func chaosSnapshotIDs(dataDir string) map[string]uint64 {
	out := map[string]uint64{}
	roots := []string{
		filepath.Join(dataDir, "raft", "snapshots"),
		filepath.Join(dataDir, "raft", "snapshots", "snapshots"),
	}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
				continue
			}
			var term, index, ts uint64
			if _, err := fmt.Sscanf(e.Name(), "%d-%d-%d", &term, &index, &ts); err == nil {
				out[e.Name()] = index
			}
		}
	}
	return out
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
				if err := chaosSetOnAnyLeader(nodes, []byte(key), []byte(val)); err != nil && writeErrs[w] == nil {
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
	if err := n4.db.WaitForLeader(45 * time.Second); err != nil {
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
	setsA := make([]Pair, 0, 60)
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("restart/a/k%02d", i)
		v := fmt.Sprintf("restart-val-a-%02d", i)
		setsA = append(setsA, Pair{Key: []byte(k), Value: []byte(v)})
		expected[k] = v
	}
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error { return db.Batch(setsA, nil) })
	chaosWaitConverged(t, 60*time.Second, "restart/", expected, nodes...)

	// Pick a follower and shut it down cleanly.
	follower := nodes[1]
	if follower.db.IsLeader() {
		follower = nodes[2]
	}
	if err := follower.db.Close(); err != nil {
		t.Fatalf("chaos: close follower %q: %v", follower.cfg.NodeID, err)
	}

	// Batch B on the leader while the follower is down: 60 new keys plus
	// deletes of 5 batch-A keys, so the catch-up must apply both sets and
	// deletes.
	setsB := make([]Pair, 0, 60)
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("restart/b/k%02d", i)
		v := fmt.Sprintf("restart-val-b-%02d", i)
		setsB = append(setsB, Pair{Key: []byte(k), Value: []byte(v)})
		expected[k] = v
	}
	dels := make([][]byte, 0, 5)
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("restart/a/k%02d", i)
		dels = append(dels, []byte(k))
		delete(expected, k)
	}
	chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error { return db.Batch(setsB, dels) })

	// The two survivors must converge on their own (quorum never broke).
	survivors := []*chaosNode{nodes[0], nodes[2]}
	if survivors[1] == follower {
		survivors[1] = nodes[1]
	}
	chaosWaitConverged(t, 60*time.Second, "restart/", expected, survivors...)

	// Restart the follower; it must catch up and serve batch A + B.
	chaosReopen(t, follower)
	if err := follower.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: restarted follower %q: %v", follower.cfg.NodeID, err)
	}
	chaosWaitConverged(t, chaosConvergeTO, "restart/", expected, nodes...)
}

// TestChaosSnapshotCatchUp exercises follower catch-up through Raft
// snapshots. Phase 1 restarts a follower after ~400 committed entries at a
// low SnapshotThreshold (catch-up via ordinary log replication, since raft
// keeps 10240 trailing logs). Phase 2 takes the follower down again, commits
// 12000 more entries (more than raft's hardcoded TrailingLogs of 10240),
// waits for the leader's periodic snapshot check to fire (SnapshotInterval
// is raft's 120s default, so this can take minutes), then restarts the
// follower: the leader can no longer serve it logs and must stream an
// InstallSnapshot, which wipes and reloads the follower's Badger DB via FSM
// Restore. The test proves the snapshot path was used by requiring the
// follower's snapshot store to contain the leader's exact snapshot ID.
func TestChaosSnapshotCatchUp(t *testing.T) {
	const threshold = 64
	nodes := chaosCluster(t, "snap", threshold)

	expected := map[string]string{}
	writeKeys := func(prefix string, count int) {
		for i := 0; i < count; i++ {
			k := fmt.Sprintf("%sk%05d", prefix, i)
			v := fmt.Sprintf("%sval-%05d", prefix, i)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Set([]byte(k), []byte(v))
			})
			expected[k] = v
		}
	}

	// ---------------- phase 1: restart, catch up via log replication ----
	writeKeys("snap/p1/", 200)
	chaosWaitConverged(t, 60*time.Second, "snap/", expected, nodes...)

	yNode := nodes[1] // the follower that gets restarted
	if yNode.db.IsLeader() {
		yNode = nodes[2]
	}
	xNode := nodes[2] // the follower that stays up
	if xNode == yNode {
		xNode = nodes[1]
	}

	if err := yNode.db.Close(); err != nil {
		t.Fatalf("chaos: close follower %q: %v", yNode.cfg.NodeID, err)
	}

	writeKeys("snap/p2/", 200)
	chaosWaitConverged(t, 60*time.Second, "snap/", expected, nodes[0], xNode)

	chaosReopen(t, yNode)
	if err := yNode.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: restarted follower %q: %v", yNode.cfg.NodeID, err)
	}
	chaosWaitConverged(t, chaosConvergeTO, "snap/", expected, nodes...)
	t.Log("phase 1: restarted follower caught up via log replication")

	// ------------- phase 2: force InstallSnapshot -> FSM Restore ---------
	yAppliedBefore, _ := strconv.ParseUint(yNode.db.Stats()["honeybadger_applied_index"], 10, 64)
	if err := yNode.db.Close(); err != nil {
		t.Fatalf("chaos: second close of follower %q: %v", yNode.cfg.NodeID, err)
	}

	// Commit 12000 fresh entries while the follower is down. After the next
	// snapshot + compaction the leader's oldest retained log is at most
	// lastLog-10240, far beyond the follower's applied index (~400), so
	// ordinary AppendEntries can no longer bring it up to date.
	const extraWriters, extraPerWriter = 24, 500
	writeErrs := make([]error, extraWriters)
	var wg sync.WaitGroup
	for w := 0; w < extraWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < extraPerWriter; i++ {
				n := w*extraPerWriter + i
				k := fmt.Sprintf("snap/p3/k%05d", n)
				v := fmt.Sprintf("snap/p3/val-%05d", n)
				if err := chaosSetOnAnyLeader([]*chaosNode{nodes[0], xNode}, []byte(k), []byte(v)); err != nil && writeErrs[w] == nil {
					writeErrs[w] = err
				}
			}
		}(w)
	}
	wg.Wait()
	for w, err := range writeErrs {
		if err != nil {
			t.Fatalf("chaos: phase-2 writer %d failed: %v", w, err)
		}
	}
	for w := 0; w < extraWriters; w++ {
		for i := 0; i < extraPerWriter; i++ {
			n := w*extraPerWriter + i
			expected[fmt.Sprintf("snap/p3/k%05d", n)] = fmt.Sprintf("snap/p3/val-%05d", n)
		}
	}

	// Wait for the leader's periodic snapshot check to take a snapshot of
	// the write burst. The check fires every 120-240s of wall clock (raft
	// default SnapshotInterval), hence the long timeout. Any snapshot index
	// past ~2500 suffices: the leader retains the last 10240 logs after
	// compaction, its lastLog is ~12400 here, so its oldest remaining log
	// (~2160) is far beyond the follower's applied index (~400) and
	// ordinary AppendEntries can no longer catch the follower up.
	ldr := chaosLeader(t, 30*time.Second, nodes[0], xNode)
	var leaderSnaps map[string]uint64
	var leaderSnapIdx uint64
	chaosWaitFor(t, 280*time.Second,
		"leader to snapshot the write burst (raft snapshot checks tick every 120-240s)", func() bool {
			snapIdx, _ := strconv.ParseUint(ldr.db.Stats()["last_snapshot_index"], 10, 64)
			if snapIdx < 2500 {
				return false
			}
			leaderSnaps = chaosSnapshotIDs(ldr.cfg.DataDir)
			if len(leaderSnaps) == 0 {
				return false
			}
			leaderSnapIdx = snapIdx
			return true
		})

	// Establish that ordinary log replication really cannot catch the
	// follower up: after compaction the leader's oldest log is at least
	// lastLog-TrailingLogs (TrailingLogs is raft's hardcoded 10240), which
	// must lie beyond the follower's applied index when it was stopped.
	leaderLastLog, _ := strconv.ParseUint(ldr.db.Stats()["last_log_index"], 10, 64)
	if leaderLastLog <= 10240+yAppliedBefore {
		t.Fatalf("chaos: test setup failed to force the snapshot path: leader lastLog=%d, follower applied=%d (need lastLog > 10240+%d)",
			leaderLastLog, yAppliedBefore, yAppliedBefore)
	}

	// The two survivors must hold the full state before the follower returns.
	chaosWaitConverged(t, 60*time.Second, "snap/", expected, nodes[0], xNode)

	// Restart the follower: only InstallSnapshot can catch it up now.
	chaosReopen(t, yNode)
	if err := yNode.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: follower %q after snapshot restart: %v", yNode.cfg.NodeID, err)
	}
	chaosWaitConverged(t, 150*time.Second, "snap/", expected, nodes...)

	// Prove the InstallSnapshot -> FSM Restore path was actually taken: the
	// follower's snapshot store must now hold a snapshot at the leader's
	// snapshot index. (Raft stores a received snapshot under a fresh local
	// ID, so term+index are compared rather than the full ID.) The follower
	// cannot have taken that snapshot itself: its own snapshot check only
	// starts ticking 120-240s after its restart, seconds ago. Combined with
	// the compaction precondition above, receiving the leader's snapshot
	// via InstallSnapshot is the only way it could hold one.
	ySnaps := chaosSnapshotIDs(yNode.cfg.DataDir)
	found := ""
	for id, idx := range ySnaps {
		if idx == leaderSnapIdx {
			found = id
			break
		}
	}
	if found == "" {
		t.Fatalf("chaos: follower converged without receiving the leader's snapshot "+
			"(leader snapshots=%v, follower snapshots=%v): InstallSnapshot/Restore path not exercised",
			leaderSnaps, ySnaps)
	}
	t.Logf("phase 2: follower restored from leader snapshot index %d (local snapshot %s) and converged on %d keys",
		leaderSnapIdx, found, len(expected))
}

// TestChaosMixedOpStorm interleaves Set/Delete/Batch/SetWithTTL operations
// on the leader while readers hammer the followers, then compares the full
// keyspace of every node against an in-test model of the expected state.
// TTLs are 120s so no key can expire before the comparison completes.
func TestChaosMixedOpStorm(t *testing.T) {
	nodes := chaosCluster(t, "mixed", 0)

	rng := rand.New(rand.NewSource(20260815))
	const keyspace, numOps = 150, 600
	keyFn := func(i int) string { return fmt.Sprintf("mixed/k%03d", i) }
	model := make(map[string]string, keyspace)

	// Concurrent readers on the followers for the duration of the storm:
	// Get must only ever return a value or ErrKeyNotFound.
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
					_, err := n.db.Get([]byte(keyFn(rr.Intn(keyspace))))
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
				return db.Set([]byte(k), []byte(v))
			})
			model[k] = v
		case roll < 0.65: // Delete (possibly of a missing key: not an error)
			k := keyFn(rng.Intn(keyspace))
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Delete([]byte(k))
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
			sets := make([]Pair, 0, nSets)
			for i := 0; i < nSets; i++ {
				k := keyFn(pick())
				v := fmt.Sprintf("mixed-val-op%04d-%d", op, i)
				sets = append(sets, Pair{Key: []byte(k), Value: []byte(v)})
			}
			dels := make([][]byte, 0, nDels)
			for i := 0; i < nDels; i++ {
				dels = append(dels, []byte(keyFn(pick())))
			}
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Batch(sets, dels)
			})
			// Mirror the FSM's apply order: sets first, then deletes.
			for _, p := range sets {
				model[string(p.Key)] = string(p.Value)
			}
			for _, d := range dels {
				delete(model, string(d))
			}
		default: // SetWithTTL, long enough to outlive the comparison
			k := keyFn(rng.Intn(keyspace))
			v := fmt.Sprintf("mixed-val-op%04d-ttl", op)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.SetWithTTL([]byte(k), []byte(v), 120*time.Second)
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
