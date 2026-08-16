package honeybadger

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// freePort returns a currently-unused TCP port on 127.0.0.1.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// testNode opens a node on 127.0.0.1:port with a fresh temp dir and
// registers its cleanup with t. newCluster must be true for exactly the
// first node of a new cluster; plain Open never bootstraps.
func testNode(t *testing.T, port int, newCluster bool) *DB {
	t.Helper()
	cfg := Config{
		NodeID:   fmt.Sprintf("node-%d", port),
		RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:  t.TempDir(),
	}
	var db *DB
	var err error
	if newCluster {
		db, err = Open(cfg, NewCluster())
	} else {
		db, err = Open(cfg)
	}
	if err != nil {
		t.Fatalf("open node on port %d: %v", port, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
}

// leaderOf returns the leader among nodes, or nil if none is found.
func leaderOf(nodes ...*DB) *DB {
	for _, db := range nodes {
		if st, err := db.Status(); err == nil && st.State == StateLeader {
			return db
		}
	}
	return nil
}

// getLocal reads key from this node's local store (eventual consistency):
// the read pattern follower tests use while waiting for convergence.
func getLocal(db *DB, key string) (string, error) {
	return db.GetWithOptions(key, ReadOptions{Mode: ReadLocal})
}

func TestSingleNode(t *testing.T) {
	db := testNode(t, freePort(t), true)
	// NewCluster waited for the first election: the node is immediately
	// the leader, with no separate WaitForLeader needed.
	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != StateLeader {
		t.Fatalf("State = %s, want Leader", st.State)
	}
	if st.Leader == nil || st.Leader.ID == "" || st.Leader.RaftAddr == "" {
		t.Fatalf("Status.Leader = %+v, want set", st.Leader)
	}

	// Set / Get.
	if err := db.Set("foo", "bar"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, err := db.Get("foo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "bar" {
		t.Fatalf("Get(foo) = %q, want %q", val, "bar")
	}
	// Overwrite.
	if err := db.Set("foo", "baz"); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	val, err = db.Get("foo")
	if err != nil || val != "baz" {
		t.Fatalf("Get(foo) after overwrite = %q, %v; want baz", val, err)
	}

	// Get on a missing key.
	if _, err := db.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrKeyNotFound", err)
	}

	// Delete.
	if err := db.Delete("foo"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get("foo"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(foo) after Delete = %v, want ErrKeyNotFound", err)
	}
	// Deleting a missing key is not an error.
	if err := db.Delete("never-existed"); err != nil {
		t.Fatalf("Delete missing key: %v", err)
	}

	// Batch.
	err = db.Batch(
		SetOp("a/1", "v1"),
		SetOp("a/2", "v2"),
		SetOp("b/1", "v3"),
		DeleteOp("foo"),
	)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	// ScanPrefixBytes.
	entries, err := db.ScanPrefixBytes([]byte("a/"), ScanOptions{Unlimited: true})
	if err != nil {
		t.Fatalf("ScanPrefixBytes: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ScanPrefixBytes(a/) returned %d entries, want 2", len(entries))
	}
	if string(entries[0].Key) != "a/1" || string(entries[0].Value) != "v1" ||
		string(entries[1].Key) != "a/2" || string(entries[1].Value) != "v2" {
		t.Fatalf("ScanPrefixBytes(a/) = %+v, want a/1=v1, a/2=v2 in order", entries)
	}
	entries, err = db.ScanPrefixBytes([]byte("a/"), ScanOptions{Limit: 1})
	if err != nil || len(entries) != 1 {
		t.Fatalf("ScanPrefixBytes with limit 1 = %v, %v; want exactly 1 entry", entries, err)
	}

	// ViewBadger escape hatch (linearizable on the leader).
	err = db.ViewBadger(ReadOptions{}, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("b/1"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if string(v) != "v3" {
				return fmt.Errorf("ViewBadger read b/1 = %q, want v3", v)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("ViewBadger: %v", err)
	}

	// Barrier on the leader.
	if err := db.Barrier(5 * time.Second); err != nil {
		t.Fatalf("Barrier: %v", err)
	}

	// RawRaftStats contains raft stats and the applied index.
	stats := db.RawRaftStats()
	if stats["state"] != "Leader" {
		t.Fatalf("RawRaftStats()[state] = %q, want Leader", stats["state"])
	}
	if stats["honeybadger_applied_index"] == "" || stats["honeybadger_applied_index"] == "0" {
		t.Fatalf("RawRaftStats() missing applied index: %v", stats["honeybadger_applied_index"])
	}
}

func TestSetWithTTL(t *testing.T) {
	db := testNode(t, freePort(t), true)

	// Badger stores expirations with second granularity, so a 2s TTL lives
	// at least ~1s and at most 2s.
	if err := db.Set("ttl-key", "ttl-val", WithTTL(2*time.Second)); err != nil {
		t.Fatalf("Set WithTTL: %v", err)
	}
	val, err := db.Get("ttl-key")
	if err != nil || val != "ttl-val" {
		t.Fatalf("Get(ttl-key) = %q, %v; want ttl-val", val, err)
	}

	time.Sleep(3 * time.Second)
	if _, err := db.Get("ttl-key"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(ttl-key) after TTL expiry = %v, want ErrKeyNotFound", err)
	}
}

func TestThreeNodeCluster(t *testing.T) {
	port1, port2, port3 := freePort(t), freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)
	node3 := testNode(t, port3, false)
	nodes := []*DB{node1, node2, node3}

	// Join the two followers through the leader (node1 is already elected:
	// NewCluster waited for the first election).
	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port3), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port3)}); err != nil {
		t.Fatalf("AddVoter node3: %v", err)
	}
	for i, db := range nodes {
		leader, err := db.WaitForLeader(15 * time.Second)
		if err != nil {
			t.Fatalf("node%d WaitForLeader: %v", i+1, err)
		}
		if leader.ID != fmt.Sprintf("node-%d", port1) {
			t.Fatalf("node%d sees leader %q, want node-%d", i+1, leader.ID, port1)
		}
	}

	// Write 50 keys on the leader and wait for every follower to converge.
	const numKeys = 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := fmt.Sprintf("val-%03d", i)
		if err := node1.Set(key, val); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	for ni, db := range nodes {
		db := db
		waitFor(t, 15*time.Second, func() bool {
			for i := 0; i < numKeys; i++ {
				val, err := getLocal(db, fmt.Sprintf("key-%03d", i))
				if err != nil || val != fmt.Sprintf("val-%03d", i) {
					return false
				}
			}
			return true
		}, fmt.Sprintf("node%d to replicate all %d keys", ni+1, numKeys))
	}

	// Delete on the leader; followers must observe the deletion.
	if err := node1.Delete("key-007"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for ni, db := range nodes {
		db := db
		waitFor(t, 10*time.Second, func() bool {
			_, err := getLocal(db, "key-007")
			return errors.Is(err, ErrKeyNotFound)
		}, fmt.Sprintf("node%d to see key-007 deleted", ni+1))
	}

	// Atomic batch: sets and deletes in one log entry, one with a TTL.
	err := node1.Batch(
		SetOp("batch/1", "b1"),
		SetOp("batch/2", "b2"),
		SetOp("batch/ttl", "b3", WithTTL(time.Hour)),
		DeleteOp("key-001"),
		DeleteOp("key-002"),
	)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	for ni, db := range nodes {
		db := db
		waitFor(t, 10*time.Second, func() bool {
			v1, e1 := getLocal(db, "batch/1")
			v2, e2 := getLocal(db, "batch/2")
			v3, e3 := getLocal(db, "batch/ttl")
			if e1 != nil || e2 != nil || e3 != nil ||
				v1 != "b1" || v2 != "b2" || v3 != "b3" {
				return false
			}
			_, d1 := getLocal(db, "key-001")
			_, d2 := getLocal(db, "key-002")
			return errors.Is(d1, ErrKeyNotFound) && errors.Is(d2, ErrKeyNotFound)
		}, fmt.Sprintf("node%d to apply batch atomically", ni+1))
	}
}

func TestNotLeader(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if _, err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	follower := node2
	if st, _ := node2.Status(); st.State == StateLeader {
		follower = node1 // extremely unlikely, but stay correct either way
	}

	if err := follower.Set("k", "v"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Set on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Set("k", "v", WithTTL(time.Minute)); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Set WithTTL on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Delete("k"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Delete on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Batch(SetOp("k", "v")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Batch on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.AddVoter(Node{ID: "node-x", RaftAddr: "127.0.0.1:1"}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("AddVoter on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.RemoveNode("node-x"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("RemoveNode on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Barrier(time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Barrier on follower = %v, want ErrNotLeader", err)
	}
}

// TestGetStrict verifies the Tier-1 read contract: Get is linearizable and
// leader-only, and fails loudly with a typed *NotLeaderError on followers
// instead of silently serving stale data.
func TestGetStrict(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if _, err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	leader, follower := node1, node2
	if st, _ := node2.Status(); st.State == StateLeader {
		leader, follower = node2, node1
	}

	if err := leader.Set("strict", "yes"); err != nil {
		t.Fatalf("Set on leader: %v", err)
	}

	// Follower: strictly not allowed, with the typed error.
	_, err := follower.Get("strict")
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Get on follower = %v, want ErrNotLeader", err)
	}
	var nlErr *NotLeaderError
	if !errors.As(err, &nlErr) {
		t.Fatalf("Get on follower error %T is not *NotLeaderError", err)
	}
	if nlErr.LeaderAddr == "" || nlErr.LeaderID == "" {
		t.Fatalf("NotLeaderError should carry leader id+addr, got %+v", nlErr)
	}

	// Leader: barrier + read.
	val, err := leader.Get("strict")
	if err != nil || val != "yes" {
		t.Fatalf("Get on leader = %q, %v; want yes", val, err)
	}
	if _, err := leader.Get("nope"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(nope) on leader = %v, want ErrKeyNotFound", err)
	}
}

// TestReadLocalOnFollower verifies the explicit opt-in to eventually
// consistent reads works on followers, uniformly across the read APIs.
func TestReadLocalOnFollower(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if _, err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	leader, follower := node1, node2
	if st, _ := node2.Status(); st.State == StateLeader {
		leader, follower = node2, node1
	}

	if err := leader.Set("local", "yes"); err != nil {
		t.Fatalf("Set on leader: %v", err)
	}
	local := ReadOptions{Mode: ReadLocal}

	// The default (zero) ReadOptions is linearizable: every read API must
	// refuse it on the follower.
	if _, err := follower.GetWithOptions("local", ReadOptions{}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("GetWithOptions zero-value on follower = %v, want ErrNotLeader", err)
	}
	if _, err := follower.GetBytes([]byte("local"), ReadOptions{}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("GetBytes zero-value on follower = %v, want ErrNotLeader", err)
	}
	if _, err := follower.ScanPrefixBytes([]byte("local"), ScanOptions{}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ScanPrefixBytes zero-value on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.ViewBadger(ReadOptions{}, func(*badger.Txn) error { return nil }); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ViewBadger zero-value on follower = %v, want ErrNotLeader", err)
	}

	// ReadLocal serves replicated data on the follower once it converges.
	waitFor(t, 10*time.Second, func() bool {
		val, err := follower.GetWithOptions("local", local)
		return err == nil && val == "yes"
	}, "follower local read to observe the leader write")

	val, err := follower.GetBytes([]byte("local"), local)
	if err != nil || string(val) != "yes" {
		t.Fatalf("GetBytes local on follower = %q, %v; want yes", val, err)
	}
	entries, err := follower.ScanPrefixBytes([]byte("local"), ScanOptions{Read: local})
	if err != nil || len(entries) != 1 || string(entries[0].Value) != "yes" {
		t.Fatalf("ScanPrefixBytes local on follower = %v, %v; want one yes", entries, err)
	}
	err = follower.ViewBadger(local, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("local"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if string(v) != "yes" {
				return fmt.Errorf("ViewBadger local = %q, want yes", v)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("ViewBadger local on follower: %v", err)
	}
}

func TestRestart(t *testing.T) {
	port := freePort(t)
	dir := t.TempDir()
	cfg := Config{
		NodeID:   fmt.Sprintf("node-%d", port),
		RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:  dir,
	}

	db, err := Open(cfg, NewCluster())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := db.Set(fmt.Sprintf("persist-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close must be idempotent.
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Re-opening an already-bootstrapped DataDir with NewCluster is
	// tolerated (the stale bootstrap attempt is ignored) and still waits
	// for leadership.
	db2, err := Open(cfg, NewCluster())
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	for i := 0; i < 10; i++ {
		val, err := db2.Get(fmt.Sprintf("persist-%d", i))
		if err != nil || val != fmt.Sprintf("value-%d", i) {
			t.Fatalf("Get(persist-%d) after restart = %q, %v", i, val, err)
		}
	}
}

// TestSnapshotRestore forces a snapshot on the leader, compacts the log,
// and restarts a lagging follower, which must catch up via snapshot
// install (exercising fsm.Restore) and converge to the same state.
func TestSnapshotRestore(t *testing.T) {
	port1, port2, port3 := freePort(t), freePort(t), freePort(t)

	open := func(port int, newCluster bool, dir string) *DB {
		cfg := Config{
			NodeID:   fmt.Sprintf("node-%d", port),
			RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
			DataDir:  dir,
			Advanced: AdvancedConfig{SnapshotThreshold: 16},
		}
		var db *DB
		var err error
		if newCluster {
			db, err = Open(cfg, NewCluster())
		} else {
			db, err = Open(cfg)
		}
		if err != nil {
			t.Fatalf("open node-%d: %v", port, err)
		}
		return db
	}

	node1 := open(port1, true, t.TempDir())
	t.Cleanup(func() { node1.Close() })
	node2 := open(port2, false, t.TempDir())
	t.Cleanup(func() { node2.Close() })
	dir3 := t.TempDir()
	node3 := open(port3, false, dir3)

	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port3), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port3)}); err != nil {
		t.Fatalf("AddVoter node3: %v", err)
	}
	if _, err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}
	if _, err := node3.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node3 WaitForLeader: %v", err)
	}

	// Give every node some initial state, then take node3 down.
	for i := 0; i < 5; i++ {
		if err := node1.Set(fmt.Sprintf("snap-%03d", i), fmt.Sprintf("v-%03d", i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := getLocal(node3, "snap-004")
		return err == nil
	}, "node3 to receive initial keys")
	if err := node3.Close(); err != nil {
		t.Fatalf("node3 Close: %v", err)
	}

	// Write enough keys that the leader's snapshot (plus trailing log
	// retention of SnapshotThreshold entries) leaves node3 behind the
	// leader's first retained log entry.
	const total = 100
	for i := 5; i < total; i++ {
		if err := node1.Set(fmt.Sprintf("snap-%03d", i), fmt.Sprintf("v-%03d", i)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	// Force a snapshot; when Snapshot returns, log compaction has finished.
	if err := node1.Snapshot(); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restart node3. The leader can no longer feed it the missing entries
	// from the log and must install the snapshot instead.
	node3 = open(port3, false, dir3)
	t.Cleanup(func() { node3.Close() })

	waitFor(t, 30*time.Second, func() bool {
		for i := 0; i < total; i++ {
			val, err := getLocal(node3, fmt.Sprintf("snap-%03d", i))
			if err != nil || val != fmt.Sprintf("v-%03d", i) {
				return false
			}
		}
		return true
	}, "node3 to converge after snapshot restore")

	// The cluster must keep working after the restore.
	if err := node1.Set("post-restore", "ok"); err != nil {
		t.Fatalf("Set after restore: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		val, err := getLocal(node3, "post-restore")
		return err == nil && val == "ok"
	}, "node3 to receive post-restore writes")
}

// TestTTLNoResurrectAfterRestart is the regression test for TTL keys
// resurrecting when Raft replays log entries after a restart: the command
// replicates an absolute expiry stamped once at write submission, so
// re-apply is idempotent.
func TestTTLNoResurrectAfterRestart(t *testing.T) {
	port := freePort(t)
	dir := t.TempDir()
	cfg := Config{
		NodeID:   fmt.Sprintf("node-%d", port),
		RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:  dir,
	}

	db, err := Open(cfg, NewCluster())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Set("durable", "stays"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set("ttl", "goes", WithTTL(2*time.Second)); err != nil {
		t.Fatalf("Set WithTTL: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := getLocal(db, "ttl")
		return errors.Is(err, ErrKeyNotFound)
	}, "ttl key to expire")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open: Raft replays the log through the FSM. The replayed TTL
	// command carries its original (now past) absolute expiry, so the key
	// must stay gone — before, during, and after the replay.
	db2, err := Open(cfg, NewCluster())
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	for i := 0; i < 20; i++ {
		if _, err := db2.Get("ttl"); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("ttl key resurrected after restart (check %d): %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if val, err := db2.Get("durable"); err != nil || val != "stays" {
		t.Fatalf("durable key after restart = %q, %v; want stays", val, err)
	}
}

// TestNotLeaderErrorTyped verifies the typed not-leader error: errors.As
// yields a *NotLeaderError with the leader's ID and address, and errors.Is
// against ErrNotLeader keeps working.
func TestNotLeaderErrorTyped(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if _, err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	follower := node2
	if st, _ := node2.Status(); st.State == StateLeader {
		follower = node1
	}

	err := follower.Set("k", "v")
	var nlErr *NotLeaderError
	if !errors.As(err, &nlErr) {
		t.Fatalf("Set on follower error %T (%v) is not a *NotLeaderError", err, err)
	}
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("errors.Is(%v, ErrNotLeader) = false", err)
	}
	if nlErr.LeaderAddr == "" {
		t.Fatal("NotLeaderError.LeaderAddr empty; follower should know the leader")
	}
	if nlErr.LeaderID == "" {
		t.Fatal("NotLeaderError.LeaderID empty; follower should know the leader")
	}
	leader, err := follower.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if nlErr.LeaderID != leader.ID || nlErr.LeaderAddr != leader.RaftAddr {
		t.Fatalf("NotLeaderError = (%q, %q), WaitForLeader = (%q, %q)",
			nlErr.LeaderID, nlErr.LeaderAddr, leader.ID, leader.RaftAddr)
	}
}

// TestBytesAPI exercises the explicit byte-slice API surface.
func TestBytesAPI(t *testing.T) {
	db := testNode(t, freePort(t), true)

	// The zero ReadOptions is linearizable; on the leader it just works.
	if err := db.SetBytes([]byte("bkey"), []byte("bval")); err != nil {
		t.Fatalf("SetBytes: %v", err)
	}
	val, err := db.GetBytes([]byte("bkey"), ReadOptions{})
	if err != nil || string(val) != "bval" {
		t.Fatalf("GetBytes(bkey) = %q, %v; want bval", val, err)
	}
	// The returned slice must be a copy that stays valid.
	if err := db.SetBytes([]byte("bkey"), []byte("zzzz")); err != nil {
		t.Fatalf("SetBytes overwrite: %v", err)
	}
	if string(val) != "bval" {
		t.Fatalf("returned slice mutated by overwrite: %q", val)
	}
	if _, err := db.GetBytes([]byte("missing"), ReadOptions{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetBytes(missing) = %v, want ErrKeyNotFound", err)
	}
	if err := db.DeleteBytes([]byte("bkey")); err != nil {
		t.Fatalf("DeleteBytes: %v", err)
	}
	if _, err := db.GetBytes([]byte("bkey"), ReadOptions{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetBytes(bkey) after DeleteBytes = %v, want ErrKeyNotFound", err)
	}

	// Byte mutations in a batch, with a TTL on one.
	err = db.Batch(
		SetBytesOp([]byte("bb/1"), []byte("1")),
		SetBytesOp([]byte("bb/2"), []byte("2"), WithTTL(time.Hour)),
	)
	if err != nil {
		t.Fatalf("Batch bytes: %v", err)
	}
	if err := db.Batch(DeleteBytesOp([]byte("bb/1"))); err != nil {
		t.Fatalf("Batch delete bytes: %v", err)
	}
	if _, err := db.GetBytes([]byte("bb/1"), ReadOptions{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetBytes(bb/1) after batch delete = %v, want ErrKeyNotFound", err)
	}
	if val, err := db.GetBytes([]byte("bb/2"), ReadOptions{}); err != nil || string(val) != "2" {
		t.Fatalf("GetBytes(bb/2) = %q, %v; want 2", val, err)
	}
}

// TestMembers verifies cluster-membership introspection on leaders and
// followers alike, with typed roles.
func TestMembers(t *testing.T) {
	port1, port2, port3 := freePort(t), freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)
	node3 := testNode(t, port3, false)

	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port2), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port2)}); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}
	if err := node1.AddVoter(Node{ID: fmt.Sprintf("node-%d", port3), RaftAddr: fmt.Sprintf("127.0.0.1:%d", port3)}); err != nil {
		t.Fatalf("AddVoter node3: %v", err)
	}
	if _, err := node3.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node3 WaitForLeader: %v", err)
	}

	wantIDs := map[string]string{}
	for _, port := range []int{port1, port2, port3} {
		wantIDs[fmt.Sprintf("node-%d", port)] = fmt.Sprintf("127.0.0.1:%d", port)
	}
	// Leaders and followers must both answer, eventually agreeing.
	for ni, db := range []*DB{node1, node2, node3} {
		db := db
		waitFor(t, 10*time.Second, func() bool {
			members, err := db.Members()
			if err != nil || len(members) != 3 {
				return false
			}
			for _, m := range members {
				addr, ok := wantIDs[m.ID]
				if !ok || addr != m.RaftAddr || m.Role != RoleVoter {
					return false
				}
			}
			return true
		}, fmt.Sprintf("node%d Members() to show all 3 voters", ni+1))
	}

	if RoleVoter.String() != "Voter" || RoleNonvoter.String() != "Nonvoter" ||
		RoleStaging.String() != "Staging" || RoleNone.String() != "None" {
		t.Fatalf("NodeRole.String wrong: %s %s %s %s", RoleVoter, RoleNonvoter, RoleStaging, RoleNone)
	}
	if got := NodeRole(9).String(); got != "NodeRole(9)" {
		t.Fatalf("NodeRole(9).String() = %q, want honest unknown rendering", got)
	}
}

// TestStatus verifies the typed status snapshot.
func TestStatus(t *testing.T) {
	port := freePort(t)
	db := testNode(t, port, true)

	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Local.ID != fmt.Sprintf("node-%d", port) {
		t.Fatalf("Status.Local.ID = %q", st.Local.ID)
	}
	if st.Local.RaftAddr != fmt.Sprintf("127.0.0.1:%d", port) {
		t.Fatalf("Status.Local.RaftAddr = %q", st.Local.RaftAddr)
	}
	if st.Local.Role != RoleVoter {
		t.Fatalf("Status.Local.Role = %s, want Voter", st.Local.Role)
	}
	if st.State != StateLeader || st.State.String() != "Leader" {
		t.Fatalf("Status.State = %s (%q), want Leader", st.State, st.State)
	}
	if st.Leader == nil || st.Leader.ID != st.Local.ID || st.Leader.RaftAddr != st.Local.RaftAddr {
		t.Fatalf("Status.Leader = %+v, want the local node", st.Leader)
	}

	// AppliedIndex advances as writes are committed.
	before := st.AppliedIndex
	const writes = 5
	for i := 0; i < writes; i++ {
		if err := db.Set(fmt.Sprintf("ai-%d", i), "v"); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	st, err = db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.AppliedIndex < before+writes {
		t.Fatalf("AppliedIndex = %d before, %d after %d writes", before, st.AppliedIndex, writes)
	}
	stats := db.RawRaftStats()
	if stats["honeybadger_applied_index"] != fmt.Sprintf("%d", st.AppliedIndex) {
		t.Fatalf("RawRaftStats()[honeybadger_applied_index] = %q, want %d",
			stats["honeybadger_applied_index"], st.AppliedIndex)
	}

	if StateFollower.String() != "Follower" || StateCandidate.String() != "Candidate" ||
		StateShutdown.String() != "Shutdown" {
		t.Fatalf("State.String wrong: %s %s %s", StateFollower, StateCandidate, StateShutdown)
	}
	if got := State(7).String(); got != "State(7)" {
		t.Fatalf("State(7).String() = %q, want honest unknown rendering", got)
	}
	if got := ReadMode(42).String(); got != "ReadMode(42)" {
		t.Fatalf("ReadMode(42).String() = %q, want honest unknown rendering", got)
	}
}

// TestValidation verifies that invalid arguments are rejected with
// ErrInvalidArgument BEFORE any Raft entry is submitted.
func TestValidation(t *testing.T) {
	db := testNode(t, freePort(t), true)

	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	appliedBefore := st.AppliedIndex

	cases := map[string]func() error{
		"empty key Set":         func() error { return db.Set("", "v") },
		"empty key SetBytes":    func() error { return db.SetBytes(nil, []byte("v")) },
		"empty key Delete":      func() error { return db.Delete("") },
		"empty key DeleteBytes": func() error { return db.DeleteBytes([]byte{}) },
		"empty key Get":         func() error { _, err := db.Get(""); return err },
		"empty key GetBytes":    func() error { _, err := db.GetBytes(nil, ReadOptions{}); return err },
		"zero TTL":              func() error { return db.Set("k", "v", WithTTL(0)) },
		"negative TTL":          func() error { return db.Set("k", "v", WithTTL(-time.Second)) },
		"unknown set option": func() error {
			return db.Set("k", "v", nil)
		},
		"batch empty key":   func() error { return db.Batch(SetOp("", "v")) },
		"batch zero TTL":    func() error { return db.Batch(SetOp("k", "v", WithTTL(0))) },
		"batch dup set/set": func() error { return db.Batch(SetOp("dup", "1"), SetOp("dup", "2")) },
		"batch dup set/del": func() error { return db.Batch(SetOp("dup", "1"), DeleteOp("dup")) },
		"batch dup del/del": func() error { return db.Batch(DeleteOp("dup"), DeleteBytesOp([]byte("dup"))) },
		"batch zero mutation": func() error {
			return db.Batch(Mutation{})
		},
		"negative scan limit": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Limit: -1})
			return err
		},
		"unknown read mode Get": func() error {
			_, err := db.GetWithOptions("k", ReadOptions{Mode: ReadMode(42)})
			return err
		},
		"unknown read mode scan": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Read: ReadOptions{Mode: ReadMode(42)}})
			return err
		},
		"nil ViewBadger callback": func() error { return db.ViewBadger(ReadOptions{}, nil) },
		"AddVoter empty ID":       func() error { return db.AddVoter(Node{RaftAddr: "127.0.0.1:1"}) },
		"AddVoter empty addr":     func() error { return db.AddVoter(Node{ID: "x"}) },
		"AddVoter contradictory role": func() error {
			return db.AddVoter(Node{ID: "x", RaftAddr: "127.0.0.1:1", Role: RoleNonvoter})
		},
		"RemoveNode empty ID": func() error { return db.RemoveNode("") },
		"negative read timeout (linearizable)": func() error {
			_, err := db.GetWithOptions("k", ReadOptions{Timeout: -time.Second})
			return err
		},
		"negative read timeout (local scan)": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Read: ReadOptions{Mode: ReadLocal, Timeout: -time.Second}})
			return err
		},
	}
	for name, fn := range cases {
		if err := fn(); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s = %v, want ErrInvalidArgument", name, err)
		}
	}

	// An empty batch is a documented no-op, not an error.
	if err := db.Batch(); err != nil {
		t.Fatalf("empty Batch() = %v, want nil", err)
	}

	// None of the rejections above may have submitted a Raft entry.
	st, err = db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.AppliedIndex != appliedBefore {
		t.Fatalf("validation failures submitted raft entries: AppliedIndex %d -> %d",
			appliedBefore, st.AppliedIndex)
	}
}

// TestScanLimits verifies the scan limit rules: zero means the default of
// 100, negatives are invalid, Unlimited must be explicit, and truncation
// happens in key order.
func TestScanLimits(t *testing.T) {
	db := testNode(t, freePort(t), true)

	const total = 150
	sets := make([]Mutation, 0, total)
	for i := 0; i < total; i++ {
		sets = append(sets, SetOp(fmt.Sprintf("scan/%03d", i), fmt.Sprintf("v%03d", i)))
	}
	if err := db.Batch(sets...); err != nil {
		t.Fatalf("Batch seed: %v", err)
	}

	// Zero limit => default of 100, truncated in key order.
	entries, err := db.ScanPrefixBytes([]byte("scan/"), ScanOptions{})
	if err != nil {
		t.Fatalf("ScanPrefixBytes default: %v", err)
	}
	if len(entries) != defaultScanLimit {
		t.Fatalf("default scan returned %d entries, want %d", len(entries), defaultScanLimit)
	}
	if string(entries[0].Key) != "scan/000" || string(entries[len(entries)-1].Key) != "scan/099" {
		t.Fatalf("default scan range %s..%s, want scan/000..scan/099",
			entries[0].Key, entries[len(entries)-1].Key)
	}

	// Explicit limit.
	entries, err = db.ScanPrefixBytes([]byte("scan/"), ScanOptions{Limit: 7})
	if err != nil || len(entries) != 7 || string(entries[6].Key) != "scan/006" {
		t.Fatalf("Limit 7 scan = %d entries (%v), want 7 ending at scan/006", len(entries), err)
	}

	// Negative limit is invalid.
	if _, err := db.ScanPrefixBytes([]byte("scan/"), ScanOptions{Limit: -5}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Limit -5 = %v, want ErrInvalidArgument", err)
	}

	// Unlimited must be explicit and returns everything.
	entries, err = db.ScanPrefixBytes([]byte("scan/"), ScanOptions{Unlimited: true})
	if err != nil || len(entries) != total {
		t.Fatalf("Unlimited scan = %d entries (%v), want %d", len(entries), err, total)
	}

	// Empty prefix scans the whole keyspace.
	entries, err = db.ScanPrefixBytes(nil, ScanOptions{Unlimited: true})
	if err != nil || len(entries) != total {
		t.Fatalf("empty-prefix scan = %d entries (%v), want %d", len(entries), err, total)
	}

	// Keys and values are copies.
	entries, err = db.ScanPrefixBytes([]byte("scan/000"), ScanOptions{Limit: 1})
	if err != nil || len(entries) != 1 {
		t.Fatalf("point scan = %v, %v", entries, err)
	}
	entries[0].Value[0] = 'X'
	if val, err := db.Get("scan/000"); err != nil || val != "v000" {
		t.Fatalf("stored value mutated via scan entry: %q, %v", val, err)
	}
}

// TestNewClusterWaitsForLeadership verifies that Open with NewCluster
// returns a node that has already completed its first election and is
// immediately writable.
func TestNewClusterWaitsForLeadership(t *testing.T) {
	db := testNode(t, freePort(t), true)

	// No WaitForLeader call here: Open already waited.
	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != StateLeader {
		t.Fatalf("State right after Open(NewCluster) = %s, want Leader", st.State)
	}
	if st.Leader == nil || st.Leader.ID != st.Local.ID {
		t.Fatalf("Leader right after Open(NewCluster) = %+v, want self", st.Leader)
	}
	if err := db.Set("immediate", "write"); err != nil {
		t.Fatalf("immediate Set after Open(NewCluster): %v", err)
	}
	if val, err := db.Get("immediate"); err != nil || val != "write" {
		t.Fatalf("immediate Get = %q, %v; want write", val, err)
	}
}

// TestOpenNeverBootstraps verifies that plain Open (no NewCluster) never
// forms a cluster: the node stays leaderless and refuses writes.
func TestOpenNeverBootstraps(t *testing.T) {
	db := testNode(t, freePort(t), false)

	// The node must not elect itself. Elections would complete within a
	// couple of seconds, so a short observation window suffices.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, err := db.Status()
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.State == StateLeader || st.Leader != nil {
			t.Fatalf("plain Open bootstrapped a cluster: %+v", st)
		}
		if st.Local.Role != RoleNone {
			t.Fatalf("un-bootstrapped node has role %s, want None", st.Local.Role)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// WaitForLeader times out with a matchable ErrNoLeader.
	if _, err := db.WaitForLeader(500 * time.Millisecond); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("WaitForLeader on un-bootstrapped node = %v, want ErrNoLeader", err)
	}

	// Writes fail fast with ErrNotLeader.
	if err := db.Set("k", "v"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Set on un-bootstrapped node = %v, want ErrNotLeader", err)
	}
}

// TestRaftAdvertise verifies the advertised-address override.
func TestRaftAdvertise(t *testing.T) {
	port := freePort(t)
	bind := fmt.Sprintf("127.0.0.1:%d", port)
	db, err := Open(Config{
		NodeID:   "adv-node",
		RaftBind: bind,
		DataDir:  t.TempDir(),
		Advanced: AdvancedConfig{RaftAdvertise: bind},
	}, NewCluster())
	if err != nil {
		t.Fatalf("Open with RaftAdvertise: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Local.RaftAddr != bind {
		t.Fatalf("advertised addr = %q, want %q", st.Local.RaftAddr, bind)
	}
	if err := db.Set("k", "v"); err != nil {
		t.Fatalf("Set through advertised address: %v", err)
	}

	// An unresolvable advertise address fails Open.
	_, err = Open(Config{
		NodeID:   "adv-bad",
		RaftBind: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		DataDir:  t.TempDir(),
		Advanced: AdvancedConfig{RaftAdvertise: "not a host:port"},
	})
	if err == nil || !strings.Contains(err.Error(), "RaftAdvertise") {
		t.Fatalf("Open with bad RaftAdvertise = %v, want resolve error", err)
	}
}

// TestClosedErrors verifies every public operation fails with ErrClosed
// after Close, that Close stays idempotent, and that RawRaftStats is the
// documented exception that remains callable.
func TestClosedErrors(t *testing.T) {
	port := freePort(t)
	db := testNode(t, port, true)
	if err := db.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	cases := map[string]func() error{
		"Set":            func() error { return db.Set("k", "v") },
		"SetBytes":       func() error { return db.SetBytes([]byte("k"), []byte("v")) },
		"Delete":         func() error { return db.Delete("k") },
		"DeleteBytes":    func() error { return db.DeleteBytes([]byte("k")) },
		"Batch":          func() error { return db.Batch(SetOp("k", "v")) },
		"Batch (empty)":  func() error { return db.Batch() },
		"AddVoter":       func() error { return db.AddVoter(Node{ID: "x", RaftAddr: "127.0.0.1:1"}) },
		"RemoveNode":     func() error { return db.RemoveNode("x") },
		"Barrier":        func() error { return db.Barrier(time.Second) },
		"Snapshot":       func() error { return db.Snapshot() },
		"Get":            func() error { _, err := db.Get("k"); return err },
		"GetWithOptions": func() error { _, err := db.GetWithOptions("k", ReadOptions{Mode: ReadLocal}); return err },
		"GetBytes":       func() error { _, err := db.GetBytes([]byte("k"), ReadOptions{Mode: ReadLocal}); return err },
		"ScanPrefixBytes": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Read: ReadOptions{Mode: ReadLocal}})
			return err
		},
		"ViewBadger": func() error {
			return db.ViewBadger(ReadOptions{Mode: ReadLocal}, func(*badger.Txn) error { return nil })
		},
		"Members":       func() error { _, err := db.Members(); return err },
		"Status":        func() error { _, err := db.Status(); return err },
		"WaitForLeader": func() error { _, err := db.WaitForLeader(time.Second); return err },
	}
	for name, fn := range cases {
		if err := fn(); !errors.Is(err, ErrClosed) {
			t.Fatalf("%s after Close = %v, want ErrClosed", name, err)
		}
	}

	// RawRaftStats is the documented passive exception: it keeps returning
	// the final statistics, including the shutdown state.
	stats := db.RawRaftStats()
	if stats["state"] != "Shutdown" {
		t.Fatalf("RawRaftStats()[state] after Close = %q, want Shutdown", stats["state"])
	}
}

// TestOpenValidation verifies Open-level option validation.
func TestOpenValidation(t *testing.T) {
	port := freePort(t)
	cfg := func() Config {
		return Config{
			NodeID:   fmt.Sprintf("node-%d", port),
			RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
			DataDir:  t.TempDir(),
		}
	}

	// Unknown (nil) option.
	if _, err := Open(cfg(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open(nil option) = %v, want ErrInvalidArgument", err)
	}
	// Duplicate NewCluster.
	if _, err := Open(cfg(), NewCluster(), NewCluster()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open(NewCluster, NewCluster) = %v, want ErrInvalidArgument", err)
	}
}

// TestNewClusterElectionTimeout verifies that when the first election does
// not complete in time, Open fails with an error matching ErrNoLeader and
// leaves no half-open node behind: the same directory can be opened
// afterwards. The wait is shrunk to ~zero to make the path deterministic.
func TestNewClusterElectionTimeout(t *testing.T) {
	old := newClusterLeaderTimeout
	newClusterLeaderTimeout = time.Nanosecond
	defer func() { newClusterLeaderTimeout = old }()

	cfg := Config{
		NodeID:   "election-timeout",
		RaftBind: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		DataDir:  t.TempDir(),
	}
	db, err := Open(cfg, NewCluster())
	if err == nil {
		db.Close()
		t.Fatal("Open with ~zero election wait succeeded, want ErrNoLeader")
	}
	if !errors.Is(err, ErrNoLeader) {
		t.Fatalf("Open election-timeout error = %v, want errors.Is(_, ErrNoLeader)", err)
	}
	if db != nil {
		t.Fatalf("Open returned non-nil DB on failure: %v", db)
	}

	// The failed Open must have cleaned up after itself: the directory is
	// immediately re-openable and reaches leadership normally.
	newClusterLeaderTimeout = old
	db2, err := Open(cfg, NewCluster())
	if err != nil {
		t.Fatalf("re-Open after election-timeout failure: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	if err := db2.Set("k", "v"); err != nil {
		t.Fatalf("Set after re-Open: %v", err)
	}
}

// TestMutationImmutable verifies a Mutation is frozen at construction:
// mutating the source key, value, or option slice afterwards cannot change
// what Batch applies (and cannot race a concurrent Batch).
func TestMutationImmutable(t *testing.T) {
	db := testNode(t, freePort(t), true)

	key := []byte("immut/key")
	value := []byte("original")
	opts := []SetOption{WithTTL(time.Hour)}
	m := SetBytesOp(key, value, opts...)

	// Mutate every source after construction.
	key[0] = 'X'
	value[0] = 'X'
	opts[0] = WithTTL(0) // would fail validation if it leaked into m

	if err := db.Batch(m); err != nil {
		t.Fatalf("Batch with mutated sources: %v", err)
	}
	got, err := db.GetBytes([]byte("immut/key"), ReadOptions{})
	if err != nil || string(got) != "original" {
		t.Fatalf("GetBytes(immut/key) = %q, %v; want original", got, err)
	}
	if _, err := db.GetBytes([]byte("Xmmut/key"), ReadOptions{Mode: ReadLocal}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("mutated key was written: %v", err)
	}
	// The original ~1h TTL must have been applied, not the mutated zero.
	err = db.ViewBadger(ReadOptions{}, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("immut/key"))
		if err != nil {
			return err
		}
		exp := time.Unix(int64(item.ExpiresAt()), 0)
		if d := time.Until(exp); d < 55*time.Minute || d > time.Hour {
			return fmt.Errorf("ExpiresAt = %v (%s from now), want ~1h", exp, d)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("TTL check: %v", err)
	}
}
