package honeybadger

import (
	"errors"
	"fmt"
	"net"
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
// registers its cleanup with t.
func testNode(t *testing.T, port int, bootstrap bool) *DB {
	t.Helper()
	db, err := Open(Config{
		NodeID:    fmt.Sprintf("node-%d", port),
		RaftBind:  fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:   t.TempDir(),
		Bootstrap: bootstrap,
	})
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
		if db.IsLeader() {
			return db
		}
	}
	return nil
}

func TestSingleNode(t *testing.T) {
	db := testNode(t, freePort(t), true)

	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if !db.IsLeader() {
		t.Fatal("single bootstrapped node should become leader")
	}
	if got := db.State(); got != "Leader" {
		t.Fatalf("State() = %q, want Leader", got)
	}
	if id, addr := db.Leader(); id == "" || addr == "" {
		t.Fatalf("Leader() = (%q, %q), want both set", id, addr)
	}

	// Set / Get.
	if err := db.Set([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, err := db.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "bar" {
		t.Fatalf("Get(foo) = %q, want %q", val, "bar")
	}
	// The returned slice must be a copy that stays valid.
	if err := db.Set([]byte("foo"), []byte("baz")); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	val, err = db.Get([]byte("foo"))
	if err != nil || string(val) != "baz" {
		t.Fatalf("Get(foo) after overwrite = %q, %v; want baz", val, err)
	}

	// Get on a missing key.
	if _, err := db.Get([]byte("missing")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrKeyNotFound", err)
	}

	// Delete.
	if err := db.Delete([]byte("foo")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get([]byte("foo")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(foo) after Delete = %v, want ErrKeyNotFound", err)
	}
	// Deleting a missing key is not an error.
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("Delete missing key: %v", err)
	}

	// Batch.
	err = db.Batch(
		[]Pair{
			{Key: []byte("a/1"), Value: []byte("v1")},
			{Key: []byte("a/2"), Value: []byte("v2")},
			{Key: []byte("b/1"), Value: []byte("v3")},
		},
		[][]byte{[]byte("foo")},
	)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	// PrefixScan.
	pairs, err := db.PrefixScan([]byte("a/"), 0)
	if err != nil {
		t.Fatalf("PrefixScan: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("PrefixScan(a/) returned %d pairs, want 2", len(pairs))
	}
	if string(pairs[0].Key) != "a/1" || string(pairs[0].Value) != "v1" ||
		string(pairs[1].Key) != "a/2" || string(pairs[1].Value) != "v2" {
		t.Fatalf("PrefixScan(a/) = %+v, want a/1=v1, a/2=v2 in order", pairs)
	}
	pairs, err = db.PrefixScan([]byte("a/"), 1)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("PrefixScan with limit 1 = %v, %v; want exactly 1 pair", pairs, err)
	}

	// View escape hatch.
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("b/1"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if string(v) != "v3" {
				return fmt.Errorf("View read b/1 = %q, want v3", v)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}

	// Barrier + GetConsistent on the leader.
	if err := db.Barrier(5 * time.Second); err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	if val, err := db.GetConsistent([]byte("b/1")); err != nil || string(val) != "v3" {
		t.Fatalf("GetConsistent(b/1) = %q, %v; want v3", val, err)
	}

	// Stats contains raft stats and the applied index.
	stats := db.Stats()
	if stats["state"] != "Leader" {
		t.Fatalf("Stats()[state] = %q, want Leader", stats["state"])
	}
	if stats["honeybadger_applied_index"] == "" || stats["honeybadger_applied_index"] == "0" {
		t.Fatalf("Stats() missing applied index: %v", stats["honeybadger_applied_index"])
	}
}

func TestSetWithTTL(t *testing.T) {
	db := testNode(t, freePort(t), true)
	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Badger stores expirations with second granularity, so a 2s TTL lives
	// at least ~1s and at most 2s.
	if err := db.SetWithTTL([]byte("ttl-key"), []byte("ttl-val"), 2*time.Second); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	val, err := db.Get([]byte("ttl-key"))
	if err != nil || string(val) != "ttl-val" {
		t.Fatalf("Get(ttl-key) = %q, %v; want ttl-val", val, err)
	}

	time.Sleep(3 * time.Second)
	if _, err := db.Get([]byte("ttl-key")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(ttl-key) after TTL expiry = %v, want ErrKeyNotFound", err)
	}
}

func TestThreeNodeCluster(t *testing.T) {
	port1, port2, port3 := freePort(t), freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)
	node3 := testNode(t, port3, false)
	nodes := []*DB{node1, node2, node3}

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}

	// Join the two followers through the leader.
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port3), fmt.Sprintf("127.0.0.1:%d", port3)); err != nil {
		t.Fatalf("Join node3: %v", err)
	}
	for i, db := range nodes {
		if err := db.WaitForLeader(15 * time.Second); err != nil {
			t.Fatalf("node%d WaitForLeader: %v", i+1, err)
		}
	}

	// Write 50 keys on the leader and wait for every follower to converge.
	const numKeys = 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := fmt.Sprintf("val-%03d", i)
		if err := node1.Set([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	for ni, db := range nodes {
		db := db
		waitFor(t, 15*time.Second, func() bool {
			for i := 0; i < numKeys; i++ {
				val, err := db.Get([]byte(fmt.Sprintf("key-%03d", i)))
				if err != nil || string(val) != fmt.Sprintf("val-%03d", i) {
					return false
				}
			}
			return true
		}, fmt.Sprintf("node%d to replicate all %d keys", ni+1, numKeys))
	}

	// Delete on the leader; followers must observe the deletion.
	if err := node1.Delete([]byte("key-007")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for ni, db := range nodes {
		db := db
		waitFor(t, 10*time.Second, func() bool {
			_, err := db.Get([]byte("key-007"))
			return errors.Is(err, ErrKeyNotFound)
		}, fmt.Sprintf("node%d to see key-007 deleted", ni+1))
	}

	// Atomic batch: sets and deletes in one log entry.
	err := node1.Batch(
		[]Pair{
			{Key: []byte("batch/1"), Value: []byte("b1")},
			{Key: []byte("batch/2"), Value: []byte("b2")},
			{Key: []byte("batch/ttl"), Value: []byte("b3"), TTL: time.Hour},
		},
		[][]byte{[]byte("key-001"), []byte("key-002")},
	)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	for ni, db := range nodes {
		db := db
		waitFor(t, 10*time.Second, func() bool {
			v1, e1 := db.Get([]byte("batch/1"))
			v2, e2 := db.Get([]byte("batch/2"))
			v3, e3 := db.Get([]byte("batch/ttl"))
			if e1 != nil || e2 != nil || e3 != nil ||
				string(v1) != "b1" || string(v2) != "b2" || string(v3) != "b3" {
				return false
			}
			_, d1 := db.Get([]byte("key-001"))
			_, d2 := db.Get([]byte("key-002"))
			return errors.Is(d1, ErrKeyNotFound) && errors.Is(d2, ErrKeyNotFound)
		}, fmt.Sprintf("node%d to apply batch atomically", ni+1))
	}
}

func TestNotLeader(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	follower := node2
	if node2.IsLeader() {
		follower = node1 // extremely unlikely, but stay correct either way
	}

	if err := follower.Set([]byte("k"), []byte("v")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Set on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Delete([]byte("k")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Delete on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Batch([]Pair{{Key: []byte("k"), Value: []byte("v")}}, nil); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Batch on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Join("node-x", "127.0.0.1:1"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Join on follower = %v, want ErrNotLeader", err)
	}
	if err := follower.Remove("node-x"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Remove on follower = %v, want ErrNotLeader", err)
	}
}

func TestGetConsistent(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	leader, follower := node1, node2
	if node2.IsLeader() {
		leader, follower = node2, node1
	}

	if err := leader.Set([]byte("consistent"), []byte("yes")); err != nil {
		t.Fatalf("Set on leader: %v", err)
	}

	// Linearizable read on the leader.
	val, err := leader.GetConsistent([]byte("consistent"))
	if err != nil || string(val) != "yes" {
		t.Fatalf("GetConsistent on leader = %q, %v; want yes", val, err)
	}

	// On a follower GetConsistent falls back to a local read, so poll until
	// the value has been replicated and applied.
	waitFor(t, 10*time.Second, func() bool {
		val, err := follower.GetConsistent([]byte("consistent"))
		return err == nil && string(val) == "yes"
	}, "follower GetConsistent to observe the leader write")
}

func TestRestart(t *testing.T) {
	port := freePort(t)
	dir := t.TempDir()
	cfg := Config{
		NodeID:    fmt.Sprintf("node-%d", port),
		RaftBind:  fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:   dir,
		Bootstrap: true,
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := db.Set([]byte(fmt.Sprintf("persist-%d", i)), []byte(fmt.Sprintf("value-%d", i))); err != nil {
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

	db2, err := Open(cfg)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	if err := db2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader after restart: %v", err)
	}
	for i := 0; i < 10; i++ {
		val, err := db2.Get([]byte(fmt.Sprintf("persist-%d", i)))
		if err != nil || string(val) != fmt.Sprintf("value-%d", i) {
			t.Fatalf("Get(persist-%d) after restart = %q, %v", i, val, err)
		}
	}
}

// TestSnapshotRestore forces a snapshot on the leader, compacts the log,
// and restarts a lagging follower, which must catch up via snapshot
// install (exercising fsm.Restore) and converge to the same state.
func TestSnapshotRestore(t *testing.T) {
	port1, port2, port3 := freePort(t), freePort(t), freePort(t)

	open := func(port int, bootstrap bool, dir string) *DB {
		db, err := Open(Config{
			NodeID:            fmt.Sprintf("node-%d", port),
			RaftBind:          fmt.Sprintf("127.0.0.1:%d", port),
			DataDir:           dir,
			Bootstrap:         bootstrap,
			SnapshotThreshold: 16,
		})
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

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port3), fmt.Sprintf("127.0.0.1:%d", port3)); err != nil {
		t.Fatalf("Join node3: %v", err)
	}
	if err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}
	if err := node3.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node3 WaitForLeader: %v", err)
	}

	// Give every node some initial state, then take node3 down.
	for i := 0; i < 5; i++ {
		if err := node1.Set([]byte(fmt.Sprintf("snap-%03d", i)), []byte(fmt.Sprintf("v-%03d", i))); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := node3.Get([]byte("snap-004"))
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
		if err := node1.Set([]byte(fmt.Sprintf("snap-%03d", i)), []byte(fmt.Sprintf("v-%03d", i))); err != nil {
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
			val, err := node3.Get([]byte(fmt.Sprintf("snap-%03d", i)))
			if err != nil || string(val) != fmt.Sprintf("v-%03d", i) {
				return false
			}
		}
		return true
	}, "node3 to converge after snapshot restore")

	// The cluster must keep working after the restore.
	if err := node1.Set([]byte("post-restore"), []byte("ok")); err != nil {
		t.Fatalf("Set after restore: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		val, err := node3.Get([]byte("post-restore"))
		return err == nil && string(val) == "ok"
	}, "node3 to receive post-restore writes")
}

// TestTTLNoResurrectAfterRestart is the regression test for TTL keys
// resurrecting when Raft replays log entries after a restart: the command
// now replicates an absolute expiry stamped once at write submission, so
// re-apply is idempotent.
func TestTTLNoResurrectAfterRestart(t *testing.T) {
	port := freePort(t)
	dir := t.TempDir()
	cfg := Config{
		NodeID:    fmt.Sprintf("node-%d", port),
		RaftBind:  fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:   dir,
		Bootstrap: true,
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if err := db.Set([]byte("durable"), []byte("stays")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.SetWithTTL([]byte("ttl"), []byte("goes"), 2*time.Second); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := db.Get([]byte("ttl"))
		return errors.Is(err, ErrKeyNotFound)
	}, "ttl key to expire")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open: Raft replays the log through the FSM. The replayed TTL
	// command carries its original (now past) absolute expiry, so the key
	// must stay gone — before, during, and after the replay.
	db2, err := Open(cfg)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	if err := db2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader after restart: %v", err)
	}
	for i := 0; i < 20; i++ {
		if _, err := db2.Get([]byte("ttl")); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("ttl key resurrected after restart (check %d): %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if val, err := db2.Get([]byte("durable")); err != nil || string(val) != "stays" {
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

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	follower := node2
	if node2.IsLeader() {
		follower = node1
	}

	err := follower.Set([]byte("k"), []byte("v"))
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
	leaderID, leaderAddr := follower.Leader()
	if nlErr.LeaderID != leaderID || nlErr.LeaderAddr != leaderAddr {
		t.Fatalf("NotLeaderError = (%q, %q), Leader() = (%q, %q)",
			nlErr.LeaderID, nlErr.LeaderAddr, leaderID, leaderAddr)
	}
}

// TestStringWrappers exercises the string convenience methods.
func TestStringWrappers(t *testing.T) {
	db := testNode(t, freePort(t), true)
	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	if err := db.SetString("skey", "sval"); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	val, err := db.GetString("skey")
	if err != nil || val != "sval" {
		t.Fatalf("GetString(skey) = %q, %v; want sval", val, err)
	}
	if _, err := db.GetString("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetString(missing) = %v, want ErrKeyNotFound", err)
	}
	if err := db.DeleteString("skey"); err != nil {
		t.Fatalf("DeleteString: %v", err)
	}
	if _, err := db.GetString("skey"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetString(skey) after DeleteString = %v, want ErrKeyNotFound", err)
	}
}

// TestNodes verifies cluster-membership introspection on leaders and
// followers alike.
func TestNodes(t *testing.T) {
	port1, port2, port3 := freePort(t), freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)
	node3 := testNode(t, port3, false)

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port3), fmt.Sprintf("127.0.0.1:%d", port3)); err != nil {
		t.Fatalf("Join node3: %v", err)
	}
	if err := node3.WaitForLeader(15 * time.Second); err != nil {
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
			nodes, err := db.Nodes()
			if err != nil || len(nodes) != 3 {
				return false
			}
			for _, n := range nodes {
				addr, ok := wantIDs[n.ID]
				if !ok || addr != n.Addr || n.Suffrage != "Voter" {
					return false
				}
			}
			return true
		}, fmt.Sprintf("node%d Nodes() to show all 3 voters", ni+1))
	}

	// ID and Addr reflect the node's own configuration.
	if node1.ID() != fmt.Sprintf("node-%d", port1) {
		t.Fatalf("node1.ID() = %q", node1.ID())
	}
	if node1.Addr() != fmt.Sprintf("127.0.0.1:%d", port1) {
		t.Fatalf("node1.Addr() = %q", node1.Addr())
	}
}

// TestGetLinearizable verifies the strictly leader-only consistent read.
func TestGetLinearizable(t *testing.T) {
	port1, port2 := freePort(t), freePort(t)
	node1 := testNode(t, port1, true)
	node2 := testNode(t, port2, false)

	if err := node1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node1 WaitForLeader: %v", err)
	}
	if err := node1.Join(fmt.Sprintf("node-%d", port2), fmt.Sprintf("127.0.0.1:%d", port2)); err != nil {
		t.Fatalf("Join node2: %v", err)
	}
	if err := node2.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("node2 WaitForLeader: %v", err)
	}

	leader, follower := node1, node2
	if node2.IsLeader() {
		leader, follower = node2, node1
	}

	if err := leader.Set([]byte("lin"), []byte("yes")); err != nil {
		t.Fatalf("Set on leader: %v", err)
	}

	// Follower: strictly not allowed, with the typed error.
	_, err := follower.GetLinearizable([]byte("lin"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("GetLinearizable on follower = %v, want ErrNotLeader", err)
	}
	var nlErr *NotLeaderError
	if !errors.As(err, &nlErr) {
		t.Fatalf("GetLinearizable on follower error %T is not *NotLeaderError", err)
	}

	// Leader: barrier + read.
	val, err := leader.GetLinearizable([]byte("lin"))
	if err != nil || string(val) != "yes" {
		t.Fatalf("GetLinearizable on leader = %q, %v; want yes", val, err)
	}
	if _, err := leader.GetLinearizable([]byte("nope")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetLinearizable(nope) on leader = %v, want ErrKeyNotFound", err)
	}
}

// TestAppliedIndex verifies the typed applied-index accessor advances as
// writes are committed.
func TestAppliedIndex(t *testing.T) {
	db := testNode(t, freePort(t), true)
	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	before := db.AppliedIndex()
	const writes = 5
	for i := 0; i < writes; i++ {
		if err := db.Set([]byte(fmt.Sprintf("ai-%d", i)), []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	after := db.AppliedIndex()
	if after < before+writes {
		t.Fatalf("AppliedIndex = %d before, %d after %d writes", before, after, writes)
	}
	stats := db.Stats()
	if stats["honeybadger_applied_index"] != fmt.Sprintf("%d", after) {
		t.Fatalf("Stats()[honeybadger_applied_index] = %q, want %d",
			stats["honeybadger_applied_index"], after)
	}
}

// TestClosedErrors verifies every public operation fails with ErrClosed
// after Close, and that Close stays idempotent.
func TestClosedErrors(t *testing.T) {
	port := freePort(t)
	db := testNode(t, port, true)
	if err := db.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	cases := map[string]func() error{
		"Set":             func() error { return db.Set([]byte("k"), []byte("v")) },
		"SetWithTTL":      func() error { return db.SetWithTTL([]byte("k"), []byte("v"), time.Second) },
		"Delete":          func() error { return db.Delete([]byte("k")) },
		"Batch":           func() error { return db.Batch([]Pair{{Key: []byte("k"), Value: []byte("v")}}, nil) },
		"Join":            func() error { return db.Join("x", "127.0.0.1:1") },
		"Remove":          func() error { return db.Remove("x") },
		"Barrier":         func() error { return db.Barrier(time.Second) },
		"Snapshot":        func() error { return db.Snapshot() },
		"GetLinearizable": func() error { _, err := db.GetLinearizable([]byte("k")); return err },
		"GetConsistent":   func() error { _, err := db.GetConsistent([]byte("k")); return err },
		"Get":             func() error { _, err := db.Get([]byte("k")); return err },
		"View":            func() error { return db.View(func(*badger.Txn) error { return nil }) },
		"PrefixScan":      func() error { _, err := db.PrefixScan([]byte("k"), 0); return err },
		"SetString":       func() error { return db.SetString("k", "v") },
		"GetString":       func() error { _, err := db.GetString("k"); return err },
		"DeleteString":    func() error { return db.DeleteString("k") },
		"Nodes":           func() error { _, err := db.Nodes(); return err },
	}
	for name, fn := range cases {
		if err := fn(); !errors.Is(err, ErrClosed) {
			t.Fatalf("%s after Close = %v, want ErrClosed", name, err)
		}
	}
}
