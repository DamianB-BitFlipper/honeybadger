// Consumer-style tests: they exercise honeybadger exactly the way a real
// embedding application would — through the exported API only, in an
// external test package — and double as an API-ergonomics review.
package honeybadger_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"honeybadger"

	"github.com/dgraph-io/badger/v4"
)

// openNode starts a node in a fresh t.TempDir() on a free loopback port
// and closes it when the test ends. newCluster must be true for exactly
// the first node of a new cluster; plain Open never bootstraps.
func openNode(t *testing.T, id string, newCluster bool) (*honeybadger.DB, string) {
	t.Helper()
	port, err := freeTCPPort()
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := honeybadger.Config{
		NodeID:   id,
		RaftBind: addr,
		DataDir:  t.TempDir(),
	}
	var db *honeybadger.DB
	if newCluster {
		db, err = honeybadger.Open(cfg, honeybadger.NewCluster())
	} else {
		db, err = honeybadger.Open(cfg)
	}
	if err != nil {
		t.Fatalf("open %s: %v", id, err)
	}
	t.Cleanup(func() { db.Close() })
	return db, addr
}

// waitForCond polls cond until it holds or fails the test after timeout.
func waitForCond(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// localRead is the deliberate opt-in to eventually consistent local reads
// used when watching followers converge.
var localRead = honeybadger.ReadOptions{Mode: honeybadger.ReadLocal}

// TestConsumerSingleNodeWorkflow walks the everyday path: NewCluster, CRUD,
// TTLs, batches, scans, and introspection.
func TestConsumerSingleNodeWorkflow(t *testing.T) {
	// NewCluster bootstraps AND waits for the first election: no separate
	// WaitForLeader dance before the first write.
	db, _ := openNode(t, "consumer-1", true)

	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != honeybadger.StateLeader {
		t.Fatalf("State = %s, want Leader", st.State)
	}
	if st.Leader == nil || st.Leader.ID == "" || st.Leader.RaftAddr == "" {
		t.Fatalf("Leader = %+v, want set", st.Leader)
	}

	// Set / Get roundtrip, including overwrite.
	if err := db.Set("color", "blue"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set("color", "green"); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	got, err := db.Get("color")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "green" {
		t.Fatalf("Get = %q, want %q", got, "green")
	}

	// Byte reads return copies: mutating one must not corrupt stored state.
	gotBytes, err := db.GetBytes([]byte("color"), honeybadger.ReadOptions{})
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}
	gotBytes[0] = 'X'
	again, err := db.Get("color")
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	if again != "green" {
		t.Fatalf("stored value mutated via returned slice: %q", again)
	}

	// Missing key.
	if _, err := db.Get("nope"); !errors.Is(err, honeybadger.ErrKeyNotFound) {
		t.Fatalf("Get missing: err = %v, want errors.Is(_, ErrKeyNotFound)", err)
	}

	// Delete, then re-Get; deleting a missing key is fine.
	if err := db.Delete("color"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get("color"); !errors.Is(err, honeybadger.ErrKeyNotFound) {
		t.Fatalf("Get after Delete: err = %v, want ErrKeyNotFound", err)
	}
	if err := db.Delete("never-existed"); err != nil {
		t.Fatalf("Delete missing should be a no-op, got %v", err)
	}

	// TTL: present immediately, gone shortly after expiry (Badger expires
	// on read). Poll instead of sleeping a fixed amount to stay robust
	// under -race on loaded machines.
	if err := db.Set("otp", "123456", honeybadger.WithTTL(time.Second)); err != nil {
		t.Fatalf("Set WithTTL: %v", err)
	}
	if _, err := db.Get("otp"); err != nil {
		t.Fatalf("TTL key should exist right after write: %v", err)
	}
	waitForCond(t, 5*time.Second, "TTL key to expire", func() bool {
		_, err := db.Get("otp")
		return errors.Is(err, honeybadger.ErrKeyNotFound)
	})

	// Batch: atomic sets + deletes in one raft entry.
	err = db.Batch(
		honeybadger.SetOp("acct:1", "100"),
		honeybadger.SetOp("acct:2", "250"),
		honeybadger.SetOp("acct:3", "75"),
	)
	if err != nil {
		t.Fatalf("Batch sets: %v", err)
	}
	err = db.Batch(
		honeybadger.SetOp("acct:4", "0"),
		honeybadger.DeleteOp("acct:3"),
	)
	if err != nil {
		t.Fatalf("Batch mixed: %v", err)
	}

	// ScanPrefixBytes returns key-ordered entries and honors the limit.
	entries, err := db.ScanPrefixBytes([]byte("acct:"), honeybadger.ScanOptions{Unlimited: true})
	if err != nil {
		t.Fatalf("ScanPrefixBytes: %v", err)
	}
	wantKeys := []string{"acct:1", "acct:2", "acct:4"}
	if len(entries) != len(wantKeys) {
		t.Fatalf("ScanPrefixBytes returned %d entries, want %d: %v", len(entries), len(wantKeys), entries)
	}
	for i, wk := range wantKeys {
		if string(entries[i].Key) != wk {
			t.Fatalf("entries[%d].Key = %q, want %q", i, entries[i].Key, wk)
		}
	}
	limited, err := db.ScanPrefixBytes([]byte("acct:"), honeybadger.ScanOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ScanPrefixBytes limited: %v", err)
	}
	if len(limited) != 1 || string(limited[0].Key) != "acct:1" {
		t.Fatalf("ScanPrefixBytes limit=1 = %v, want just acct:1", limited)
	}

	// Escape hatch: raw Badger read transaction.
	var count int
	err = db.ViewBadger(honeybadger.ReadOptions{}, func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ViewBadger: %v", err)
	}
	if count != len(wantKeys) {
		t.Fatalf("ViewBadger counted %d keys, want %d", count, len(wantKeys))
	}

	// Introspection.
	st, err = db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.AppliedIndex == 0 {
		t.Fatal("Status.AppliedIndex = 0 after writes")
	}
	stats := db.RawRaftStats()
	if stats["honeybadger_applied_index"] == "" {
		t.Fatal("RawRaftStats() missing honeybadger_applied_index")
	}
	if stats["state"] != "Leader" {
		t.Fatalf("RawRaftStats()[state] = %q, want Leader", stats["state"])
	}

	// Close is documented idempotent; a consumer should be able to defer
	// it and also call it explicitly.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be a no-op success, got %v", err)
	}
	// After Close every operational method reports ErrClosed, while
	// RawRaftStats keeps returning the final statistics.
	if _, err := db.Status(); !errors.Is(err, honeybadger.ErrClosed) {
		t.Fatalf("Status after Close = %v, want ErrClosed", err)
	}
	if got := db.RawRaftStats()["state"]; got != "Shutdown" {
		t.Fatalf("RawRaftStats()[state] after Close = %q, want Shutdown", got)
	}
}

// TestConsumerClusterWorkflow is the multi-node reality check: add two
// followers, watch writes converge, and confirm the error surface a
// consumer hits when talking to a follower.
func TestConsumerClusterWorkflow(t *testing.T) {
	n1, _ := openNode(t, "cons-1", true)
	n2, addr2 := openNode(t, "cons-2", false)
	n3, addr3 := openNode(t, "cons-3", false)

	// n1 is already elected (NewCluster waited) and can add voters.
	if err := n1.AddVoter(honeybadger.Node{ID: "cons-2", RaftAddr: addr2}); err != nil {
		t.Fatalf("AddVoter cons-2: %v", err)
	}
	if err := n1.AddVoter(honeybadger.Node{ID: "cons-3", RaftAddr: addr3}); err != nil {
		t.Fatalf("AddVoter cons-3: %v", err)
	}
	for i, n := range []*honeybadger.DB{n1, n2, n3} {
		if _, err := n.WaitForLeader(10 * time.Second); err != nil {
			t.Fatalf("node %d WaitForLeader: %v", i+1, err)
		}
	}

	// The leader should be able to identify itself consistently.
	st, err := n1.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != honeybadger.StateLeader {
		t.Skip("bootstrap node lost leadership; skipping leader-dependent checks")
	}
	if st.Leader == nil || st.Leader.ID != "cons-1" {
		t.Fatalf("n1 thinks leader is %+v, want cons-1", st.Leader)
	}

	// Writes on the leader converge on followers (watched via local reads).
	const keys = 20
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key:%02d", i)
		if err := n1.Set(k, fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	for ni, n := range []*honeybadger.DB{n2, n3} {
		waitForCond(t, 15*time.Second, fmt.Sprintf("node %d to replicate %d keys", ni+2, keys), func() bool {
			for i := 0; i < keys; i++ {
				v, err := n.GetWithOptions(fmt.Sprintf("key:%02d", i), localRead)
				if err != nil || v != fmt.Sprintf("value-%d", i) {
					return false
				}
			}
			return true
		})
	}

	// A delete on the leader propagates too.
	if err := n1.Delete("key:00"); err != nil {
		t.Fatalf("Delete on leader: %v", err)
	}
	waitForCond(t, 15*time.Second, "followers to see key:00 deleted", func() bool {
		_, e2 := n2.GetWithOptions("key:00", localRead)
		_, e3 := n3.GetWithOptions("key:00", localRead)
		return errors.Is(e2, honeybadger.ErrKeyNotFound) && errors.Is(e3, honeybadger.ErrKeyNotFound)
	})

	// Error surface on a follower: every mutation path, and the strict
	// default read, must fail with an error matching
	// errors.Is(err, ErrNotLeader).
	follower := n2
	if err := follower.Set("x", "y"); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Set on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Set("x", "y", honeybadger.WithTTL(time.Minute)); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Set WithTTL on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Delete("x"); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Delete on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Batch(honeybadger.SetOp("x", "y")); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Batch on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.AddVoter(honeybadger.Node{ID: "cons-4", RaftAddr: "127.0.0.1:1"}); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("AddVoter on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.RemoveNode("cons-3"); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("RemoveNode on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Barrier(time.Second); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Barrier on follower: err = %v, want ErrNotLeader", err)
	}
	if _, err := follower.Get("key:01"); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Get on follower: err = %v, want ErrNotLeader", err)
	}

	// The deliberate ReadLocal opt-in serves the replicated value on the
	// follower after convergence.
	waitForCond(t, 15*time.Second, "follower local read to serve key:01", func() bool {
		v, err := follower.GetWithOptions("key:01", localRead)
		return err == nil && v == "value-1"
	})
}

// TestConsumerRestartPersistence closes a node and re-opens it against the
// same data directory: the Badger data and the Raft log both survive, so
// the node rejoins leadership and serves its old keys.
func TestConsumerRestartPersistence(t *testing.T) {
	dir := t.TempDir()
	port, err := freeTCPPort()
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	cfg := honeybadger.Config{
		NodeID:   "restart-1",
		RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:  dir,
	}

	db, err := honeybadger.Open(cfg, honeybadger.NewCluster())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Set("durable", "yes"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set("ephemeral", "no", honeybadger.WithTTL(50*time.Millisecond)); err != nil {
		t.Fatalf("Set WithTTL: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-opening with NewCluster is tolerated (stale bootstrap ignored)
	// and waits for leadership again.
	db2, err := honeybadger.Open(cfg, honeybadger.NewCluster())
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })

	v, err := db2.Get("durable")
	if err != nil {
		t.Fatalf("Get durable after restart: %v", err)
	}
	if v != "yes" {
		t.Fatalf("durable = %q, want %q", v, "yes")
	}

	// The TTL key expired during the shutdown window and must stay gone.
	waitForCond(t, 5*time.Second, "expired key to stay absent", func() bool {
		_, err := db2.Get("ephemeral")
		return errors.Is(err, honeybadger.ErrKeyNotFound)
	})
}
