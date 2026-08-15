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
// and closes it when the test ends. bootstrap must be true for exactly the
// first node of a new cluster.
func openNode(t *testing.T, id string, bootstrap bool) (*honeybadger.DB, string) {
	t.Helper()
	port, err := freeTCPPort()
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	db, err := honeybadger.Open(honeybadger.Config{
		NodeID:    id,
		RaftBind:  addr,
		DataDir:   t.TempDir(),
		Bootstrap: bootstrap,
	})
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

// TestConsumerSingleNodeWorkflow walks the everyday path: bootstrap, wait
// for leadership, CRUD, TTLs, batches, scans, and introspection.
func TestConsumerSingleNodeWorkflow(t *testing.T) {
	db, _ := openNode(t, "consumer-1", true)

	if err := db.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if !db.IsLeader() {
		t.Fatal("single bootstrap node should be leader")
	}
	if got := db.State(); got != "Leader" {
		t.Fatalf("State() = %q, want Leader", got)
	}
	if id, addr := db.Leader(); id == "" || addr == "" {
		t.Fatalf("Leader() = (%q, %q), want both non-empty", id, addr)
	}

	// Set / Get roundtrip, including overwrite.
	if err := db.Set([]byte("color"), []byte("blue")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("color"), []byte("green")); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	got, err := db.Get([]byte("color"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "green" {
		t.Fatalf("Get = %q, want %q", got, "green")
	}

	// Get returns a copy: mutating it must not corrupt stored state.
	got[0] = 'X'
	again, err := db.Get([]byte("color"))
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	if string(again) != "green" {
		t.Fatalf("stored value mutated via returned slice: %q", again)
	}

	// Missing key.
	if _, err := db.Get([]byte("nope")); !errors.Is(err, honeybadger.ErrKeyNotFound) {
		t.Fatalf("Get missing: err = %v, want errors.Is(_, ErrKeyNotFound)", err)
	}

	// Delete, then re-Get; deleting a missing key is fine.
	if err := db.Delete([]byte("color")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get([]byte("color")); !errors.Is(err, honeybadger.ErrKeyNotFound) {
		t.Fatalf("Get after Delete: err = %v, want ErrKeyNotFound", err)
	}
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("Delete missing should be a no-op, got %v", err)
	}

	// TTL: present immediately, gone shortly after expiry (Badger expires
	// on read). Poll instead of sleeping a fixed amount to stay robust
	// under -race on loaded machines.
	if err := db.SetWithTTL([]byte("otp"), []byte("123456"), time.Second); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	if _, err := db.Get([]byte("otp")); err != nil {
		t.Fatalf("TTL key should exist right after write: %v", err)
	}
	waitForCond(t, 5*time.Second, "TTL key to expire", func() bool {
		_, err := db.Get([]byte("otp"))
		return errors.Is(err, honeybadger.ErrKeyNotFound)
	})

	// Batch: atomic sets + deletes in one raft entry.
	err = db.Batch(
		[]honeybadger.Pair{
			{Key: []byte("acct:1"), Value: []byte("100")},
			{Key: []byte("acct:2"), Value: []byte("250")},
			{Key: []byte("acct:3"), Value: []byte("75")},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("Batch sets: %v", err)
	}
	err = db.Batch([]honeybadger.Pair{{Key: []byte("acct:4"), Value: []byte("0")}}, [][]byte{[]byte("acct:3")})
	if err != nil {
		t.Fatalf("Batch mixed: %v", err)
	}

	// PrefixScan returns key-ordered pairs and honors the limit.
	pairs, err := db.PrefixScan([]byte("acct:"), 0)
	if err != nil {
		t.Fatalf("PrefixScan: %v", err)
	}
	wantKeys := []string{"acct:1", "acct:2", "acct:4"}
	if len(pairs) != len(wantKeys) {
		t.Fatalf("PrefixScan returned %d pairs, want %d: %v", len(pairs), len(wantKeys), pairs)
	}
	for i, wk := range wantKeys {
		if string(pairs[i].Key) != wk {
			t.Fatalf("pairs[%d].Key = %q, want %q", i, pairs[i].Key, wk)
		}
	}
	limited, err := db.PrefixScan([]byte("acct:"), 1)
	if err != nil {
		t.Fatalf("PrefixScan limited: %v", err)
	}
	if len(limited) != 1 || string(limited[0].Key) != "acct:1" {
		t.Fatalf("PrefixScan limit=1 = %v, want just acct:1", limited)
	}

	// Linearizable read on the leader.
	if _, err := db.GetConsistent([]byte("acct:1")); err != nil {
		t.Fatalf("GetConsistent on leader: %v", err)
	}

	// Escape hatch: raw Badger read transaction.
	var count int
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if count != len(wantKeys) {
		t.Fatalf("View counted %d keys, want %d", count, len(wantKeys))
	}

	// Introspection.
	stats := db.Stats()
	if stats["honeybadger_applied_index"] == "" {
		t.Fatal("Stats() missing honeybadger_applied_index")
	}
	if stats["state"] != "Leader" {
		t.Fatalf("Stats()[state] = %q, want Leader", stats["state"])
	}

	// Close is documented idempotent; a consumer should be able to defer
	// it and also call it explicitly.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be a no-op success, got %v", err)
	}
	if got := db.State(); got != "Shutdown" {
		t.Fatalf("State() after Close = %q, want Shutdown", got)
	}
}

// TestConsumerClusterWorkflow is the multi-node reality check: join two
// followers, watch writes converge, and confirm the error surface a
// consumer hits when talking to a follower.
func TestConsumerClusterWorkflow(t *testing.T) {
	n1, _ := openNode(t, "cons-1", true)
	n2, addr2 := openNode(t, "cons-2", false)
	n3, addr3 := openNode(t, "cons-3", false)

	if err := n1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("n1 WaitForLeader: %v", err)
	}
	if err := n1.Join("cons-2", addr2); err != nil {
		t.Fatalf("Join cons-2: %v", err)
	}
	if err := n1.Join("cons-3", addr3); err != nil {
		t.Fatalf("Join cons-3: %v", err)
	}
	for i, n := range []*honeybadger.DB{n1, n2, n3} {
		if err := n.WaitForLeader(10 * time.Second); err != nil {
			t.Fatalf("node %d WaitForLeader: %v", i+1, err)
		}
	}

	// The leader should be able to identify itself consistently.
	if !n1.IsLeader() {
		t.Skip("bootstrap node lost leadership; skipping leader-dependent checks")
	}
	leaderID, _ := n1.Leader()
	if leaderID != "cons-1" {
		t.Fatalf("n1 thinks leader is %q, want cons-1", leaderID)
	}

	// Writes on the leader converge on followers.
	const keys = 20
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key:%02d", i)
		if err := n1.Set([]byte(k), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	for ni, n := range []*honeybadger.DB{n2, n3} {
		waitForCond(t, 15*time.Second, fmt.Sprintf("node %d to replicate %d keys", ni+2, keys), func() bool {
			for i := 0; i < keys; i++ {
				v, err := n.Get([]byte(fmt.Sprintf("key:%02d", i)))
				if err != nil || string(v) != fmt.Sprintf("value-%d", i) {
					return false
				}
			}
			return true
		})
	}

	// A delete on the leader propagates too.
	if err := n1.Delete([]byte("key:00")); err != nil {
		t.Fatalf("Delete on leader: %v", err)
	}
	waitForCond(t, 15*time.Second, "followers to see key:00 deleted", func() bool {
		_, e2 := n2.Get([]byte("key:00"))
		_, e3 := n3.Get([]byte("key:00"))
		return errors.Is(e2, honeybadger.ErrKeyNotFound) && errors.Is(e3, honeybadger.ErrKeyNotFound)
	})

	// Error surface on a follower: every mutation path must fail with an
	// error matching errors.Is(err, ErrNotLeader).
	follower := n2
	if err := follower.Set([]byte("x"), []byte("y")); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Set on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.SetWithTTL([]byte("x"), []byte("y"), time.Minute); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("SetWithTTL on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Delete([]byte("x")); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Delete on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Batch([]honeybadger.Pair{{Key: []byte("x"), Value: []byte("y")}}, nil); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Batch on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Join("cons-4", "127.0.0.1:1"); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Join on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Remove("cons-3"); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Remove on follower: err = %v, want ErrNotLeader", err)
	}
	if err := follower.Barrier(time.Second); !errors.Is(err, honeybadger.ErrNotLeader) {
		t.Fatalf("Barrier on follower: err = %v, want ErrNotLeader", err)
	}

	// GetConsistent on a follower is documented to fall back to a local
	// read: after convergence it serves the replicated value.
	waitForCond(t, 15*time.Second, "follower GetConsistent to serve key:01", func() bool {
		v, err := follower.GetConsistent([]byte("key:01"))
		return err == nil && string(v) == "value-1"
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
		NodeID:    "restart-1",
		RaftBind:  fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:   dir,
		Bootstrap: true,
	}

	db, err := honeybadger.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if err := db.Set([]byte("durable"), []byte("yes")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.SetWithTTL([]byte("ephemeral"), []byte("no"), 50*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := honeybadger.Open(cfg)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	if err := db2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader after restart: %v", err)
	}

	v, err := db2.Get([]byte("durable"))
	if err != nil {
		t.Fatalf("Get durable after restart: %v", err)
	}
	if string(v) != "yes" {
		t.Fatalf("durable = %q, want %q", v, "yes")
	}

	// The TTL key expired during the shutdown window and must stay gone.
	waitForCond(t, 5*time.Second, "expired key to stay absent", func() bool {
		_, err := db2.Get([]byte("ephemeral"))
		return errors.Is(err, honeybadger.ErrKeyNotFound)
	})
}
