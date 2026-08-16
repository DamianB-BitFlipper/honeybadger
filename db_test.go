package honeybadger

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

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
