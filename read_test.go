package honeybadger

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

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
