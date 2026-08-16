package honeybadger

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

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

	// Every leader-only operation must fail with ErrNotLeader on a follower.
	cases := []struct {
		name string
		call func() error
	}{
		{"Set", func() error { return follower.Set("k", "v") }},
		{"Set WithTTL", func() error { return follower.Set("k", "v", WithTTL(time.Minute)) }},
		{"Delete", func() error { return follower.Delete("k") }},
		{"Batch", func() error { return follower.Batch(SetOp("k", "v")) }},
		{"AddVoter", func() error { return follower.AddVoter(Node{ID: "node-x", RaftAddr: "127.0.0.1:1"}) }},
		{"RemoveNode", func() error { return follower.RemoveNode("node-x") }},
		{"Barrier", func() error { return follower.Barrier(time.Second) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrNotLeader) {
				t.Fatalf("%v, want ErrNotLeader", err)
			}
		})
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
