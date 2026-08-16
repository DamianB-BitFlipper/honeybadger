// Follower-restart chaos tests.
package honeybadger

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

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

// TestChaosTTLAcrossRestart verifies that TTLs replicate as absolute
// expiries: a follower restarted inside the TTL window must serve the keys
// while they are unexpired and must report ErrKeyNotFound once the original
// expiry passes — the restart must not extend the lease (replaying the log
// used to restamp a fresh TTL per apply). The restart is deliberately
// positioned late in the TTL window so a restart-extended lease would
// outlive the expiry deadline by a wide, unflaky margin.
func TestChaosTTLAcrossRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos: skipping long-running scenario in -short mode")
	}
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
