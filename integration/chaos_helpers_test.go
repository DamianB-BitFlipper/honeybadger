// The integration tests exercise multi-node write storms, follower
// restarts, and snapshot catch-up using real loopback TCP transports.
// Longer scenarios skip under -short; this file holds their shared helpers.
package integration

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DamianB-BitFlipper/honeybadger"
)

const (
	// chaosPoll is the interval between condition re-checks in poll loops.
	chaosPoll = 120 * time.Millisecond
	// chaosConvergeTO bounds a single "all nodes converge" wait.
	chaosConvergeTO = 90 * time.Second
)

// chaosLocal is the read mode used by all convergence checks: deliberately
// local (eventually consistent), on any node.
var chaosLocal = honeybadger.ReadOptions{Mode: honeybadger.ReadLocal}

// chaosNode couples an open DB with the Config it was opened from so tests
// can close and re-open it in place (simulating a process restart).
type chaosNode struct {
	db         *honeybadger.DB
	cfg        honeybadger.Config
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
func chaosIsLeader(db *honeybadger.DB) bool {
	st, err := db.Status()
	return err == nil && st.State == honeybadger.StateLeader
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
		cfg := honeybadger.Config{
			NodeID:   id,
			RaftBind: fmt.Sprintf("127.0.0.1:%d", chaosFreePort(t)),
			DataDir:  dir,
			Advanced: honeybadger.AdvancedConfig{
				ApplyTimeout:      10 * time.Second,
				SnapshotThreshold: snapThreshold,
			},
		}
		var db *honeybadger.DB
		var err error
		if newCluster {
			db, err = honeybadger.Open(cfg, honeybadger.NewCluster())
		} else {
			db, err = honeybadger.Open(cfg)
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
		var db *honeybadger.DB
		var err error
		if n.newCluster {
			db, err = honeybadger.Open(n.cfg, honeybadger.NewCluster())
		} else {
			db, err = honeybadger.Open(n.cfg)
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
func chaosApplyOnLeader(t *testing.T, timeout time.Duration, nodes []*chaosNode, fn func(*honeybadger.DB) error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ldr := chaosLeader(t, 30*time.Second, nodes...)
		err := fn(ldr.db)
		if err == nil {
			return
		}
		if errors.Is(err, honeybadger.ErrNotLeader) && time.Now().Before(deadline) {
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
				if err == nil || !errors.Is(err, honeybadger.ErrNotLeader) {
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
	chaosApplyOnLeader(t, 60*time.Second, members, func(db *honeybadger.DB) error {
		return db.AddVoter(honeybadger.Node{ID: joiner.cfg.NodeID, RaftAddr: joiner.cfg.RaftBind})
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
func chaosScan(db *honeybadger.DB, prefix string) (map[string]string, error) {
	entries, err := db.ScanPrefixBytes([]byte(prefix), honeybadger.ScanOptions{
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
