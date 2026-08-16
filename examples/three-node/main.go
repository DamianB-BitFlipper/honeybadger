// Command three-node is a runnable honeybadger demo: it starts a three-node
// cluster inside a single process, elects a leader, replicates writes
// through Raft, reads them back from a follower, and shuts down cleanly.
//
// Run it from the repository root with:
//
//	go run ./examples/three-node
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"honeybadger"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("demo failed: %v", err)
	}
}

func run() error {
	root, err := os.MkdirTemp("", "honeybadger-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	fmt.Println("== honeybadger three-node demo ==")
	fmt.Printf("(data dir %s is removed on exit)\n\n", root)

	nodes, ids, addrs, cleanup, err := bootNodes(root)
	if err != nil {
		return err
	}
	// Shut down in reverse boot order, whatever fails from here on.
	defer func() {
		fmt.Println("\n== shutting down ==")
		cleanup()
	}()

	if err := formCluster(nodes, ids, addrs); err != nil {
		return err
	}
	if err := demoWrites(nodes[0]); err != nil {
		return err
	}
	follower, followerIndex := firstFollower(nodes)
	if err := demoReads(follower, ids[followerIndex], addrs[followerIndex]); err != nil {
		return err
	}
	if err := demoTTL(follower); err != nil {
		return err
	}

	fmt.Println("\n== demo complete ==")
	return nil
}

// bootNodes opens the three demo nodes and returns them with a cleanup
// that closes them in reverse boot order. Only the very first node of a
// new cluster is opened with NewCluster() — it bootstraps AND waits for
// the first election; the others are opened plain (plain Open never
// bootstraps) and joined in formCluster. If a later Open fails, the nodes
// opened so far are closed before the error is returned.
func bootNodes(root string) (nodes []*honeybadger.DB, ids, addrs []string, cleanup func(), err error) {
	ids = []string{"node-1", "node-2", "node-3"}
	addrs = make([]string, len(ids))
	nodes = make([]*honeybadger.DB, len(ids))

	fmt.Println("== booting nodes ==")
	opened := 0
	cleanup = func() {
		for i := opened - 1; i >= 0; i-- {
			if err := nodes[i].Close(); err != nil {
				fmt.Printf("[stop] %s: %v\n", ids[i], err)
			} else {
				fmt.Printf("[stop] %s closed\n", ids[i])
			}
		}
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	for i, id := range ids {
		var port int
		port, err = freePort()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", port)
		cfg := honeybadger.Config{
			NodeID:   id,
			RaftBind: addrs[i],
			DataDir:  filepath.Join(root, id),
		}
		if i == 0 {
			nodes[i], err = honeybadger.Open(cfg, honeybadger.NewCluster())
		} else {
			nodes[i], err = honeybadger.Open(cfg)
		}
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("open %s: %w", id, err)
		}
		opened++
		fmt.Printf("[boot] %-7s raft=%s newCluster=%v\n", id, addrs[i], i == 0)
	}
	return nodes, ids, addrs, cleanup, nil
}

// formCluster joins nodes[1:] as voters on the bootstrap node — already
// elected, since NewCluster waited for the first election — and waits
// until every node reports itself a voter before presenting the formation
// status. Membership changes are leader-only.
func formCluster(nodes []*honeybadger.DB, ids, addrs []string) error {
	fmt.Println("\n== forming cluster ==")
	leader := nodes[0]
	for i := 1; i < len(nodes); i++ {
		if err := leader.AddVoter(honeybadger.Node{ID: ids[i], RaftAddr: addrs[i]}); err != nil {
			return fmt.Errorf("add voter %s: %w", ids[i], err)
		}
		fmt.Printf("[join] %s added as voter\n", ids[i])
	}
	for i, n := range nodes {
		if _, err := n.WaitForLeader(10 * time.Second); err != nil {
			return fmt.Errorf("%s: %w", ids[i], err)
		}
		// A known leader does not imply this node has applied its own
		// membership entry yet: wait until the replicated configuration
		// shows it as a voter before reporting formation.
		if err := waitFor(10*time.Second, ids[i]+" to report itself voter", func() bool {
			st, err := n.Status()
			return err == nil && st.Local.Role == honeybadger.RoleVoter
		}); err != nil {
			return fmt.Errorf("%s: %w", ids[i], err)
		}
		st, err := n.Status()
		if err != nil {
			return fmt.Errorf("%s: %w", ids[i], err)
		}
		fmt.Printf("[raft] %-7s state=%s role=%s\n", ids[i], st.State, st.Local.Role)
	}
	st, err := leader.Status()
	if err != nil {
		return err
	}
	if st.Leader == nil {
		return fmt.Errorf("%s reports no cluster leader yet", ids[0])
	}
	fmt.Printf("[raft] cluster leader: %s at %s\n", st.Leader.ID, st.Leader.RaftAddr)
	return nil
}

// demoWrites sends a Set, a TTL Set and a Batch to the leader. Every
// mutation is a Raft log entry first; each node's FSM applies it to its
// local Badger only after the entry is committed.
func demoWrites(leader *honeybadger.DB) error {
	fmt.Println("\n== writes (sent to the leader) ==")
	if err := leader.Set("user:1", "ada"); err != nil {
		return err
	}
	fmt.Println(`[set]  user:1 = "ada"`)

	if err := leader.Set("session:xyz", "token-123", honeybadger.WithTTL(2*time.Second)); err != nil {
		return err
	}
	fmt.Println(`[set]  session:xyz = "token-123" (TTL 2s)`)

	err := leader.Batch(
		honeybadger.SetOp("user:2", "grace"),
		honeybadger.SetOp("user:3", "edsger"),
		honeybadger.DeleteOp("user:1"),
	)
	if err != nil {
		return err
	}
	fmt.Println("[batch] set user:2 + user:3, deleted user:1 (one Raft entry, one txn)")
	return nil
}

// firstFollower returns the first node that does not report itself leader.
func firstFollower(nodes []*honeybadger.DB) (*honeybadger.DB, int) {
	for i, n := range nodes {
		if st, err := n.Status(); err == nil && st.State != honeybadger.StateLeader {
			return n, i
		}
	}
	return nodes[1], 1
}

// demoReads reads the writes back from a follower. Tier 1 Get is strictly
// consistent and leader-only, so the demo deliberately opts into local
// reads with ReadOptions{Mode: ReadLocal} and polls until the follower has
// replayed the entries.
func demoReads(follower *honeybadger.DB, id, addr string) error {
	fmt.Println("\n== reads (local, on a follower) ==")
	fmt.Printf("[read] using follower %s at %s\n", id, addr)
	local := honeybadger.ReadOptions{Mode: honeybadger.ReadLocal}

	if err := waitFor(10*time.Second, "follower to replicate user:2", func() bool {
		v, err := follower.GetWithOptions("user:2", local)
		return err == nil && v == "grace"
	}); err != nil {
		return err
	}
	fmt.Println("[repl] follower caught up")

	v, err := follower.GetWithOptions("user:2", local)
	if err != nil {
		return err
	}
	fmt.Printf("[get]  user:2 = %q\n", v)

	if err := waitFor(10*time.Second, "follower to see user:1 deleted", func() bool {
		_, err := follower.GetWithOptions("user:1", local)
		return errors.Is(err, honeybadger.ErrKeyNotFound)
	}); err != nil {
		return err
	}
	fmt.Println("[get]  user:1 -> ErrKeyNotFound (batch delete replicated)")

	entries, err := follower.ScanPrefixBytes([]byte("user:"), honeybadger.ScanOptions{
		Read:      local,
		Unlimited: true,
	})
	if err != nil {
		return err
	}
	fmt.Printf(`[scan] prefix "user:" ->`)
	for _, e := range entries {
		fmt.Printf(" %s=%q", e.Key, e.Value)
	}
	fmt.Println()
	return nil
}

// demoTTL watches the TTL key expire on the follower: Badger expires
// entries on read, no sweep required.
func demoTTL(follower *honeybadger.DB) error {
	fmt.Println("\n== TTL expiry ==")
	local := honeybadger.ReadOptions{Mode: honeybadger.ReadLocal}
	if err := waitFor(10*time.Second, "session:xyz to replicate", func() bool {
		_, err := follower.GetWithOptions("session:xyz", local)
		return err == nil
	}); err != nil {
		return err
	}
	v, _ := follower.GetWithOptions("session:xyz", local)
	fmt.Printf("[get]  session:xyz = %q (before expiry)\n", v)
	fmt.Println("[wait] sleeping 2.6s for the 2s TTL to pass...")
	time.Sleep(2600 * time.Millisecond)
	_, err := follower.GetWithOptions("session:xyz", local)
	fmt.Printf("[get]  session:xyz -> ErrKeyNotFound: %v\n", errors.Is(err, honeybadger.ErrKeyNotFound))
	return nil
}

// freePort asks the OS for a spare TCP port on the loopback interface.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// waitFor polls cond until it reports success or the timeout expires.
func waitFor(timeout time.Duration, what string, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
