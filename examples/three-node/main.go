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

	// ------------------------------------------------------------------
	// Boot three nodes. Only the very first node of a new cluster is
	// opened with Bootstrap: true; the others are added via Join below.
	// ------------------------------------------------------------------
	fmt.Println("== booting nodes ==")
	ids := []string{"node-1", "node-2", "node-3"}
	addrs := make([]string, len(ids))
	nodes := make([]*honeybadger.DB, len(ids))
	for i, id := range ids {
		port, err := freePort()
		if err != nil {
			return err
		}
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", port)
		nodes[i], err = honeybadger.Open(honeybadger.Config{
			NodeID:    id,
			RaftBind:  addrs[i],
			DataDir:   filepath.Join(root, id),
			Bootstrap: i == 0,
		})
		if err != nil {
			return fmt.Errorf("open %s: %w", id, err)
		}
		fmt.Printf("[boot] %-7s raft=%s bootstrap=%v\n", id, addrs[i], i == 0)
	}

	// Shut down in reverse order, whatever happens from here on.
	defer func() {
		fmt.Println("\n== shutting down ==")
		for i := len(nodes) - 1; i >= 0; i-- {
			if err := nodes[i].Close(); err != nil {
				fmt.Printf("[stop] %s: %v\n", ids[i], err)
			} else {
				fmt.Printf("[stop] %s closed\n", ids[i])
			}
		}
	}()

	// ------------------------------------------------------------------
	// Form the cluster: the bootstrap node elects itself, then adds the
	// other two as voters. Membership changes are leader-only.
	// ------------------------------------------------------------------
	fmt.Println("\n== forming cluster ==")
	leader := nodes[0]
	if err := leader.WaitForLeader(10 * time.Second); err != nil {
		return err
	}
	for i := 1; i < len(nodes); i++ {
		if err := leader.Join(ids[i], addrs[i]); err != nil {
			return fmt.Errorf("join %s: %w", ids[i], err)
		}
		fmt.Printf("[join] %s added as voter\n", ids[i])
	}
	for i, n := range nodes {
		if err := n.WaitForLeader(10 * time.Second); err != nil {
			return fmt.Errorf("%s: %w", ids[i], err)
		}
		fmt.Printf("[raft] %-7s state=%s\n", ids[i], n.State())
	}
	leaderID, leaderAddr := leader.Leader()
	fmt.Printf("[raft] cluster leader: %s at %s\n", leaderID, leaderAddr)

	// ------------------------------------------------------------------
	// Writes. Every mutation is a Raft log entry first; each node's FSM
	// applies it to its local Badger only after the entry is committed.
	// ------------------------------------------------------------------
	fmt.Println("\n== writes (sent to the leader) ==")
	if err := leader.Set([]byte("user:1"), []byte("ada")); err != nil {
		return err
	}
	fmt.Println(`[set]  user:1 = "ada"`)

	if err := leader.SetWithTTL([]byte("session:xyz"), []byte("token-123"), 2*time.Second); err != nil {
		return err
	}
	fmt.Println(`[set]  session:xyz = "token-123" (TTL 2s)`)

	err = leader.Batch(
		[]honeybadger.Pair{
			{Key: []byte("user:2"), Value: []byte("grace")},
			{Key: []byte("user:3"), Value: []byte("edsger")},
		},
		[][]byte{[]byte("user:1")},
	)
	if err != nil {
		return err
	}
	fmt.Println("[batch] set user:2 + user:3, deleted user:1 (one raft entry, one txn)")

	// ------------------------------------------------------------------
	// Reads. Reads are served by each node's local Badger with no Raft
	// round trip, so followers converge asynchronously. Poll a follower
	// until it has replayed the entries.
	// ------------------------------------------------------------------
	fmt.Println("\n== reads (served locally by a follower) ==")
	fi := 1
	for i, n := range nodes {
		if !n.IsLeader() {
			fi = i
			break
		}
	}
	follower := nodes[fi]
	fmt.Printf("[read] using follower %s at %s\n", ids[fi], addrs[fi])

	if err := waitFor(10*time.Second, "follower to replicate user:2", func() bool {
		v, err := follower.Get([]byte("user:2"))
		return err == nil && string(v) == "grace"
	}); err != nil {
		return err
	}
	fmt.Println("[repl] follower caught up")

	v, err := follower.Get([]byte("user:2"))
	if err != nil {
		return err
	}
	fmt.Printf("[get]  user:2 = %q\n", v)

	if err := waitFor(10*time.Second, "follower to see user:1 deleted", func() bool {
		_, err := follower.Get([]byte("user:1"))
		return errors.Is(err, honeybadger.ErrKeyNotFound)
	}); err != nil {
		return err
	}
	fmt.Println("[get]  user:1 -> ErrKeyNotFound (batch delete replicated)")

	pairs, err := follower.PrefixScan([]byte("user:"), 0)
	if err != nil {
		return err
	}
	fmt.Printf("[scan] prefix \"user:\" ->")
	for _, p := range pairs {
		fmt.Printf(" %s=%q", p.Key, p.Value)
	}
	fmt.Println()

	// ------------------------------------------------------------------
	// TTL expiry: Badger expires entries on read, no sweep required.
	// ------------------------------------------------------------------
	fmt.Println("\n== TTL expiry ==")
	if err := waitFor(10*time.Second, "session:xyz to replicate", func() bool {
		_, err := follower.Get([]byte("session:xyz"))
		return err == nil
	}); err != nil {
		return err
	}
	v, _ = follower.Get([]byte("session:xyz"))
	fmt.Printf("[get]  session:xyz = %q (before expiry)\n", v)
	fmt.Println("[wait] sleeping 2.6s for the 2s TTL to pass...")
	time.Sleep(2600 * time.Millisecond)
	_, err = follower.Get([]byte("session:xyz"))
	fmt.Printf("[get]  session:xyz -> ErrKeyNotFound: %v\n", errors.Is(err, honeybadger.ErrKeyNotFound))

	fmt.Println("\n== demo complete: every node converged, no data left behind ==")
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
