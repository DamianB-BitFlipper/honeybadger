package honeybadger

import (
	"fmt"
	"net"
	"testing"
	"time"
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
// registers its cleanup with t. newCluster must be true for exactly the
// first node of a new cluster; plain Open never bootstraps.
func testNode(t *testing.T, port int, newCluster bool) *DB {
	t.Helper()
	cfg := Config{
		NodeID:   fmt.Sprintf("node-%d", port),
		RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
		DataDir:  t.TempDir(),
	}
	var db *DB
	var err error
	if newCluster {
		db, err = Open(cfg, NewCluster())
	} else {
		db, err = Open(cfg)
	}
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

// getLocal reads key from this node's local store (eventual consistency):
// the read pattern follower tests use while waiting for convergence.
func getLocal(db *DB, key string) (string, error) {
	return db.GetWithOptions(key, ReadOptions{Mode: ReadLocal})
}
