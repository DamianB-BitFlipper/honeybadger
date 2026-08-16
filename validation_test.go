package honeybadger

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// TestValidation verifies that invalid arguments are rejected with
// ErrInvalidArgument BEFORE any Raft entry is submitted.
func TestValidation(t *testing.T) {
	db := testNode(t, freePort(t), true)

	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	appliedBefore := st.AppliedIndex

	cases := map[string]func() error{
		"empty key Set":         func() error { return db.Set("", "v") },
		"empty key SetBytes":    func() error { return db.SetBytes(nil, []byte("v")) },
		"empty key Delete":      func() error { return db.Delete("") },
		"empty key DeleteBytes": func() error { return db.DeleteBytes([]byte{}) },
		"empty key Get":         func() error { _, err := db.Get(""); return err },
		"empty key GetBytes":    func() error { _, err := db.GetBytes(nil, ReadOptions{}); return err },
		"zero TTL":              func() error { return db.Set("k", "v", WithTTL(0)) },
		"negative TTL":          func() error { return db.Set("k", "v", WithTTL(-time.Second)) },
		"unknown set option": func() error {
			return db.Set("k", "v", nil)
		},
		"batch empty key":   func() error { return db.Batch(SetOp("", "v")) },
		"batch zero TTL":    func() error { return db.Batch(SetOp("k", "v", WithTTL(0))) },
		"batch dup set/set": func() error { return db.Batch(SetOp("dup", "1"), SetOp("dup", "2")) },
		"batch dup set/del": func() error { return db.Batch(SetOp("dup", "1"), DeleteOp("dup")) },
		"batch dup del/del": func() error { return db.Batch(DeleteOp("dup"), DeleteBytesOp([]byte("dup"))) },
		"batch zero mutation": func() error {
			return db.Batch(Mutation{})
		},
		"negative scan limit": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Limit: -1})
			return err
		},
		"unknown read mode Get": func() error {
			_, err := db.GetWithOptions("k", ReadOptions{Mode: ReadMode(42)})
			return err
		},
		"unknown read mode scan": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Read: ReadOptions{Mode: ReadMode(42)}})
			return err
		},
		"nil ViewBadger callback": func() error { return db.ViewBadger(ReadOptions{}, nil) },
		"AddVoter empty ID":       func() error { return db.AddVoter(Node{RaftAddr: "127.0.0.1:1"}) },
		"AddVoter empty addr":     func() error { return db.AddVoter(Node{ID: "x"}) },
		"AddVoter contradictory role": func() error {
			return db.AddVoter(Node{ID: "x", RaftAddr: "127.0.0.1:1", Role: RoleNonvoter})
		},
		"RemoveNode empty ID": func() error { return db.RemoveNode("") },
		"negative read timeout (linearizable)": func() error {
			_, err := db.GetWithOptions("k", ReadOptions{Timeout: -time.Second})
			return err
		},
		"negative read timeout (local scan)": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Read: ReadOptions{Mode: ReadLocal, Timeout: -time.Second}})
			return err
		},
	}
	for name, fn := range cases {
		if err := fn(); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s = %v, want ErrInvalidArgument", name, err)
		}
	}

	// An empty batch is a documented no-op, not an error.
	if err := db.Batch(); err != nil {
		t.Fatalf("empty Batch() = %v, want nil", err)
	}

	// None of the rejections above may have submitted a Raft entry.
	st, err = db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.AppliedIndex != appliedBefore {
		t.Fatalf("validation failures submitted raft entries: AppliedIndex %d -> %d",
			appliedBefore, st.AppliedIndex)
	}
}

// TestClosedErrors verifies every public operation fails with ErrClosed
// after Close, that Close stays idempotent, and that RawRaftStats is the
// documented exception that remains callable.
func TestClosedErrors(t *testing.T) {
	port := freePort(t)
	db := testNode(t, port, true)
	if err := db.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	cases := map[string]func() error{
		"Set":            func() error { return db.Set("k", "v") },
		"SetBytes":       func() error { return db.SetBytes([]byte("k"), []byte("v")) },
		"Delete":         func() error { return db.Delete("k") },
		"DeleteBytes":    func() error { return db.DeleteBytes([]byte("k")) },
		"Batch":          func() error { return db.Batch(SetOp("k", "v")) },
		"Batch (empty)":  func() error { return db.Batch() },
		"AddVoter":       func() error { return db.AddVoter(Node{ID: "x", RaftAddr: "127.0.0.1:1"}) },
		"RemoveNode":     func() error { return db.RemoveNode("x") },
		"Barrier":        func() error { return db.Barrier(time.Second) },
		"Snapshot":       func() error { return db.Snapshot() },
		"Get":            func() error { _, err := db.Get("k"); return err },
		"GetWithOptions": func() error { _, err := db.GetWithOptions("k", ReadOptions{Mode: ReadLocal}); return err },
		"GetBytes":       func() error { _, err := db.GetBytes([]byte("k"), ReadOptions{Mode: ReadLocal}); return err },
		"ScanPrefixBytes": func() error {
			_, err := db.ScanPrefixBytes([]byte("k"), ScanOptions{Read: ReadOptions{Mode: ReadLocal}})
			return err
		},
		"ViewBadger": func() error {
			return db.ViewBadger(ReadOptions{Mode: ReadLocal}, func(*badger.Txn) error { return nil })
		},
		"Members":       func() error { _, err := db.Members(); return err },
		"Status":        func() error { _, err := db.Status(); return err },
		"WaitForLeader": func() error { _, err := db.WaitForLeader(time.Second); return err },
	}
	for name, fn := range cases {
		if err := fn(); !errors.Is(err, ErrClosed) {
			t.Fatalf("%s after Close = %v, want ErrClosed", name, err)
		}
	}

	// RawRaftStats is the documented passive exception: it keeps returning
	// the final statistics, including the shutdown state.
	stats := db.RawRaftStats()
	if stats["state"] != "Shutdown" {
		t.Fatalf("RawRaftStats()[state] after Close = %q, want Shutdown", stats["state"])
	}
}

// TestOpenValidation verifies Open-level option validation.
func TestOpenValidation(t *testing.T) {
	port := freePort(t)
	cfg := func() Config {
		return Config{
			NodeID:   fmt.Sprintf("node-%d", port),
			RaftBind: fmt.Sprintf("127.0.0.1:%d", port),
			DataDir:  t.TempDir(),
		}
	}

	// Unknown (nil) option.
	if _, err := Open(cfg(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open(nil option) = %v, want ErrInvalidArgument", err)
	}
	// Duplicate NewCluster.
	if _, err := Open(cfg(), NewCluster(), NewCluster()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open(NewCluster, NewCluster) = %v, want ErrInvalidArgument", err)
	}
}
