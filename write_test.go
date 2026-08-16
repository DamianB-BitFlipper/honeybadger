package honeybadger

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestSetWithTTL(t *testing.T) {
	db := testNode(t, freePort(t), true)

	// Badger stores expirations with second granularity, so a 2s TTL lives
	// at least ~1s and at most 2s.
	if err := db.Set("ttl-key", "ttl-val", WithTTL(2*time.Second)); err != nil {
		t.Fatalf("Set WithTTL: %v", err)
	}
	val, err := db.Get("ttl-key")
	if err != nil || val != "ttl-val" {
		t.Fatalf("Get(ttl-key) = %q, %v; want ttl-val", val, err)
	}

	time.Sleep(3 * time.Second)
	if _, err := db.Get("ttl-key"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(ttl-key) after TTL expiry = %v, want ErrKeyNotFound", err)
	}
}

// TestTTLNoResurrectAfterRestart is the regression test for TTL keys
// resurrecting when Raft replays log entries after a restart: the command
// replicates an absolute expiry stamped once at write submission, so
// re-apply is idempotent.
func TestTTLNoResurrectAfterRestart(t *testing.T) {
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
	if err := db.Set("durable", "stays"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set("ttl", "goes", WithTTL(2*time.Second)); err != nil {
		t.Fatalf("Set WithTTL: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := getLocal(db, "ttl")
		return errors.Is(err, ErrKeyNotFound)
	}, "ttl key to expire")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open: Raft replays the log through the FSM. The replayed TTL
	// command carries its original (now past) absolute expiry, so the key
	// must stay gone — before, during, and after the replay.
	db2, err := Open(cfg, NewCluster())
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	for i := 0; i < 20; i++ {
		if _, err := db2.Get("ttl"); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("ttl key resurrected after restart (check %d): %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if val, err := db2.Get("durable"); err != nil || val != "stays" {
		t.Fatalf("durable key after restart = %q, %v; want stays", val, err)
	}
}

// TestBytesAPI exercises the explicit byte-slice API surface.
func TestBytesAPI(t *testing.T) {
	db := testNode(t, freePort(t), true)

	// The zero ReadOptions is linearizable; on the leader it just works.
	if err := db.SetBytes([]byte("bkey"), []byte("bval")); err != nil {
		t.Fatalf("SetBytes: %v", err)
	}
	val, err := db.GetBytes([]byte("bkey"), ReadOptions{})
	if err != nil || string(val) != "bval" {
		t.Fatalf("GetBytes(bkey) = %q, %v; want bval", val, err)
	}
	// The returned slice must be a copy that stays valid.
	if err := db.SetBytes([]byte("bkey"), []byte("zzzz")); err != nil {
		t.Fatalf("SetBytes overwrite: %v", err)
	}
	if string(val) != "bval" {
		t.Fatalf("returned slice mutated by overwrite: %q", val)
	}
	if _, err := db.GetBytes([]byte("missing"), ReadOptions{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetBytes(missing) = %v, want ErrKeyNotFound", err)
	}
	if err := db.DeleteBytes([]byte("bkey")); err != nil {
		t.Fatalf("DeleteBytes: %v", err)
	}
	if _, err := db.GetBytes([]byte("bkey"), ReadOptions{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetBytes(bkey) after DeleteBytes = %v, want ErrKeyNotFound", err)
	}

	// Byte mutations in a batch, with a TTL on one.
	err = db.Batch(
		SetBytesOp([]byte("bb/1"), []byte("1")),
		SetBytesOp([]byte("bb/2"), []byte("2"), WithTTL(time.Hour)),
	)
	if err != nil {
		t.Fatalf("Batch bytes: %v", err)
	}
	if err := db.Batch(DeleteBytesOp([]byte("bb/1"))); err != nil {
		t.Fatalf("Batch delete bytes: %v", err)
	}
	if _, err := db.GetBytes([]byte("bb/1"), ReadOptions{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetBytes(bb/1) after batch delete = %v, want ErrKeyNotFound", err)
	}
	if val, err := db.GetBytes([]byte("bb/2"), ReadOptions{}); err != nil || string(val) != "2" {
		t.Fatalf("GetBytes(bb/2) = %q, %v; want 2", val, err)
	}
}

// TestMutationImmutable verifies a Mutation is frozen at construction:
// mutating the source key, value, or option slice afterwards cannot change
// what Batch applies (and cannot race a concurrent Batch).
func TestMutationImmutable(t *testing.T) {
	db := testNode(t, freePort(t), true)

	key := []byte("immut/key")
	value := []byte("original")
	opts := []SetOption{WithTTL(time.Hour)}
	m := SetBytesOp(key, value, opts...)

	// Mutate every source after construction.
	key[0] = 'X'
	value[0] = 'X'
	opts[0] = WithTTL(0) // would fail validation if it leaked into m

	if err := db.Batch(m); err != nil {
		t.Fatalf("Batch with mutated sources: %v", err)
	}
	got, err := db.GetBytes([]byte("immut/key"), ReadOptions{})
	if err != nil || string(got) != "original" {
		t.Fatalf("GetBytes(immut/key) = %q, %v; want original", got, err)
	}
	if _, err := db.GetBytes([]byte("Xmmut/key"), ReadOptions{Mode: ReadLocal}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("mutated key was written: %v", err)
	}
	// The original ~1h TTL must have been applied, not the mutated zero.
	err = db.ViewBadger(ReadOptions{}, func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("immut/key"))
		if err != nil {
			return err
		}
		exp := time.Unix(int64(item.ExpiresAt()), 0)
		if d := time.Until(exp); d < 55*time.Minute || d > time.Hour {
			return fmt.Errorf("ExpiresAt = %v (%s from now), want ~1h", exp, d)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("TTL check: %v", err)
	}
}
