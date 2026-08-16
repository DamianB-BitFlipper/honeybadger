// Package honeybadger_test holds runnable examples and consumer-style
// tests for honeybadger. The examples below use a single-node cluster so
// their output is deterministic; see examples/three-node for a full
// multi-node program.
package honeybadger_test

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"honeybadger"
)

// exampleNode opens a bootstrapped single-node cluster in a fresh temp
// directory on a free loopback port and returns the DB plus a cleanup
// function. NewCluster bootstraps AND waits for the first election, so the
// node is usable the moment Open returns. Examples panic on error for
// brevity; real programs should handle every error.
func exampleNode() (*honeybadger.DB, func()) {
	dir, err := os.MkdirTemp("", "honeybadger-example-*")
	if err != nil {
		panic(err)
	}
	db, err := honeybadger.Open(honeybadger.Config{
		NodeID:   "example-node",
		RaftBind: fmt.Sprintf("127.0.0.1:%d", mustFreeTCPPort()),
		DataDir:  dir,
	}, honeybadger.NewCluster())
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// freeTCPPort asks the OS for a spare TCP port on the loopback interface.
func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// mustFreeTCPPort is freeTCPPort for examples: it panics on failure.
func mustFreeTCPPort() int {
	port, err := freeTCPPort()
	if err != nil {
		panic(err)
	}
	return port
}

// Example demonstrates the smallest useful honeybadger program: open a
// single-node cluster, write a key through Raft, and read it back.
func Example() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.Set("hello", "world"); err != nil {
		panic(err)
	}
	value, err := db.Get("hello")
	if err != nil {
		panic(err)
	}
	fmt.Printf("hello = %s\n", value)

	// Output:
	// hello = world
}

// ExampleDB_Get shows that Get reports missing keys with an error that
// wraps ErrKeyNotFound, so errors.Is works.
func ExampleDB_Get() {
	db, cleanup := exampleNode()
	defer cleanup()

	_, err := db.Get("missing")
	fmt.Println("missing:", errors.Is(err, honeybadger.ErrKeyNotFound))

	if err := db.Set("lang", "go"); err != nil {
		panic(err)
	}
	value, err := db.Get("lang")
	if err != nil {
		panic(err)
	}
	fmt.Printf("lang = %s\n", value)

	// Output:
	// missing: true
	// lang = go
}

// ExampleDB_Batch shows an atomic multi-key write: every mutation lands in
// one Raft entry and one Badger transaction.
func ExampleDB_Batch() {
	db, cleanup := exampleNode()
	defer cleanup()

	err := db.Batch(
		honeybadger.SetOp("user:1", "ada"),
		honeybadger.SetOp("user:2", "grace"),
		honeybadger.SetOp("user:3", "edsger"),
	)
	if err != nil {
		panic(err)
	}

	entries, err := db.ScanPrefixBytes([]byte("user:"), honeybadger.ScanOptions{Limit: 2})
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		fmt.Printf("%s = %s\n", e.Key, e.Value)
	}

	// Output:
	// user:1 = ada
	// user:2 = grace
}

// ExampleDB_Delete shows that deleting a key makes subsequent Gets behave
// exactly as if the key never existed. Deleting a missing key is not an
// error.
func ExampleDB_Delete() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.Set("temp", "x"); err != nil {
		panic(err)
	}
	if err := db.Delete("temp"); err != nil {
		panic(err)
	}
	_, err := db.Get("temp")
	fmt.Println(errors.Is(err, honeybadger.ErrKeyNotFound))

	// Output:
	// true
}

// ExampleDB_Set_withTTL shows a key that expires: it is readable right
// after the write and behaves like a missing key once the TTL has passed
// (Badger expires entries on read; no sweeper is involved). A 2s TTL with
// a 2.6s wait keeps the example deterministic because Badger stores expiry
// with one-second granularity.
func ExampleDB_Set_withTTL() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.Set("session", "token", honeybadger.WithTTL(2*time.Second)); err != nil {
		panic(err)
	}

	value, err := db.Get("session")
	if err != nil {
		panic(err)
	}
	fmt.Printf("before expiry: %s\n", value)

	time.Sleep(2600 * time.Millisecond)
	_, err = db.Get("session")
	fmt.Println("after expiry, not found:", errors.Is(err, honeybadger.ErrKeyNotFound))

	// Output:
	// before expiry: token
	// after expiry, not found: true
}

// ExampleDB_GetWithOptions shows how to opt out of the strict default
// read: ReadLocal serves from this node's local store, on any node, with
// no Raft round trip — at the price of eventual consistency.
func ExampleDB_GetWithOptions() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.Set("config:feature", "on"); err != nil {
		panic(err)
	}

	// The strict default: linearizable, leader-only.
	value, err := db.Get("config:feature")
	if err != nil {
		panic(err)
	}
	fmt.Printf("strict: %s\n", value)

	// The deliberate opt-out: local read (works on followers too).
	value, err = db.GetWithOptions("config:feature", honeybadger.ReadOptions{Mode: honeybadger.ReadLocal})
	if err != nil {
		panic(err)
	}
	fmt.Printf("local: %s\n", value)

	// Output:
	// strict: on
	// local: on
}
