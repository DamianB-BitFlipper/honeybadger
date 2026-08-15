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
// directory on a free loopback port, waits for it to elect itself leader,
// and returns the DB plus a cleanup function. Examples panic on error for
// brevity; real programs should handle every error.
func exampleNode() (*honeybadger.DB, func()) {
	dir, err := os.MkdirTemp("", "honeybadger-example-*")
	if err != nil {
		panic(err)
	}
	db, err := honeybadger.Open(honeybadger.Config{
		NodeID:    "example-node",
		RaftBind:  fmt.Sprintf("127.0.0.1:%d", mustFreeTCPPort()),
		DataDir:   dir,
		Bootstrap: true,
	})
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	if err := db.WaitForLeader(10 * time.Second); err != nil {
		db.Close()
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

// mustFreeTCPPort is freeTCPPort for examples, which cannot fail tests.
func mustFreeTCPPort() int {
	port, err := freeTCPPort()
	if err != nil {
		panic(err)
	}
	return port
}

// Example demonstrates the smallest useful honeybadger program: open a
// node, write a key through Raft, and read it back from local storage.
func Example() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.Set([]byte("hello"), []byte("world")); err != nil {
		panic(err)
	}
	value, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("hello = %s\n", value)

	// Output:
	// hello = world
}

// ExampleDB_Get shows that Get returns a value copy and reports missing
// keys with an error that wraps ErrKeyNotFound, so errors.Is works.
func ExampleDB_Get() {
	db, cleanup := exampleNode()
	defer cleanup()

	_, err := db.Get([]byte("missing"))
	fmt.Println("missing:", errors.Is(err, honeybadger.ErrKeyNotFound))

	if err := db.Set([]byte("lang"), []byte("go")); err != nil {
		panic(err)
	}
	value, err := db.Get([]byte("lang"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("lang = %s\n", value)

	// Output:
	// missing: true
	// lang = go
}

// ExampleDB_Batch shows an atomic multi-key write: every set and delete
// lands in one Raft entry and one Badger transaction.
func ExampleDB_Batch() {
	db, cleanup := exampleNode()
	defer cleanup()

	err := db.Batch([]honeybadger.Pair{
		{Key: []byte("user:1"), Value: []byte("ada")},
		{Key: []byte("user:2"), Value: []byte("grace")},
		{Key: []byte("user:3"), Value: []byte("edsger")},
	}, nil)
	if err != nil {
		panic(err)
	}

	pairs, err := db.PrefixScan([]byte("user:"), 2)
	if err != nil {
		panic(err)
	}
	for _, p := range pairs {
		fmt.Printf("%s = %s\n", p.Key, p.Value)
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

	if err := db.Set([]byte("temp"), []byte("x")); err != nil {
		panic(err)
	}
	if err := db.Delete([]byte("temp")); err != nil {
		panic(err)
	}
	_, err := db.Get([]byte("temp"))
	fmt.Println(errors.Is(err, honeybadger.ErrKeyNotFound))

	// Output:
	// true
}

// ExampleDB_SetWithTTL shows a key that expires: it is readable right
// after the write and behaves like a missing key once the TTL has passed
// (Badger expires entries on read; no sweeper is involved). A 2s TTL with
// a 2.6s wait keeps the example deterministic because Badger stores expiry
// with one-second granularity.
func ExampleDB_SetWithTTL() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.SetWithTTL([]byte("session"), []byte("token"), 2*time.Second); err != nil {
		panic(err)
	}

	value, err := db.Get([]byte("session"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("before expiry: %s\n", value)

	time.Sleep(2600 * time.Millisecond)
	_, err = db.Get([]byte("session"))
	fmt.Println("after expiry, not found:", errors.Is(err, honeybadger.ErrKeyNotFound))

	// Output:
	// before expiry: token
	// after expiry, not found: true
}

// ExampleDB_GetConsistent shows a linearizable read: on the leader,
// GetConsistent first runs a Raft barrier so all previously committed
// writes are visible. On followers it falls back to a plain local read.
func ExampleDB_GetConsistent() {
	db, cleanup := exampleNode()
	defer cleanup()

	if err := db.Set([]byte("config:feature"), []byte("on")); err != nil {
		panic(err)
	}
	value, err := db.GetConsistent([]byte("config:feature"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("config:feature = %s\n", value)

	// Output:
	// config:feature = on
}
