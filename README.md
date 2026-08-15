# honeybadger

An embeddable, replicated, in-process key-value store for Go. honeybadger
combines [hashicorp/raft](https://github.com/hashicorp/raft) for consensus
with [dgraph-io/badger](https://github.com/dgraph-io/badger) for local
storage.

**Core invariant:** every mutation is committed through Raft first, and only
applied to the local Badger database by the Raft FSM after the log entry is
committed. Each node keeps its own fully independent Badger database on
disk; Raft guarantees they converge to identical state. Reads are served
locally from Badger with no Raft round trip.

## Architecture

```
  Client                Leader node                     Follower nodes
    │                       │                                 │
    │  db.Set(k, v)         │                                 │
    │ ─────────────▶ gob-encode command                       │
    │                  raft.Apply ────── replicate ──────────▶│ (log append)
    │                       │◀─────────── acks ───────────────│
    │                       │                                 │
    │                entry committed (majority)               │
    │                       │                                 │
    │                  FSM.Apply ─────── committed ──────▶ FSM.Apply
    │                       │                                 │
    │                  badger.Update                      badger.Update
    │                       │                                 │
    │  error/ok ◀───────────┘                                 │
    │                                                         │
    │  db.Get(k) ──▶ local badger read txn (any node, no raft)│
```

- **`DB`** owns a Badger DB, a Raft node, raft-boltdb log/stable stores, a
  TCP transport, and a file snapshot store.
- **Writes** (`Set`, `SetWithTTL`, `Delete`, `Batch`) are gob-encoded into a
  command struct, submitted via `raft.Apply`, and executed against Badger
  inside `fsm.Apply` — one `badger.Update` transaction per log entry. The
  FSM's error surfaces to the caller through the apply future.
- **Reads** (`Get`, `View`, `PrefixScan`) go straight to the local Badger
  read transaction.
- **Snapshots** stream `badger.DB.Backup` into the Raft snapshot sink.
  `Restore` is staged: the snapshot is loaded into a fresh Badger instance
  in a temporary directory, and only on success is the live database swapped
  (guarded by a mutex, since Restore runs on a Raft goroutine). A failed
  restore leaves the old store serving reads and the error goes back to
  Raft, which retries. Raft then replays any remaining log entries.
  Snapshots live in `<DataDir>/raft/snapshots`.

## Quick start

Three nodes in one process (in production, run one node per process/host):

```go
package main

import (
	"fmt"
	"time"

	"honeybadger"
)

func main() {
	// Node 1 bootstraps the cluster.
	n1, err := honeybadger.Open(honeybadger.Config{
		NodeID: "n1", RaftBind: "127.0.0.1:7001", DataDir: "/tmp/n1", Bootstrap: true,
	})
	if err != nil { panic(err) }
	defer n1.Close()

	// Nodes 2 and 3 start un-bootstrapped and are joined through the leader.
	n2, err := honeybadger.Open(honeybadger.Config{
		NodeID: "n2", RaftBind: "127.0.0.1:7002", DataDir: "/tmp/n2",
	})
	if err != nil { panic(err) }
	defer n2.Close()

	n3, err := honeybadger.Open(honeybadger.Config{
		NodeID: "n3", RaftBind: "127.0.0.1:7003", DataDir: "/tmp/n3",
	})
	if err != nil { panic(err) }
	defer n3.Close()

	if err := n1.WaitForLeader(10 * time.Second); err != nil { panic(err) }
	if err := n1.Join("n2", "127.0.0.1:7002"); err != nil { panic(err) }
	if err := n1.Join("n3", "127.0.0.1:7003"); err != nil { panic(err) }

	// Writes go to the leader; they return once committed and applied locally.
	if err := n1.Set([]byte("hello"), []byte("world")); err != nil { panic(err) }

	// Reads are local on every node (eventually consistent on followers).
	time.Sleep(100 * time.Millisecond) // allow replication
	val, err := n3.Get([]byte("hello"))
	if err != nil { panic(err) }
	fmt.Println(string(val)) // "world"

	// Linearizable read on the leader (strictly leader-only).
	val, err = n1.GetLinearizable([]byte("hello"))
	_ = val

	// Introspection.
	fmt.Println(n1.IsLeader())      // true
	id, addr := n1.Leader()         // ("n1", "127.0.0.1:7001")
	fmt.Println(id, addr)
	nodes, _ := n1.Nodes()          // 3 voters: n1, n2, n3
	fmt.Println(len(nodes))
	fmt.Println(n1.AppliedIndex())  // last raft index applied to badger
}
```

## API summary

| Operation | Description |
|---|---|
| `Open(Config) (*DB, error)` | Open or create a node. |
| `(*DB) Close() error` | Shutdown raft, transport, stores, badger. Idempotent. |
| `Set(key, value []byte) error` | Replicated write. |
| `SetWithTTL(key, value []byte, ttl time.Duration) error` | Replicated write with expiry. |
| `Delete(key []byte) error` | Replicated delete (missing keys are not an error). |
| `Batch(sets []Pair, deletes [][]byte) error` | Atomic multi-key write: one Raft entry, one Badger txn. |
| `Get(key []byte) ([]byte, error)` | Local read; `ErrKeyNotFound` if missing/expired. |
| `GetConsistent(key []byte) ([]byte, error)` | Barrier + Get on the leader; plain local Get on followers. |
| `View(fn func(*badger.Txn) error) error` | Raw Badger read-transaction escape hatch. |
| `PrefixScan(prefix []byte, limit int) ([]Pair, error)` | Local prefix scan in key order. |
| `GetLinearizable(key []byte) ([]byte, error)` | Strictly leader-only Barrier + Get; `ErrNotLeader` on followers. |
| `Barrier(timeout time.Duration) error` | Wait until all outstanding entries are applied (leader only). |
| `Join(nodeID, raftAddr string) error` | Add a voter (leader only). |
| `Remove(nodeID string) error` | Remove a server (leader only). |
| `Snapshot() error` | Force a local Raft snapshot + log compaction (any node). |
| `SetString(k, v string) error` / `GetString(k string) (string, error)` / `DeleteString(k string) error` | String conveniences over Set/Get/Delete. |
| `IsLeader() bool` / `Leader() (id, addr string)` / `State() string` | Leadership introspection. |
| `ID() string` / `Addr() string` | This node's ID and Raft address. |
| `Nodes() ([]Node, error)` | Cluster configuration (`Node{ID, Addr, Suffrage}`); works on followers too. |
| `AppliedIndex() uint64` | Last Raft log index applied to local Badger. |
| `Stats() map[string]string` | Raft stats + `honeybadger_applied_index`. |
| `WaitForLeader(timeout time.Duration) error` | Block until a leader is known. |

Errors: `ErrKeyNotFound`, `ErrNotLeader`, `ErrClosed` (any operation after
`Close`). Every not-leader failure is a typed `*NotLeaderError` carrying the
leader's ID and address when known — `errors.Is(err, ErrNotLeader)` always
matches, and `errors.As` extracts the details:

```go
_, err := follower.GetLinearizable([]byte("k"))
var nl *honeybadger.NotLeaderError
if errors.As(err, &nl) {
	fmt.Printf("redirect to leader %s at %s\n", nl.LeaderID, nl.LeaderAddr)
}
```

## Consistency model

- **Writes are linearizable.** A write returns only after its log entry is
  committed by a Raft majority and applied to the leader's Badger instance.
  Writes on a follower fail fast with `ErrNotLeader`.
- **Reads are local and eventually consistent.** `Get`, `View`, and
  `PrefixScan` read the node's own Badger database with no Raft round trip;
  a follower may briefly serve stale data while it catches up.
- **Linearizable reads** are available on the leader via `Barrier` (waits
  until all previously committed entries are applied) or the
  `GetLinearizable` convenience, which is strictly leader-only and fails
  with `ErrNotLeader` on followers. `GetConsistent` is the lenient variant:
  it barriers on the leader but deliberately falls back to a plain local
  `Get` on followers — inspect the `*NotLeaderError` from `GetLinearizable`
  and re-issue against the leader if you need strict reads.
- **TTLs** are converted to an absolute expiry timestamp once, at write
  submission time on the leader, and replicated with the command. Every node
  therefore applies the identical expiry, log replay after a restart is
  idempotent (expired keys never resurrect, live keys never get extended),
  and snapshots preserve expiries verbatim. Badger tracks expirations with
  one-second granularity, so sub-second TTLs may expire almost immediately.
  Expired keys behave exactly like missing keys on read (`ErrKeyNotFound`).
- **Batch** is atomic across nodes: all sets and deletes travel in a single
  Raft log entry and are applied in a single Badger transaction, so no node
  ever observes a partial batch. Very large batches can exceed Badger's max
  transaction size; split them if needed.

## Testing

```
go test ./... -race
```

The suite spins up real single-node and three-node clusters on 127.0.0.1
with dynamically allocated ports, and covers replication, deletes, batches,
TTL expiry, leader-only write enforcement, consistent reads, restarts
(durability of both the Badger data and the Raft log), and snapshot install
into a lagging follower.
