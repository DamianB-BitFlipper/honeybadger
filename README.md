# honeybadger

An embeddable, replicated, in-process key-value store for Go. honeybadger
combines [hashicorp/raft](https://github.com/hashicorp/raft) for consensus
with [dgraph-io/badger](https://github.com/dgraph-io/badger) for local
storage.

**Core invariant:** every mutation is committed through Raft first, and only
applied to the local Badger database by the Raft FSM after the log entry is
committed. Each node keeps its own fully independent Badger database on
disk; Raft guarantees they converge to identical state.

The API comes in **two tiers**:

- **Tier 1 — a replicated `map[string]string`.** Open the first node with
  `NewCluster()`, then `Set`, `Get`, `Delete`, `Batch` with string keys and
  values. Every default is safe: writes commit through a Raft majority
  before returning, and `Get` is strictly consistent (leader-only, behind a
  Raft barrier — it fails loudly on followers instead of silently serving
  stale data).
- **Tier 2 — advanced control.** Byte-slice keys and values, TTLs via
  `WithTTL`, an explicit `ReadOptions` model (`ReadLocal` opts into
  eventually consistent reads on any node), prefix scans with guarded
  limits, raw Badger read transactions, cluster membership, and typed
  status introspection.

## Quick start (Tier 1)

The first node of a cluster is opened with `NewCluster()`: it bootstraps
**and** waits for the first election, so it is immediately usable — no
separate leader-wait dance:

```go
db, err := honeybadger.Open(honeybadger.Config{
    NodeID: "n1", RaftBind: "127.0.0.1:7001", DataDir: "/var/lib/myapp/n1",
}, honeybadger.NewCluster())
if err != nil {
    log.Fatal(err)
}
defer db.Close()

if err := db.Set("hello", "world"); err != nil { // committed by a majority
    log.Fatal(err)
}
val, err := db.Get("hello") // strictly consistent read-your-write
if err != nil {
    log.Fatal(err)
}
fmt.Println(val) // world
```

Additional nodes are opened **plain** — `Open` without `NewCluster()` never
forms a cluster, so a misconfigured second node can never split-brain — and
are added through the leader:

```go
n2, err := honeybadger.Open(honeybadger.Config{
    NodeID: "n2", RaftBind: "127.0.0.1:7002", DataDir: "/var/lib/myapp/n2",
})
if err != nil {
    log.Fatal(err)
}
defer n2.Close()
err = db.AddVoter(honeybadger.Node{ID: "n2", RaftAddr: "127.0.0.1:7002"})
if err != nil {
    log.Fatal(err)
}
```

`AddVoter` is called **on the leader, on behalf of the joining node**. Its
success confirms the membership change was committed — not that the new
voter has caught up; it replays missed entries (or receives a snapshot)
asynchronously. When you need it to serve current data, poll an
application-level readiness signal (e.g. a local read of a key you just
wrote); comparing `Status().AppliedIndex` against the leader's is a
progress hint only.

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
    │  db.Get(k) ──▶ leader only: barrier + local read        │
    │  ReadLocal ──▶ local badger read txn (any node, no raft)│
```

- **`DB`** owns a Badger DB, a Raft node, raft-boltdb log/stable stores, a
  TCP transport, and a file snapshot store.
- **Writes** (`Set`/`SetBytes`, `Delete`/`DeleteBytes`, `Batch`) are
  gob-encoded into a command struct, submitted via `raft.Apply`, and
  executed against Badger inside `fsm.Apply` — one `badger.Update`
  transaction per log entry. The FSM's error surfaces to the caller through
  the apply future. Arguments (empty keys, non-positive TTLs, duplicate
  batch keys, negative scan limits) are rejected with `ErrInvalidArgument`
  **before** any Raft entry is submitted.
- **Reads** (`Get`, `GetBytes`, `ScanPrefixBytes`, `ViewBadger`) are
  governed by one `ReadOptions` model: linearizable reads barrier on the
  leader; `ReadLocal` reads go straight to the local Badger read
  transaction.
- **Snapshots** stream `badger.DB.Backup` into the Raft snapshot sink.
  `Restore` is staged: the snapshot is loaded into a fresh Badger instance
  in a temporary directory, and only on success is the live database swapped
  (guarded by a mutex, since Restore runs on a Raft goroutine). A staging
  failure leaves the old store untouched; a swap failure rolls back only
  best-effort, and the error marks the restore as failed for the invoking
  Raft path. Raft then replays any remaining log entries. Snapshots live in
  `<DataDir>/raft/snapshots`.

## API summary

### Tier 1: the simple surface

| Operation | Description |
|---|---|
| `Open(Config, ...OpenOption) (*DB, error)` | Open a node. Plain `Open` never bootstraps. |
| `NewCluster() OpenOption` | First node only: bootstrap **and** wait for the first election. |
| `(*DB) Close() error` | Shutdown Raft, transport, stores, Badger. Idempotent. |
| `Set(key, value string, ...SetOption) error` | Replicated write. Persists forever unless `WithTTL`. |
| `Get(key string) (string, error)` | **Strictly consistent** read: leader-only, barrier first; `*NotLeaderError` on followers. |
| `Delete(key string) error` | Replicated delete (missing keys are not an error). |
| `Batch(...Mutation) error` | Atomic multi-key write: one Raft entry, one Badger txn. Empty batch = no-op. |
| `SetOp(key, value string, ...SetOption) Mutation` / `DeleteOp(key string) Mutation` | Batch mutations (sealed). |
| `WithTTL(ttl) SetOption` | Per-write expiry (must be positive). |

### Tier 2: advanced control

| Operation | Description |
|---|---|
| `GetWithOptions(key string, ro ReadOptions) (string, error)` | String read with explicit consistency/timeout. |
| `GetBytes(key []byte, ro ReadOptions) ([]byte, error)` | Byte read with explicit consistency; returns a copy. |
| `SetBytes(key, value []byte, ...SetOption) error` / `DeleteBytes(key []byte) error` | Byte-slice writes. |
| `SetBytesOp(key, value []byte, ...SetOption) Mutation` / `DeleteBytesOp(key []byte) Mutation` | Byte-slice batch mutations (copy their inputs). |
| `ScanPrefixBytes(prefix []byte, opts ScanOptions) ([]Entry, error)` | Prefix scan in key order. `Limit` 0 = 100, negative = invalid, `Unlimited: true` = no cap. |
| `ViewBadger(ro ReadOptions, fn func(*badger.Txn) error) error` | Raw Badger read-transaction escape hatch (read-only; txn/items must not escape). |
| `Barrier(timeout time.Duration) error` | Wait until all outstanding entries are applied (leader only). |
| `AddVoter(Node) error` | Add a voter — **on the leader**, for the joining node. |
| `RemoveNode(id string) error` | Remove a server (leader only). |
| `Members() ([]Node, error)` | Cluster configuration with typed `NodeRole`; works on followers too. |
| `Status() (Status, error)` | Typed snapshot: local `Node`, `State`, `Leader *Node`, `AppliedIndex`. |
| `WaitForLeader(timeout) (Node, error)` | Block until a leader is known; returns it. Timeout wraps `ErrNoLeader`. |
| `Snapshot() error` | Force a local Raft snapshot + log compaction (any node). |
| `RawRaftStats() map[string]string` | Raw Raft stats + `honeybadger_applied_index`. Callable even after `Close`. |

`ReadOptions{Mode, Timeout}` governs every read uniformly. The zero value
is the safe default: `ReadLinearizable` (leader-only, behind a barrier)
with the configured apply timeout. `ReadLocal` is the explicit opt-in to
eventually consistent reads served by any node from its local store:

```go
// Strict (default): fails with *NotLeaderError on followers.
val, err := db.Get("config:feature")

// Deliberate local read, e.g. on a follower that may briefly lag:
val, err = db.GetWithOptions("config:feature", honeybadger.ReadOptions{Mode: honeybadger.ReadLocal})
```

### Errors

Sentinels: `ErrKeyNotFound`, `ErrNotLeader`, `ErrClosed`, `ErrNoLeader`
(leader-wait timeouts), `ErrInvalidArgument` (pre-submission validation).
They classify the common operational failures so `errors.Is` works; errors
from the storage engine, transport, snapshots, or your own callbacks may be
returned directly or wrapped with package context (via `%w`) — never
remapped to a sentinel, always preserving their original identity for
`errors.Is`/`errors.As`.
Every not-leader failure is a typed `*NotLeaderError` carrying the leader's
ID and Raft address when known — `errors.Is(err, ErrNotLeader)` always
matches, and `errors.As` extracts the details (the address is a Raft
transport address: a routing hint, not necessarily an application
endpoint):

```go
_, err := follower.Get("k")
var nl *honeybadger.NotLeaderError
if errors.As(err, &nl) {
    fmt.Printf("redirect to leader %s at %s\n", nl.LeaderID, nl.LeaderAddr)
}
```

After `Close`, every operational method fails with `ErrClosed`. The single
documented exception is `RawRaftStats`, a passive snapshot that keeps
returning the final statistics.

## Consistency model

- **Writes are linearizable.** A write returns only after its log entry is
  committed by a Raft majority and applied to the leader's Badger instance.
  Writes on a follower fail fast with `ErrNotLeader`.
- **Reads are strictly consistent by default — deliberately.** `Get`
  (and the zero `ReadOptions` on every other read API) runs on the leader
  behind a Raft barrier, so all previously committed writes are visible.
  On a follower it fails loudly with a typed `*NotLeaderError` instead of
  silently downgrading to possibly stale data. This is a product decision:
  a generic read should never change its correctness guarantees just
  because the code moved between nodes.
- **`ReadLocal` is the explicit opt-out.** When staleness is acceptable —
  caches, follower reads, convergence polling — pass
  `ReadOptions{Mode: ReadLocal}` to `GetWithOptions`, `GetBytes`,
  `ScanPrefixBytes`, or `ViewBadger`. It is served from the node's local
  Badger with no Raft round trip and works on any node.
- **TTLs** are converted to an absolute expiry timestamp once, at write
  submission time on the leader, and replicated with the command. Every
  node therefore applies the identical expiry, log replay after a restart
  is idempotent (expired keys never resurrect, live keys never get
  extended), and snapshots preserve expiries verbatim. TTLs must be
  positive. Badger tracks expirations with one-second granularity, so
  sub-second TTLs may expire almost immediately. Expired keys behave
  exactly like missing keys on read (`ErrKeyNotFound`).
- **Snapshot/restore convergence rests on blind writes.** Raft labels a
  snapshot with its FSM progression index I, but the Badger backup may run
  later and capture a transactionally complete prefix of command effects
  through some J ≥ I, while a restore still replays log[I+1..]. Convergence
  holds only because every command is a blind write that never reads prior
  state and is idempotent under duplication (absolute expiry timestamps
  included): replaying the overlapping entries converges to the same final
  state.
- **Batch** is atomic across nodes: all mutations travel in a single Raft
  log entry and are applied in a single Badger transaction, so no node ever
  observes a partial batch. A key may appear only once per batch. Very
  large batches can exceed Badger's max transaction size; split them if
  needed.
- **Scans are bounded by default.** `ScanPrefixBytes` returns at most 100
  entries unless you pass an explicit `Limit` — or `Unlimited: true`, which
  must be set deliberately. Results are in key order and silently
  truncated at the limit.

## Configuration

`Config` carries only the three lifecycle fields every user must set
(`NodeID`, `RaftBind`, `DataDir`). All tuning lives under
`Config.Advanced`:

```go
type AdvancedConfig struct {
    ApplyTimeout      time.Duration   // default 10s
    SnapshotThreshold uint64          // default 8192
    RaftAdvertise     string          // default RaftBind; set when binding 0.0.0.0 / behind NAT
    BadgerOptions     *badger.Options // raw, unsafe escape hatch (see godoc)
    LogOutput         io.Writer       // default io.Discard
}
```

## Testing

```
go test ./... -race
```

The suite spins up real single-node and three-node clusters on 127.0.0.1
with dynamically allocated ports, and covers replication, deletes, batches,
TTL expiry, leader-only write enforcement, strict default reads and the
`ReadLocal` opt-in, argument validation, scan limit rules, restarts
(durability of both the Badger data and the Raft log), snapshot install
into a lagging follower, and the `NewCluster` bootstrap/readiness contract.
