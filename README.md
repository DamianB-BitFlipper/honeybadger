# honeybadger

[![CI](https://github.com/DamianB-BitFlipper/honeybadger/actions/workflows/ci.yml/badge.svg)](https://github.com/DamianB-BitFlipper/honeybadger/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/DamianB-BitFlipper/honeybadger.svg)](https://pkg.go.dev/github.com/DamianB-BitFlipper/honeybadger)

Honeybadger is an embeddable, replicated, in-process key-value store for Go.
It combines [Hashicorp Raft](https://github.com/hashicorp/raft) for consensus
with [BadgerDB](https://github.com/dgraph-io/badger) for local storage. Every
node stores a complete local copy of the data, and every mutation enters the
Raft log before it is applied to Badger.

## Project status

Honeybadger is pre-release and has not published a version tag. The exported
API, replicated command format, and on-disk compatibility may change before
v1.0.0. Evaluate the operational limitations below before using it in
production. Tagged releases will follow semantic versioning.

## Install

Honeybadger requires Go 1.26 or later.

```sh
go get github.com/DamianB-BitFlipper/honeybadger
```

```go
import "github.com/DamianB-BitFlipper/honeybadger"
```

## Quick start

Open the first node with `NewCluster()`. It bootstraps a single-node Raft
configuration and waits for the first election, so the returned database is
immediately usable:

```go
db, err := honeybadger.Open(honeybadger.Config{
    NodeID:   "n1",
    RaftBind: "127.0.0.1:7001",
    DataDir:  "/var/lib/myapp/n1",
}, honeybadger.NewCluster())
if err != nil {
    log.Fatal(err)
}
defer db.Close()

if err := db.Set("hello", "world"); err != nil {
    log.Fatal(err)
}

value, err := db.Get("hello") // linearizable, leader-only read
if err != nil {
    log.Fatal(err)
}
fmt.Println(value)
```

Open additional nodes without `NewCluster()` and add them through the leader:

```go
n2, err := honeybadger.Open(honeybadger.Config{
    NodeID:   "n2",
    RaftBind: "127.0.0.1:7002",
    DataDir:  "/var/lib/myapp/n2",
})
if err != nil {
    log.Fatal(err)
}
defer n2.Close()

err = db.AddVoter(honeybadger.Node{
    ID:       "n2",
    RaftAddr: "127.0.0.1:7002",
})
if err != nil {
    log.Fatal(err)
}
```

`AddVoter` must be called on the leader. Success means the membership change
was committed; the joining node may still be replaying log entries or
installing a snapshot.

For a complete runnable cluster, see
[`examples/three-node`](examples/three-node).

## Architecture

```text
  Client                Leader node                     Follower nodes
    │                       │                                 │
    │  db.Set(k, v)         │                                 │
    │ ─────────────▶ encode command                           │
    │                  Raft.Apply ──── replicate ────────────▶│
    │                       │◀──────── acknowledgements ──────│
    │                       │                                 │
    │                entry committed by quorum                │
    │                       │                                 │
    │                  FSM.Apply ───── committed ───────▶ FSM.Apply
    │                       │                                 │
    │                  Badger.Update                     Badger.Update
    │                       │                                 │
    │  error/ok ◀───────────┘                                 │
```

## Key guarantees

- Writes return after their Raft entry is committed by a quorum and applied
  to the leader's Badger database.
- `Get` and zero-value `ReadOptions` perform linearizable, leader-only reads
  behind a Raft barrier.
- `ReadLocal` explicitly opts into potentially stale reads from any node.
- `Batch` uses one Raft entry and one Badger transaction, so its mutations are
  applied atomically on each node.
- TTLs are converted to absolute expiry timestamps on the leader before
  replication, preventing replay from extending a key's lifetime.
- Invalid operations are rejected with `ErrInvalidArgument` before a Raft
  entry is submitted.

See [the design notes](docs/design.md) for snapshot convergence, restore
failure behavior, storage layout, and the detailed consistency model.

## API

The primary API is a replicated `map[string]string`: `Set`, `Get`, `Delete`,
and atomic `Batch` operations use ordinary string keys and values.

The advanced API adds byte-slice keys and values, TTLs, bounded prefix scans,
local reads, raw Badger read transactions, cluster membership, snapshots, and
typed status information. Zero-value options select linearizable reads and the
default 100-entry scan limit.

Follower reads must opt into local consistency explicitly:

```go
value, err := follower.GetWithOptions("key", honeybadger.ReadOptions{
    Mode: honeybadger.ReadLocal,
})
```

Operational errors are classifiable with `errors.Is`; leader-routing failures
also expose a typed `*honeybadger.NotLeaderError` containing the known leader's
ID and Raft address. See the complete
[Go reference](https://pkg.go.dev/github.com/DamianB-BitFlipper/honeybadger) for
all exported operations, options, and errors.

## Operational scope and limitations

- Honeybadger is an embedded library, not a network database server. It does
  not provide a client protocol, request forwarding, service discovery, or
  automatic leader routing.
- Run three or five voters for fault tolerance. A single-node cluster has no
  redundancy, and a two-node cluster cannot commit after either node fails.
- Every node stores the complete dataset. Honeybadger does not shard data
  across nodes.
- Each node requires its own `DataDir`, stable `NodeID`, and reachable
  `RaftBind`/`RaftAdvertise` address.
- The built-in Raft transport is plain TCP with no authentication or TLS. Run
  it only on a trusted private network or provide protection at the network
  layer.
- Writes and linearizable reads must reach the leader. Followers return a
  typed `NotLeaderError`; applications are responsible for rerouting.
- A committed membership change does not mean a new voter has caught up. Use
  an application-level readiness check before directing reads to it.
- Raft snapshots support replication and log compaction; they are not an
  external backup or disaster-recovery mechanism.
- Custom `BadgerOptions`, especially a separate `ValueDir`, can weaken restore
  failure guarantees. Review the [restore design](docs/design.md#snapshot-restore)
  before using that escape hatch.

## Documentation

- [Go reference](https://pkg.go.dev/github.com/DamianB-BitFlipper/honeybadger)
- [Design and consistency notes](docs/design.md)
- [Runnable three-node example](examples/three-node)

## Development

The test suite uses real loopback TCP transports and temporary data
directories. The short suite skips the longer write-storm, snapshot-catch-up,
and TTL-across-restart scenarios:

```sh
go test -short ./...
go test ./... -race -count=1 -timeout 1200s
```

Bug reports, feature requests, and pull requests are welcome through
[GitHub issues](https://github.com/DamianB-BitFlipper/honeybadger/issues).
Before submitting a change, also run the same static checks as CI:

```sh
test -z "$(gofmt -l .)"
go mod tidy -diff
go vet ./...
```

## License

Honeybadger is released under the [MIT License](LICENSE).
