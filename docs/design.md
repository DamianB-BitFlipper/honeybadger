# Honeybadger design notes

This document records the consistency and recovery invariants behind
Honeybadger. The exported API and examples live in the
[Go reference](https://pkg.go.dev/github.com/DamianB-BitFlipper/honeybadger)
and the project [README](../README.md).

## State and storage layout

Each `DB` owns one Badger database, one Raft node, a BoltDB-backed Raft log and
stable store, a TCP transport, and a file snapshot store. Every node stores the
complete keyspace locally.

The default layout is:

```text
<DataDir>/
├── badger/
└── raft/
    ├── raft.db
    └── snapshots/
```

`AdvancedConfig.BadgerOptions` can override the Badger configuration. It is a
low-level escape hatch: changing `Dir` or `ValueDir` also changes the paths
used during snapshot restore.

## Write path

Every mutation follows the same path:

1. The leader validates the request before submitting it to Raft.
2. The mutation is encoded as one command and passed to `raft.Apply`.
3. Raft replicates and commits the log entry through a quorum.
4. Each node's FSM applies the command in one Badger write transaction.
5. The leader returns after its local transaction succeeds or reports the FSM
   failure to the caller.

A batch contains all sets and deletes in one command and one transaction, so a
node never observes a partially applied batch.

Raft commitment cannot be undone by an FSM error. If command decoding or a
local Badger transaction fails, that node leaves its local state unchanged for
the command while Raft continues applying later entries. The leader returns
its local FSM error; follower-side failures are written to `Config.LogOutput`.
Operators must treat those failures as possible local divergence rather than
as an aborted consensus entry.

## Read consistency

The zero value of `ReadOptions` selects `ReadLinearizable`.

Linearizable reads:

- must run on the leader;
- submit a Raft barrier before reading Badger; and
- return `*NotLeaderError` when invoked on a follower.

The barrier ensures the leader has applied all preceding committed entries
before the local read begins.

`ReadLocal` bypasses Raft and reads the node's Badger database directly. It
works on leaders and followers but may observe stale data while a node catches
up. It is intended for caches, readiness checks, convergence polling, and
other callers that explicitly accept staleness.

## Replicated commands and TTLs

The replicated command format uses gob. Its exported field names are part of
the persisted log representation and cannot be renamed without a migration
strategy.

Commands are deliberately blind writes: they do not read prior state and are
idempotent when applied more than once. That property is required by the
snapshot model described below. A future compare-and-swap, increment, or
other read-modify-write command would require redesigning snapshot capture or
replay.

TTLs are converted to absolute Unix expiry timestamps once, on the leader,
before the command is submitted. All nodes therefore apply the same expiry.
Log replay cannot resurrect an expired key or extend a live key's lifetime.
Badger tracks expiration with one-second granularity, so sub-second TTLs may
expire almost immediately.

## Snapshot convergence

Raft labels a snapshot with FSM progression index `I`. Honeybadger's
`Snapshot` method initially captures only a database handle; Badger streams
the actual backup later from `Persist`, which may overlap subsequent FSM
applications.

The backup can therefore contain a transactionally complete state through a
later index `J`, while restore still replays log entries `I+1..J`. Convergence
holds because every replicated command is a blind, idempotent write and TTLs
contain absolute rather than apply-time expiries. Reapplying the overlap
produces the same final state.

This invariant must be revisited before adding any replicated command whose
result depends on prior state or the applying node's clock.

## Snapshot restore

Raft invokes restore when a node starts from a local snapshot or when a
follower receives `InstallSnapshot`. After a successful restore, Raft replays
entries following the snapshot index through the normal FSM path.

With the default colocated Badger `Dir` and `ValueDir`, restore has two phases:

1. Load the snapshot into a temporary Badger directory while the old database
   continues serving reads.
2. Under an exclusive store lock, close the live database, swap the staging
   directory into place, reopen it, and publish the restored database.

A staging failure leaves the old store untouched. A swap failure attempts to
restore and reopen the old directory, but rollback is best-effort and may
leave the store unavailable. Directory swaps are not crash-atomic; a later
restore cleans up staging or backup residue where possible.

When `BadgerOptions.ValueDir` is outside `Dir`, Honeybadger cannot relocate the
database with one directory rename. Restore then uses a destructive in-place
path: it closes and clears the configured directories before loading the
snapshot. That path has no staged rollback and can leave partially restored
files if loading fails.

Snapshot persistence holds a shared store lock so the selected Badger instance
cannot be closed or swapped during backup. Normal writes may continue because
Badger's backup is safe alongside transactions; restore and close wait for the
backup to finish.

Raft snapshots are internal replication and compaction artifacts. Applications
that require disaster recovery should maintain a separate backup strategy for
their deployment.

## Membership and readiness

`AddVoter` and `RemoveNode` are leader-only operations. `AddVoter` is invoked
on behalf of an already-running node that was opened without `NewCluster()`.

A successful `AddVoter` call confirms only that the membership change was
committed. The new voter may still need to replay logs or install a snapshot.
`Status().AppliedIndex` is a progress hint, not a proof of freshness: failed
commands and restores can leave or reset that counter. Use an application-level
readiness signal before routing reads to a newly joined node.

For normal fault tolerance, deploy three or five voters. A cluster needs a
quorum to commit writes, linearizable-read barriers, and membership changes.
