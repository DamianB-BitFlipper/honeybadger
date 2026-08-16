// Package honeybadger is an embeddable, replicated, in-process key-value
// store. It combines hashicorp/raft for consensus with dgraph-io/badger for
// local storage.
//
// Every mutation is committed through Raft first and only applied to the
// local Badger database by the Raft FSM after the log entry is committed.
// Each node keeps a fully independent Badger database on disk; Raft
// guarantees they converge to identical state.
//
// The API is organized in two tiers. Tier 1 is a replicated
// map[string]string: Open with NewCluster, then Set, Get, Delete and Batch
// with string keys and values. Tier-1 reads are strictly consistent by
// default: Get runs on the leader behind a Raft barrier and fails loudly
// with a *NotLeaderError on followers rather than silently serving stale
// data. Tier 2 is for advanced users: byte-slice keys and values, an
// explicit ReadOptions model (ReadLocal opts into eventual consistency),
// prefix scans, raw Badger read transactions, TTLs, cluster membership and
// typed status introspection.
package honeybadger
