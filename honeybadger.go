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

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	defaultApplyTimeout      = 10 * time.Second
	defaultSnapshotThreshold = 8192
	snapshotRetainCount      = 2
	transportMaxPool         = 3
	transportTimeout         = 10 * time.Second
)

// newClusterLeaderTimeout bounds how long Open with NewCluster waits for
// the first election to complete. It is a var (not a const) so tests can
// shrink it to exercise the election-timeout failure path.
var newClusterLeaderTimeout = 30 * time.Second

// Config controls how a honeybadger node is opened. The top-level fields
// are the required lifecycle settings; everything tunable lives under
// Advanced and keeps its default when left at zero.
type Config struct {
	// NodeID uniquely identifies this node in the Raft cluster. Required.
	NodeID string

	// RaftBind is the host:port the Raft TCP transport listens on and, by
	// default, advertises to peers. Required. Use a routable address such
	// as "10.0.0.5:7000" or, for tests, "127.0.0.1:7000". See
	// AdvancedConfig.RaftAdvertise when the listen address is not routable
	// (e.g. binding "0.0.0.0").
	RaftBind string

	// DataDir holds the Badger database and the Raft log/snapshot state.
	// Required. The directory is created if it does not exist.
	DataDir string

	// Advanced holds tuning settings for advanced users. The zero value
	// keeps every default.
	Advanced AdvancedConfig
}

// AdvancedConfig groups the tuning knobs of Config. Most users never touch
// it.
type AdvancedConfig struct {
	// ApplyTimeout bounds how long a single write (or the barrier behind a
	// linearizable read) waits for Raft. Defaults to 10s.
	ApplyTimeout time.Duration

	// SnapshotThreshold controls how many outstanding Raft log entries
	// trigger a snapshot. Defaults to 8192.
	SnapshotThreshold uint64

	// RaftAdvertise is the host:port advertised to peers when it differs
	// from RaftBind (for example when binding "0.0.0.0" or behind NAT).
	// Defaults to RaftBind. It must be routable by every peer.
	RaftAdvertise string

	// BadgerOptions optionally overrides the Badger configuration used for
	// the local store. This is a raw, unsafe escape hatch: the Dir field
	// selects the directory wiped and reloaded during snapshot restore,
	// and a custom ValueDir selects a less safe (non-staged) restore path.
	// The rest of the options are reused verbatim on reopen. Defaults to
	// badger.DefaultOptions(filepath.Join(DataDir, "badger")).WithLogger(nil).
	BadgerOptions *badger.Options

	// LogOutput receives Raft (and snapshot-store) log output as well as
	// honeybadger's own internal diagnostics (e.g. FSM apply or restore
	// failures on followers). Defaults to io.Discard.
	LogOutput io.Writer
}

func (c *Config) setDefaults() {
	if c.Advanced.ApplyTimeout <= 0 {
		c.Advanced.ApplyTimeout = defaultApplyTimeout
	}
	if c.Advanced.SnapshotThreshold == 0 {
		c.Advanced.SnapshotThreshold = defaultSnapshotThreshold
	}
	if c.Advanced.LogOutput == nil {
		c.Advanced.LogOutput = io.Discard
	}
}

// DB is an open honeybadger node: one Badger database replicated through a
// Raft consensus group.
type DB struct {
	cfg Config

	raft      *raft.Raft
	transport *raft.NetworkTransport
	boltStore *raftboltdb.BoltStore
	fsm       *fsm
	logger    *log.Logger

	// storeMu guards store against snapshot Restore, which swaps the Badger
	// database on a Raft goroutine. Readers and the FSM hold it in read
	// (shared) mode for the duration of a Badger transaction; the Restore
	// swap takes it exclusively.
	storeMu sync.RWMutex
	store   *badger.DB

	badgerOpts badger.Options
	badgerDir  string

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Open opens (or creates) a honeybadger node according to cfg.
//
// Plain Open never forms a cluster: the node starts un-bootstrapped and
// waits to be added through DB.AddVoter on the leader of an existing
// cluster. Pass NewCluster() for exactly the first node of a new cluster;
// it bootstraps a single-server configuration and Open blocks until the
// first election completes, so the returned DB is immediately writable.
func Open(cfg Config, options ...OpenOption) (*DB, error) {
	newCluster := false
	for _, opt := range options {
		switch opt.(type) {
		case newClusterOption:
			if newCluster {
				return nil, fmt.Errorf("%w: NewCluster passed more than once", ErrInvalidArgument)
			}
			newCluster = true
		default:
			return nil, fmt.Errorf("%w: unknown open option %T", ErrInvalidArgument, opt)
		}
	}

	if cfg.NodeID == "" {
		return nil, errors.New("honeybadger: Config.NodeID is required")
	}
	if cfg.RaftBind == "" {
		return nil, errors.New("honeybadger: Config.RaftBind is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("honeybadger: Config.DataDir is required")
	}
	cfg.setDefaults()

	var advertise net.Addr
	if cfg.Advanced.RaftAdvertise != "" {
		addr, err := net.ResolveTCPAddr("tcp", cfg.Advanced.RaftAdvertise)
		if err != nil {
			return nil, fmt.Errorf("honeybadger: resolve Config.Advanced.RaftAdvertise: %w", err)
		}
		advertise = addr
	}

	db := &DB{cfg: cfg}
	db.logger = log.New(cfg.Advanced.LogOutput, "honeybadger: ", log.LstdFlags)

	// Resolve Badger options.
	if cfg.Advanced.BadgerOptions != nil {
		db.badgerOpts = *cfg.Advanced.BadgerOptions
	} else {
		db.badgerOpts = badger.DefaultOptions(filepath.Join(cfg.DataDir, "badger")).WithLogger(nil)
	}
	db.badgerDir = db.badgerOpts.Dir

	if err := os.MkdirAll(db.badgerDir, 0o755); err != nil {
		return nil, fmt.Errorf("honeybadger: create badger dir: %w", err)
	}
	store, err := badger.Open(db.badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("honeybadger: open badger: %w", err)
	}
	db.store = store
	db.fsm = &fsm{db: db}
	defer func() {
		// Roll back on any later failure.
		if err != nil {
			db.closeStore()
		}
	}()

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err = os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("honeybadger: create raft dir: %w", err)
	}

	db.boltStore, err = raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("honeybadger: open raft bolt store: %w", err)
	}
	defer func() {
		if err != nil {
			db.boltStore.Close()
		}
	}()

	// raft.NewFileSnapshotStore appends a "snapshots" element to its base,
	// so the final layout is exactly <DataDir>/raft/snapshots.
	snaps, err := raft.NewFileSnapshotStore(raftDir, snapshotRetainCount, cfg.Advanced.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("honeybadger: open snapshot store: %w", err)
	}

	db.transport, err = raft.NewTCPTransport(cfg.RaftBind, advertise, transportMaxPool, transportTimeout, cfg.Advanced.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("honeybadger: open raft transport: %w", err)
	}
	defer func() {
		if err != nil {
			db.transport.Close()
		}
	}()

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = cfg.Advanced.SnapshotThreshold
	// Keep one snapshot threshold worth of trailing logs after compaction
	// so recently snapshotted followers can still catch up from the log.
	raftCfg.TrailingLogs = cfg.Advanced.SnapshotThreshold
	raftCfg.LogOutput = cfg.Advanced.LogOutput

	if newCluster {
		configuration := raft.Configuration{
			Servers: []raft.Server{{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(cfg.NodeID),
				Address:  db.transport.LocalAddr(),
			}},
		}
		err = raft.BootstrapCluster(raftCfg, db.boltStore, db.boltStore, snaps, db.transport, configuration)
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("honeybadger: bootstrap cluster: %w", err)
		}
		// ErrCantBootstrap means this DataDir already holds cluster state
		// (a restart of the bootstrap node): recover it below instead.
		err = nil
	}

	db.raft, err = raft.NewRaft(raftCfg, db.fsm, db.boltStore, db.boltStore, snaps, db.transport)
	if err != nil {
		return nil, fmt.Errorf("honeybadger: start raft: %w", err)
	}

	if newCluster {
		// Wait for the very first election so the returned node is
		// immediately usable. db.Close tears the node down on failure, so
		// disarm the staged rollback defers to avoid double-closing.
		if _, werr := db.waitForLeader(newClusterLeaderTimeout); werr != nil {
			db.Close()
			err = nil
			return nil, fmt.Errorf("honeybadger: open new cluster: %w", werr)
		}
	}

	return db, nil
}

// Close shuts the node down: Raft is stopped, the transport and log stores
// are closed, and the Badger database is flushed and closed. Close is
// idempotent and safe to call multiple times. Operations invoked after
// Close returns fail with ErrClosed; RawRaftStats is the single documented
// exception and keeps returning the final Raft statistics.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		db.closeErr = errors.Join(
			db.raft.Shutdown().Error(),
			db.transport.Close(),
			db.boltStore.Close(),
			db.closeStore(),
		)
	})
	return db.closeErr
}

// closeStore closes the current Badger database. Caller must not hold storeMu.
func (db *DB) closeStore() error {
	db.storeMu.Lock()
	defer db.storeMu.Unlock()
	if db.store == nil {
		return nil
	}
	err := db.store.Close()
	db.store = nil
	return err
}

// checkOpen reports whether the DB is still usable.
func (db *DB) checkOpen() error {
	if db.closed.Load() {
		return ErrClosed
	}
	return nil
}

// withStore runs fn against the live Badger database while holding the
// store lock in shared mode, so a concurrent snapshot Restore cannot swap
// or close the database mid-transaction.
func (db *DB) withStore(fn func(*badger.DB) error) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	db.storeMu.RLock()
	defer db.storeMu.RUnlock()
	if db.store == nil {
		return ErrClosed
	}
	return fn(db.store)
}

// ---------------------------------------------------------------------------
// Writes. All writes are replicated through Raft and applied to Badger by
// the FSM only after the log entry is committed. Arguments are validated
// before any Raft entry is submitted.
// ---------------------------------------------------------------------------

// Set stores key/value in the cluster; it persists forever unless WithTTL
// is supplied. It returns once the write is committed by a Raft majority
// and applied locally. It fails with ErrInvalidArgument on an empty key or
// invalid option, and with a *NotLeaderError on followers.
func (db *DB) Set(key, value string, opts ...SetOption) error {
	return db.SetBytes([]byte(key), []byte(value), opts...)
}

// SetBytes is Set for byte-slice keys and values.
func (db *DB) SetBytes(key, value []byte, opts ...SetOption) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: key must not be empty", ErrInvalidArgument)
	}
	expiresAt, err := resolveSetOptions(opts)
	if err != nil {
		return err
	}
	return db.apply(command{Op: opSet, Pairs: []commandPair{{Key: key, Value: value, ExpiresAtUnix: expiresAt}}})
}

// Delete removes key from the cluster. Deleting a missing key is not an
// error.
func (db *DB) Delete(key string) error {
	return db.DeleteBytes([]byte(key))
}

// DeleteBytes is Delete for byte-slice keys.
func (db *DB) DeleteBytes(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: key must not be empty", ErrInvalidArgument)
	}
	return db.apply(command{Op: opDelete, Deletes: [][]byte{key}})
}

// Batch applies all mutations atomically: they are committed as a single
// Raft log entry and applied in a single Badger transaction, so no node
// ever observes a partial batch. Per-mutation TTLs (via WithTTL) follow
// the same absolute-expiry semantics as Set.
//
// Every mutation is validated before any Raft entry is submitted: empty
// keys, non-positive TTLs, malformed Mutations (not built by one of the
// constructors) and the same key appearing twice in one batch are all
// rejected with ErrInvalidArgument. A batch with no mutations at all is a
// no-op and returns nil without submitting a Raft entry. Very large
// batches can exceed Badger's max transaction size; split them if needed.
func (db *DB) Batch(mutations ...Mutation) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if len(mutations) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(mutations))
	pairs := make([]commandPair, 0, len(mutations))
	var deletes [][]byte
	for i, m := range mutations {
		if len(m.key) == 0 {
			return fmt.Errorf("%w: batch mutation %d: key must not be empty", ErrInvalidArgument, i)
		}
		if _, dup := seen[string(m.key)]; dup {
			return fmt.Errorf("%w: batch mutation %d: duplicate key %q", ErrInvalidArgument, i, m.key)
		}
		seen[string(m.key)] = struct{}{}
		switch m.kind {
		case mutSet:
			expiresAt, err := resolveSetOptions(m.opts)
			if err != nil {
				return fmt.Errorf("batch mutation %d: %w", i, err)
			}
			pairs = append(pairs, commandPair{Key: m.key, Value: m.value, ExpiresAtUnix: expiresAt})
		case mutDelete:
			deletes = append(deletes, m.key)
		default:
			return fmt.Errorf("%w: batch mutation %d: not created by SetOp/SetBytesOp/DeleteOp/DeleteBytesOp",
				ErrInvalidArgument, i)
		}
	}
	return db.apply(command{Op: opBatch, Pairs: pairs, Deletes: deletes})
}

// absExpiry converts a relative TTL into an absolute Unix expiry timestamp,
// stamped once at write submission on the leader. A zero TTL means persist
// forever (0).
func absExpiry(ttl time.Duration) uint64 {
	if ttl == 0 {
		return 0
	}
	return uint64(time.Now().Add(ttl).Unix())
}

// apply encodes cmd, submits it through Raft and waits for the commit.
// The FSM's error (if any) is surfaced to the caller.
func (db *DB) apply(cmd command) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	data, err := encodeCommand(cmd)
	if err != nil {
		return err
	}
	future := db.raft.Apply(data, db.cfg.Advanced.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	// Errors from the FSM (badger transaction failures) arrive via Response.
	if resp := future.Response(); resp != nil {
		if fsmErr, ok := resp.(error); ok {
			return fsmErr
		}
	}
	return nil
}

// notLeaderErr returns a *NotLeaderError with the current leader's ID and
// address filled in when known.
func (db *DB) notLeaderErr() error {
	addr, id := db.raft.LeaderWithID()
	return &NotLeaderError{LeaderID: string(id), LeaderAddr: string(addr)}
}

// ---------------------------------------------------------------------------
// Reads. Tier 1 (Get) is strictly consistent by default; Tier 2 takes an
// explicit ReadOptions with a safe zero value (ReadLinearizable) governing
// point reads, scans and views uniformly.
// ---------------------------------------------------------------------------

// readGate enforces the consistency side of ReadOptions: linearizable
// reads must run on the leader behind a Raft barrier; local reads pass.
func (db *DB) readGate(ro ReadOptions) error {
	if ro.Timeout < 0 {
		return fmt.Errorf("%w: negative read timeout %s", ErrInvalidArgument, ro.Timeout)
	}
	switch ro.Mode {
	case ReadLinearizable:
		if err := db.checkOpen(); err != nil {
			return err
		}
		if db.raft.State() != raft.Leader {
			return db.notLeaderErr()
		}
		return db.Barrier(ro.timeoutOr(db.cfg.Advanced.ApplyTimeout))
	case ReadLocal:
		return db.checkOpen()
	default:
		return fmt.Errorf("%w: unknown read mode %d", ErrInvalidArgument, ro.Mode)
	}
}

// getBytes performs the local Badger point read shared by all read APIs.
// Callers must have passed readGate first.
func (db *DB) getBytes(key []byte) ([]byte, error) {
	var value []byte
	err := db.withStore(func(b *badger.DB) error {
		return b.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			if err != nil {
				return err
			}
			value, err = item.ValueCopy(nil)
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Get returns the value stored under key, or ErrKeyNotFound.
//
// Get is strictly consistent by deliberate default: it must be called on
// the leader (it fails with a *NotLeaderError on followers — it never
// silently downgrades to a possibly stale local read) and runs a Raft
// barrier first, so every previously committed write is visible. Advanced
// callers that accept eventual consistency — e.g. reads on followers —
// opt in explicitly with GetWithOptions(key, ReadOptions{Mode: ReadLocal}).
func (db *DB) Get(key string) (string, error) {
	return db.GetWithOptions(key, ReadOptions{})
}

// GetWithOptions is Get with an explicit consistency/timeout choice.
func (db *DB) GetWithOptions(key string, ro ReadOptions) (string, error) {
	value, err := db.GetBytes([]byte(key), ro)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// GetBytes is the byte-slice point read with an explicit ReadOptions. It
// returns a copy of the value that stays valid after the call, or
// ErrKeyNotFound when the key does not exist or has expired.
func (db *DB) GetBytes(key []byte, ro ReadOptions) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("%w: key must not be empty", ErrInvalidArgument)
	}
	if err := db.readGate(ro); err != nil {
		return nil, err
	}
	return db.getBytes(key)
}

// ScanPrefixBytes returns entries whose keys start with prefix, in key
// order. An empty prefix scans the entire keyspace. Keys and values are
// copies that stay valid after the call.
//
// opts.Read selects the consistency guarantee exactly as for point reads
// (the zero value is a linearizable, leader-only scan). The result is
// truncated after opts.Limit entries — zero means the conservative default
// of 100 — and truncation is silent, in key order; set opts.Unlimited to
// true to scan without a cap. A negative Limit fails with
// ErrInvalidArgument.
func (db *DB) ScanPrefixBytes(prefix []byte, opts ScanOptions) ([]Entry, error) {
	limit, err := opts.resolveLimit()
	if err != nil {
		return nil, err
	}
	if err := db.readGate(opts.Read); err != nil {
		return nil, err
	}
	var entries []Entry
	err = db.withStore(func(b *badger.DB) error {
		return b.View(func(txn *badger.Txn) error {
			itOpts := badger.DefaultIteratorOptions
			it := txn.NewIterator(itOpts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if limit > 0 && len(entries) >= limit {
					break
				}
				item := it.Item()
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				entries = append(entries, Entry{Key: item.KeyCopy(nil), Value: value})
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// ViewBadger runs fn inside a raw Badger read-only transaction. It is an
// escape hatch for read patterns the typed API does not cover: the
// transaction is read-only, and the transaction and any items obtained
// from it must not escape the callback. ro selects the consistency
// guarantee exactly as for point reads.
func (db *DB) ViewBadger(ro ReadOptions, fn func(*badger.Txn) error) error {
	if fn == nil {
		return fmt.Errorf("%w: ViewBadger requires a callback", ErrInvalidArgument)
	}
	if err := db.readGate(ro); err != nil {
		return err
	}
	return db.withStore(func(b *badger.DB) error {
		return b.View(fn)
	})
}

// ---------------------------------------------------------------------------
// Cluster membership and introspection.
// ---------------------------------------------------------------------------

// Barrier blocks until the leader has applied all outstanding log entries
// (up to timeout), making subsequent local reads linearizable. It returns
// a *NotLeaderError on followers.
func (db *DB) Barrier(timeout time.Duration) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	if err := db.raft.Barrier(timeout).Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// AddVoter adds node to the cluster as a voter. It is called ON THE
// LEADER, on behalf of the joining node — not by the joining node itself —
// and returns a *NotLeaderError otherwise. The joining node must already
// be running (opened without NewCluster). node.Role must be left at
// RoleNone (or set to RoleVoter); any other value is rejected with
// ErrInvalidArgument because added nodes always become voters.
//
// A successful return confirms the membership change was committed, NOT
// that the new voter has caught up: it replays missed log entries (or
// receives a snapshot) asynchronously. Poll its local reads or its
// Status().AppliedIndex when you need to know it serves current data.
func (db *DB) AddVoter(node Node) error {
	if node.ID == "" {
		return fmt.Errorf("%w: AddVoter: node ID must not be empty", ErrInvalidArgument)
	}
	if node.RaftAddr == "" {
		return fmt.Errorf("%w: AddVoter: node RaftAddr must not be empty", ErrInvalidArgument)
	}
	if node.Role != RoleNone && node.Role != RoleVoter {
		return fmt.Errorf("%w: AddVoter: role must be RoleNone or RoleVoter, got %s", ErrInvalidArgument, node.Role)
	}
	if err := db.checkOpen(); err != nil {
		return err
	}
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	future := db.raft.AddVoter(raft.ServerID(node.ID), raft.ServerAddress(node.RaftAddr), 0, db.cfg.Advanced.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// RemoveNode removes the node with the given ID from the cluster
// configuration. It must be called on the leader and returns a
// *NotLeaderError otherwise.
func (db *DB) RemoveNode(id string) error {
	if id == "" {
		return fmt.Errorf("%w: RemoveNode: node ID must not be empty", ErrInvalidArgument)
	}
	if err := db.checkOpen(); err != nil {
		return err
	}
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	future := db.raft.RemoveServer(raft.ServerID(id), 0, db.cfg.Advanced.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// Members returns the servers in the current Raft cluster configuration.
// It works on any node (the configuration is replicated to followers), but
// a follower's view may lag the leader's by a configuration change or two.
func (db *DB) Members() ([]Node, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	future := db.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	servers := future.Configuration().Servers
	nodes := make([]Node, 0, len(servers))
	for _, s := range servers {
		nodes = append(nodes, Node{
			ID:       string(s.ID),
			RaftAddr: string(s.Address),
			Role:     roleFromRaft(s.Suffrage),
		})
	}
	return nodes, nil
}

// roleFromRaft maps a raft.ServerSuffrage onto the exported NodeRole type.
func roleFromRaft(s raft.ServerSuffrage) NodeRole {
	switch s {
	case raft.Voter:
		return RoleVoter
	case raft.Nonvoter:
		return RoleNonvoter
	case raft.Staging:
		return RoleStaging
	default:
		return RoleNone
	}
}

// Snapshot forces a local Raft snapshot of this node's state, compacting
// its log (one SnapshotThreshold worth of trailing log entries is
// retained). It may be called on any node, leader or follower; each node's
// snapshot covers its own applied state. It is rarely needed in practice
// because snapshots are taken automatically once the log grows past
// SnapshotThreshold, but it is useful for tests and maintenance.
func (db *DB) Snapshot() error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	return db.raft.Snapshot().Error()
}

// Status returns a typed snapshot of this node's view of the cluster: its
// own identity and role, its Raft state, the known leader (if any), and
// the index of the last log entry applied to local storage. Like every
// operational method, it fails with ErrClosed after Close.
func (db *DB) Status() (Status, error) {
	if err := db.checkOpen(); err != nil {
		return Status{}, err
	}
	st := Status{
		Local: Node{
			ID:       db.cfg.NodeID,
			RaftAddr: string(db.transport.LocalAddr()),
			Role:     RoleNone,
		},
		State:        stateFromRaft(db.raft.State()),
		AppliedIndex: db.fsm.appliedIndex.Load(),
	}
	if addr, id := db.raft.LeaderWithID(); id != "" {
		st.Leader = &Node{ID: string(id), RaftAddr: string(addr), Role: RoleVoter}
	}
	// Fill in the local role from the replicated configuration.
	future := db.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return Status{}, err
	}
	for _, s := range future.Configuration().Servers {
		if s.ID == raft.ServerID(db.cfg.NodeID) {
			st.Local.Role = roleFromRaft(s.Suffrage)
			break
		}
	}
	return st, nil
}

// RawRaftStats returns the raw, stringly Raft statistics map (keys are
// Raft's own and not covered by any stability guarantee) plus a
// "honeybadger_applied_index" entry reporting the last log index applied
// by the local FSM. Prefer Status for supported fields. Unlike operational
// methods, RawRaftStats is a passive snapshot that stays callable after
// Close and then returns the final statistics.
func (db *DB) RawRaftStats() map[string]string {
	stats := db.raft.Stats()
	stats["honeybadger_applied_index"] = strconv.FormatUint(db.fsm.appliedIndex.Load(), 10)
	return stats
}

// WaitForLeader blocks until this node knows of a cluster leader (or the
// timeout expires) and returns that leader. When no leader becomes known
// in time, the error wraps ErrNoLeader; after Close it fails with
// ErrClosed.
func (db *DB) WaitForLeader(timeout time.Duration) (Node, error) {
	return db.waitForLeader(timeout)
}

// waitForLeader is the internal implementation shared by WaitForLeader and
// the NewCluster startup wait.
func (db *DB) waitForLeader(timeout time.Duration) (Node, error) {
	deadline := time.Now().Add(timeout)
	for {
		if err := db.checkOpen(); err != nil {
			return Node{}, err
		}
		if addr, id := db.raft.LeaderWithID(); id != "" {
			return Node{ID: string(id), RaftAddr: string(addr), Role: RoleVoter}, nil
		}
		if !time.Now().Before(deadline) {
			return Node{}, fmt.Errorf("%w after %s", ErrNoLeader, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
