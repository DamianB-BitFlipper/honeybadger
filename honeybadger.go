// Package honeybadger is an embeddable, replicated, in-process key-value
// store. It combines hashicorp/raft for consensus with dgraph-io/badger for
// local storage.
//
// Every mutation is committed through Raft first and only applied to the
// local Badger database by the Raft FSM after the log entry is committed.
// Each node keeps a fully independent Badger database on disk; Raft
// guarantees they converge to identical state. Reads are served locally
// from Badger.
package honeybadger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ErrKeyNotFound is returned by Get when the requested key does not exist
// (or has expired).
var ErrKeyNotFound = errors.New("honeybadger: key not found")

// ErrNotLeader is returned by write and cluster-membership operations when
// they are invoked on a node that is not the current Raft leader. The
// wrapped message contains the known leader address, if any.
var ErrNotLeader = errors.New("honeybadger: not the leader")

// ErrClosed is returned by operations invoked on a DB that has been closed.
var ErrClosed = errors.New("honeybadger: database is closed")

const (
	defaultApplyTimeout      = 10 * time.Second
	defaultSnapshotThreshold = 8192
	snapshotRetainCount      = 2
	transportMaxPool         = 3
	transportTimeout         = 10 * time.Second
)

// Config controls how a honeybadger node is opened.
type Config struct {
	// NodeID uniquely identifies this node in the Raft cluster. Required.
	NodeID string

	// RaftBind is the host:port the Raft TCP transport listens on and
	// advertises to peers. Required. Use a routable address such as
	// "10.0.0.5:7000" or, for tests, "127.0.0.1:7000".
	RaftBind string

	// DataDir holds the Badger database and the Raft log/snapshot state.
	// Required. The directory is created if it does not exist.
	DataDir string

	// Bootstrap must be true for exactly the very first node of a new
	// cluster; it bootstraps a single-server configuration containing
	// itself. All other nodes are opened with Bootstrap=false and added
	// via Join. Re-opening an existing DataDir with Bootstrap=true is
	// harmless (the stale bootstrap attempt is ignored).
	Bootstrap bool

	// ApplyTimeout bounds how long a single write waits for Raft commit.
	// Defaults to 10s.
	ApplyTimeout time.Duration

	// SnapshotThreshold controls how many outstanding Raft log entries
	// trigger a snapshot. Defaults to 8192.
	SnapshotThreshold uint64

	// BadgerOptions optionally overrides the Badger configuration used for
	// the local store. The Dir field selects the directory wiped and
	// reloaded during snapshot restore; the rest of the options are reused
	// verbatim on reopen. Defaults to
	// badger.DefaultOptions(filepath.Join(DataDir, "badger")).WithLogger(nil).
	BadgerOptions *badger.Options

	// LogOutput receives Raft (and snapshot-store) log output.
	// Defaults to io.Discard.
	LogOutput io.Writer
}

func (c *Config) setDefaults() {
	if c.ApplyTimeout <= 0 {
		c.ApplyTimeout = defaultApplyTimeout
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = defaultSnapshotThreshold
	}
	if c.LogOutput == nil {
		c.LogOutput = io.Discard
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

	// storeMu guards store against snapshot Restore, which closes and
	// reopens the Badger database on a Raft goroutine. Readers and the FSM
	// hold it in read (shared) mode for the duration of a Badger
	// transaction; Restore takes it exclusively.
	storeMu sync.RWMutex
	store   *badger.DB

	badgerOpts badger.Options
	badgerDir  string

	closeOnce sync.Once
	closeErr  error
}

// Open opens (or creates) a honeybadger node according to cfg.
func Open(cfg Config) (*DB, error) {
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

	db := &DB{cfg: cfg}

	// Resolve Badger options.
	if cfg.BadgerOptions != nil {
		db.badgerOpts = *cfg.BadgerOptions
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

	snaps, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), snapshotRetainCount, cfg.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("honeybadger: open snapshot store: %w", err)
	}

	db.transport, err = raft.NewTCPTransport(cfg.RaftBind, nil, transportMaxPool, transportTimeout, cfg.LogOutput)
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
	raftCfg.SnapshotThreshold = cfg.SnapshotThreshold
	// Keep one snapshot threshold worth of trailing logs after compaction
	// so recently snapshotted followers can still catch up from the log.
	raftCfg.TrailingLogs = cfg.SnapshotThreshold
	raftCfg.LogOutput = cfg.LogOutput

	if cfg.Bootstrap {
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
	}

	db.raft, err = raft.NewRaft(raftCfg, db.fsm, db.boltStore, db.boltStore, snaps, db.transport)
	if err != nil {
		return nil, fmt.Errorf("honeybadger: start raft: %w", err)
	}

	return db, nil
}

// Close shuts the node down: Raft is stopped, the transport and log stores
// are closed, and the Badger database is flushed and closed. Close is
// idempotent and safe to call multiple times.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
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

// withStore runs fn against the live Badger database while holding the
// store lock in shared mode, so a concurrent snapshot Restore cannot swap
// or close the database mid-transaction.
func (db *DB) withStore(fn func(*badger.DB) error) error {
	db.storeMu.RLock()
	defer db.storeMu.RUnlock()
	if db.store == nil {
		return ErrClosed
	}
	return fn(db.store)
}

// ---------------------------------------------------------------------------
// Writes. All writes are replicated through Raft and applied to Badger by
// the FSM only after the log entry is committed.
// ---------------------------------------------------------------------------

// Set stores key/value in the cluster. It returns once the write is
// committed by a Raft majority and applied locally.
func (db *DB) Set(key, value []byte) error {
	return db.apply(command{Op: opSet, Pairs: []Pair{{Key: key, Value: value}}})
}

// SetWithTTL is like Set but the key expires after ttl. Expired keys behave
// exactly like missing keys on read.
func (db *DB) SetWithTTL(key, value []byte, ttl time.Duration) error {
	return db.apply(command{Op: opSet, Pairs: []Pair{{Key: key, Value: value, TTL: ttl}}})
}

// Delete removes key from the cluster. Deleting a missing key is not an
// error.
func (db *DB) Delete(key []byte) error {
	return db.apply(command{Op: opDelete, Deletes: [][]byte{key}})
}

// Pair is a key/value pair with an optional TTL (0 means persist forever).
// Pairs returned by PrefixScan always have a zero TTL.
type Pair struct {
	Key   []byte
	Value []byte
	TTL   time.Duration
}

// Batch applies all sets and deletes atomically: they are committed as a
// single Raft log entry and applied in a single Badger transaction.
func (db *DB) Batch(sets []Pair, deletes [][]byte) error {
	return db.apply(command{Op: opBatch, Pairs: sets, Deletes: deletes})
}

// apply encodes cmd, submits it through Raft and waits for the commit.
// The FSM's error (if any) is surfaced to the caller.
func (db *DB) apply(cmd command) error {
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	data, err := encodeCommand(cmd)
	if err != nil {
		return err
	}
	future := db.raft.Apply(data, db.cfg.ApplyTimeout)
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

// notLeaderErr wraps ErrNotLeader with the current leader address, if known.
func (db *DB) notLeaderErr() error {
	if addr := db.raft.Leader(); addr != "" {
		return fmt.Errorf("%w (leader at %s)", ErrNotLeader, string(addr))
	}
	return ErrNotLeader
}

// ---------------------------------------------------------------------------
// Reads. Reads are served from the local Badger database with no Raft round
// trip; they are eventually consistent with the leader unless preceded by
// Barrier on the leader (see GetConsistent).
// ---------------------------------------------------------------------------

// Get returns a copy of the value stored under key, or ErrKeyNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
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

// GetConsistent is Get preceded by a Raft barrier when called on the
// leader, giving a linearizable read: the barrier guarantees all previously
// committed entries are applied locally before the read. On a follower it
// falls back to a plain (eventually consistent) local Get.
func (db *DB) GetConsistent(key []byte) ([]byte, error) {
	if db.raft.State() == raft.Leader {
		if err := db.Barrier(db.cfg.ApplyTimeout); err != nil {
			return nil, err
		}
	}
	return db.Get(key)
}

// View runs fn inside a raw Badger read-only transaction. It is an escape
// hatch for read patterns the typed API does not cover.
func (db *DB) View(fn func(*badger.Txn) error) error {
	return db.withStore(func(b *badger.DB) error {
		return b.View(fn)
	})
}

// PrefixScan returns up to limit pairs whose keys start with prefix, in key
// order. A limit <= 0 means no limit. The TTL field of returned pairs is
// always zero.
func (db *DB) PrefixScan(prefix []byte, limit int) ([]Pair, error) {
	var pairs []Pair
	err := db.withStore(func(b *badger.DB) error {
		return b.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if limit > 0 && len(pairs) >= limit {
					break
				}
				item := it.Item()
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				pairs = append(pairs, Pair{Key: item.KeyCopy(nil), Value: value})
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return pairs, nil
}

// ---------------------------------------------------------------------------
// Cluster membership and introspection.
// ---------------------------------------------------------------------------

// Barrier blocks until the leader has applied all outstanding log entries
// (up to timeout), making subsequent local reads linearizable. It returns
// ErrNotLeader on followers.
func (db *DB) Barrier(timeout time.Duration) error {
	if err := db.raft.Barrier(timeout).Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// Join adds the node (nodeID, raftAddr) to the cluster as a voter. It must
// be called on the leader and returns ErrNotLeader otherwise. The joining
// node must already be running (opened with Bootstrap=false).
func (db *DB) Join(nodeID, raftAddr string) error {
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	future := db.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, db.cfg.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// Remove removes nodeID from the cluster configuration. It must be called
// on the leader and returns ErrNotLeader otherwise.
func (db *DB) Remove(nodeID string) error {
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	future := db.raft.RemoveServer(raft.ServerID(nodeID), 0, db.cfg.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// IsLeader reports whether this node is the current Raft leader.
func (db *DB) IsLeader() bool {
	return db.raft.State() == raft.Leader
}

// Leader returns the ID and Raft address of the current leader. Either may
// be empty if the leader is not (yet) known to this node.
func (db *DB) Leader() (id string, addr string) {
	leaderAddr, leaderID := db.raft.LeaderWithID()
	return string(leaderID), string(leaderAddr)
}

// State returns the Raft state of this node ("Follower", "Candidate",
// "Leader", or "Shutdown").
func (db *DB) State() string {
	return db.raft.State().String()
}

// Stats returns the Raft statistics map plus a "honeybadger_applied_index"
// entry reporting the last log index applied by the local FSM.
func (db *DB) Stats() map[string]string {
	stats := db.raft.Stats()
	stats["honeybadger_applied_index"] = strconv.FormatUint(db.fsm.appliedIndex.Load(), 10)
	return stats
}

// Snapshot forces a Raft snapshot of the current state on this node,
// compacting the log (one SnapshotThreshold worth of trailing log entries is
// retained). It must be called on the leader. It is rarely needed in
// practice because snapshots are taken automatically once the log grows
// past SnapshotThreshold, but it is useful for tests and maintenance.
func (db *DB) Snapshot() error {
	if err := db.raft.Snapshot().Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// WaitForLeader blocks until this node knows of a cluster leader or the
// timeout expires.
func (db *DB) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if db.raft.Leader() != "" {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("honeybadger: no leader known after %s", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
