package honeybadger

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	snapshotRetainCount = 2
	transportMaxPool    = 3
	transportTimeout    = 10 * time.Second
)

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
