package honeybadger

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// Command operation codes, encoded into every Raft log entry.
const (
	opSet uint8 = iota + 1
	opDelete
	opBatch
)

// commandPair is the replicated form of Pair. The expiry is an absolute
// Unix timestamp stamped once at write submission on the leader, so log
// replay is idempotent: an expired key can never resurrect, and a live
// key's lifetime never extends, no matter how often the entry is re-applied.
// All fields are exported because gob only encodes exported struct fields.
type commandPair struct {
	Key           []byte
	Value         []byte
	ExpiresAtUnix uint64 // absolute time.Unix expiry; 0 = persist forever
}

// command is the unit of replication. Every write API call encodes one
// command with gob and submits it through Raft.Apply; the FSM applies it to
// Badger once the entry is committed.
type command struct {
	Op      uint8
	Pairs   []commandPair
	Deletes [][]byte
}

func encodeCommand(cmd command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, fmt.Errorf("honeybadger: encode command: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (command, error) {
	var cmd command
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd)
	return cmd, err
}

// fsm is the Raft finite state machine. It is the only code path that
// mutates the local Badger database, and it runs strictly after log entries
// are committed by the cluster.
type fsm struct {
	db *DB
	// appliedIndex tracks the last Raft log index applied to Badger.
	appliedIndex atomic.Uint64
}

// Apply decodes the committed log entry and executes it against Badger in a
// single transaction. The returned error (if any) surfaces to the writer as
// the Response of the corresponding Raft ApplyFuture. On followers Raft
// drops the response, so failures are also logged to Config.LogOutput to
// keep a diverging node from going unnoticed.
func (f *fsm) Apply(log *raft.Log) interface{} {
	cmd, err := decodeCommand(log.Data)
	if err != nil {
		err = fmt.Errorf("honeybadger: decode command at index %d: %w", log.Index, err)
		f.db.logger.Printf("fsm: %v", err)
		return err
	}

	err = f.db.withStore(func(b *badger.DB) error {
		return b.Update(func(txn *badger.Txn) error {
			for _, p := range cmd.Pairs {
				entry := badger.NewEntry(p.Key, p.Value)
				if p.ExpiresAtUnix > 0 {
					// Absolute expiry, deterministic on every
					// node and idempotent under replay. Do NOT
					// use WithTTL here: it would restamp a
					// fresh TTL at every apply.
					entry.ExpiresAt = p.ExpiresAtUnix
				}
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
			}
			for _, key := range cmd.Deletes {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		err = fmt.Errorf("honeybadger: apply command at index %d: %w", log.Index, err)
		if !errors.Is(err, ErrClosed) {
			// ErrClosed is expected noise during shutdown.
			f.db.logger.Printf("fsm: %v", err)
		}
		return err
	}
	f.appliedIndex.Store(log.Index)
	return nil
}

// Snapshot captures the state of the Badger database for Raft log
// compaction and follower catch-up. The heavy lifting happens in Persist,
// which may run concurrently with Apply.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{db: f.db}, nil
}

// Restore replaces the entire local state with a snapshot received from the
// leader. The restore is staged: the snapshot is loaded into a fresh Badger
// instance in a temporary directory first, and only on success is the live
// database swapped under the store lock. On any failure the staging
// directory is removed, the old store keeps serving reads, and the error is
// returned to Raft (which retries InstallSnapshot). After a successful
// restore, Raft replays any log entries past the snapshot index through
// Apply as usual.
//
// Restore runs on a Raft goroutine and never concurrently with Apply, but
// it can race client reads and an in-flight snapshot Persist; the swap
// therefore takes the store lock exclusively while the staging phase runs
// lock-free against the (untouched) old store.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	db := f.db
	liveDir := db.badgerDir

	// A custom ValueDir layout cannot be relocated as a single directory;
	// fall back to a simple in-place restore for that exotic case.
	if vd := db.badgerOpts.ValueDir; vd != "" && vd != liveDir {
		return f.restoreInPlace(rc)
	}

	stagingDir := liveDir + ".restore-tmp"
	backupDir := liveDir + ".restore-old"

	// ---------------- staging phase (old store keeps serving) ----------
	if err := os.RemoveAll(stagingDir); err != nil {
		return f.restoreErr("clean staging dir", err)
	}
	stagingOpts := db.badgerOpts
	stagingOpts.Dir = stagingDir
	stagingOpts.ValueDir = stagingDir
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return f.restoreErr("create staging dir", err)
	}
	stage, err := badger.Open(stagingOpts)
	if err != nil {
		os.RemoveAll(stagingDir)
		return f.restoreErr("open staging badger", err)
	}
	if err := stage.Load(rc, 256); err != nil {
		stage.Close()
		os.RemoveAll(stagingDir)
		return f.restoreErr("load snapshot into staging badger", err)
	}
	if err := stage.Close(); err != nil {
		os.RemoveAll(stagingDir)
		return f.restoreErr("close staging badger", err)
	}

	// ---------------- swap phase (exclusive store lock) ----------------
	db.storeMu.Lock()
	defer db.storeMu.Unlock()

	os.RemoveAll(backupDir) // best-effort cleanup of crash residue
	if db.store != nil {
		if err := db.store.Close(); err != nil {
			db.logger.Printf("fsm: close badger for restore swap: %v", err)
		}
		db.store = nil
	}

	if err := os.Rename(liveDir, backupDir); err != nil {
		db.reopenLocked() // best effort: keep the old data serving
		return f.restoreErr("move live dir aside", err)
	}
	if err := os.Rename(stagingDir, liveDir); err != nil {
		if rerr := os.Rename(backupDir, liveDir); rerr != nil {
			db.logger.Printf("fsm: roll back badger dir after failed restore: %v", rerr)
		}
		db.reopenLocked()
		return f.restoreErr("move staging dir into place", err)
	}
	store, err := badger.Open(db.badgerOpts)
	if err != nil {
		os.RemoveAll(liveDir)
		if rerr := os.Rename(backupDir, liveDir); rerr != nil {
			db.logger.Printf("fsm: roll back badger dir after failed restore: %v", rerr)
		}
		db.reopenLocked()
		return f.restoreErr("open restored badger", err)
	}
	os.RemoveAll(backupDir)

	db.store = store
	f.appliedIndex.Store(0)
	return nil
}

// restoreErr logs a Restore failure (Raft drops the FSM response path for
// Restore, so the log is the only place the failure is visible) and returns
// the wrapped error to Raft, which will retry InstallSnapshot.
func (f *fsm) restoreErr(step string, err error) error {
	wrapped := fmt.Errorf("honeybadger: restore: %s: %w", step, err)
	f.db.logger.Printf("fsm: %v", wrapped)
	return wrapped
}

// reopenLocked best-effort reopens the Badger database after a failed
// restore swap so the node keeps serving whatever data is on disk.
// Caller must hold db.storeMu exclusively.
func (db *DB) reopenLocked() {
	store, err := badger.Open(db.badgerOpts)
	if err != nil {
		db.logger.Printf("fsm: reopen badger after failed restore: %v", err)
		db.store = nil
		return
	}
	db.store = store
}

// restoreInPlace is the fallback restore for custom ValueDir layouts: the
// live database is closed, its directories are wiped, and the snapshot is
// loaded directly. Unlike the staged path, a load failure here leaves the
// node empty until Raft retries.
func (f *fsm) restoreInPlace(rc io.ReadCloser) error {
	db := f.db

	db.storeMu.Lock()
	defer db.storeMu.Unlock()

	if db.store != nil {
		if err := db.store.Close(); err != nil {
			db.store = nil
			return f.restoreErr("close badger", err)
		}
		db.store = nil
	}

	dirs := []string{db.badgerDir}
	dirs = append(dirs, db.badgerOpts.ValueDir)
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return f.restoreErr("wipe badger dir", err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return f.restoreErr("recreate badger dir", err)
		}
	}

	store, err := badger.Open(db.badgerOpts)
	if err != nil {
		return f.restoreErr("reopen badger", err)
	}
	if err := store.Load(rc, 256); err != nil {
		store.Close()
		return f.restoreErr("load snapshot", err)
	}
	db.store = store
	f.appliedIndex.Store(0)
	return nil
}

// fsmSnapshot streams a Badger backup into a Raft snapshot sink.
type fsmSnapshot struct {
	db *DB
}

// Persist writes a full Badger backup to sink. It holds the store lock in
// shared mode so a concurrent Restore cannot close the database mid-backup.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	s.db.storeMu.RLock()
	defer s.db.storeMu.RUnlock()

	if s.db.store == nil {
		if err := sink.Cancel(); err != nil {
			return err
		}
		return ErrClosed
	}
	if _, err := s.db.store.Backup(sink, 0); err != nil {
		if cerr := sink.Cancel(); cerr != nil {
			return cerr
		}
		return fmt.Errorf("honeybadger: backup badger into snapshot: %w", err)
	}
	return sink.Close()
}

// Release is a no-op; the snapshot holds no resources of its own.
func (s *fsmSnapshot) Release() {}
