package honeybadger

import (
	"bytes"
	"encoding/gob"
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

// command is the unit of replication. Every write API call encodes one
// command with gob and submits it through Raft.Apply; the FSM applies it to
// Badger once the entry is committed.
type command struct {
	Op      uint8
	Pairs   []Pair
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
// the Response of the corresponding Raft ApplyFuture.
func (f *fsm) Apply(log *raft.Log) interface{} {
	cmd, err := decodeCommand(log.Data)
	if err != nil {
		return fmt.Errorf("honeybadger: decode command at index %d: %w", log.Index, err)
	}

	err = f.db.withStore(func(b *badger.DB) error {
		return b.Update(func(txn *badger.Txn) error {
			for _, p := range cmd.Pairs {
				entry := badger.NewEntry(p.Key, p.Value)
				if p.TTL > 0 {
					entry = entry.WithTTL(p.TTL)
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
		return fmt.Errorf("honeybadger: apply command at index %d: %w", log.Index, err)
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

// Restore replaces the entire local state with the snapshot received from
// the leader: the Badger database is closed, its directory wiped, reopened,
// and loaded from the snapshot stream. Raft replays any log entries past
// the snapshot index through Apply afterwards.
//
// Restore runs on a Raft goroutine and never concurrently with Apply, but
// it can race client reads and an in-flight snapshot Persist, so it takes
// the store lock exclusively.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.db.storeMu.Lock()
	defer f.db.storeMu.Unlock()

	if f.db.store != nil {
		if err := f.db.store.Close(); err != nil {
			f.db.store = nil
			return fmt.Errorf("honeybadger: close badger for restore: %w", err)
		}
		f.db.store = nil
	}

	dirs := []string{f.db.badgerDir}
	if vd := f.db.badgerOpts.ValueDir; vd != "" && vd != f.db.badgerDir {
		dirs = append(dirs, vd)
	}
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("honeybadger: wipe badger dir for restore: %w", err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("honeybadger: recreate badger dir for restore: %w", err)
		}
	}

	store, err := badger.Open(f.db.badgerOpts)
	if err != nil {
		return fmt.Errorf("honeybadger: reopen badger for restore: %w", err)
	}
	if err := store.Load(rc, 256); err != nil {
		store.Close()
		return fmt.Errorf("honeybadger: load snapshot into badger: %w", err)
	}
	f.db.store = store
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
