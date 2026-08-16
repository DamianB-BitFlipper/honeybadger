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

// commandPair is the on-log representation of a set mutation: key, value
// and expiry. ExpiresAtUnix is computed once before Raft.Apply on the
// leader (an absolute Unix timestamp) and replicated verbatim, so log
// replay is idempotent: an expired key can never resurrect, and a live
// key's lifetime never extends, no matter how often the entry is
// re-applied.
// All fields are exported because gob only encodes exported struct fields.
type commandPair struct {
	Key           []byte
	Value         []byte
	ExpiresAtUnix uint64 // absolute time.Unix expiry; 0 = persist forever
}

// command is the unit of replication. Every write API call encodes one
// command with gob and submits it through Raft.Apply; the FSM applies it
// to Badger once the entry is committed. A command carries no opcode: its
// shape is the operation — Pairs are set, Deletes keys are deleted. Field
// names are persisted in the gob wire format; do not rename them.
//
// Command semantics are load-bearing for snapshot correctness (see
// fsm.Snapshot): every command must stay a blind write that never reads
// prior state and is idempotent under duplication. Any future command that
// reads prior state (compare-and-swap, increment) or stamps relative time
// MUST rework the snapshot path before merging.
type command struct {
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

// snapshotLoadMaxPendingWrites is honeybadger's chosen pending-writes
// limit passed to badger.Load while loading a snapshot into a fresh
// database during restore.
const snapshotLoadMaxPendingWrites = 256

// fsm is the Raft finite state machine: the bridge between the committed
// Raft log and the local Badger database. Replicated mutations enter
// Badger only through Apply, strictly after their log entry is committed;
// Restore is the separate whole-state replacement path.
type fsm struct {
	db *DB
	// appliedIndex is the log index of the most recent command whose
	// Badger transaction succeeded. It is not a contiguous high-water
	// mark: Raft keeps applying entries after one fails, so a success at
	// N+1 advances it past a failed N, and decode or transaction failures
	// leave it unchanged. It counts command-log applies by this FSM only —
	// not Raft's general applied index, which also covers configuration
	// changes and barrier entries. A successful Restore resets it to 0
	// once the restored store is published, because FSM.Restore receives
	// no snapshot index (0 does not mean the restored data is empty); a
	// failed restore leaves the prior counter untouched, even if the
	// store was left nil.
	appliedIndex atomic.Uint64
}

// Apply executes one committed command against Badger in exactly one
// badger.Update transaction: every set and delete in the command shares
// the transaction, so the command applies all-or-nothing, and appliedIndex
// advances only after the transaction commits. withStore holds storeMu in
// shared mode for the whole transaction, pinning the live database against
// a concurrent Restore swap or Close.
//
// Consensus commit has already happened when Apply runs: the return value
// is an opaque client response, not an error that aborts or retries the
// log entry. A decode or Badger failure leaves local state unchanged for
// that command while Raft still advances its own application progression.
// The response reaches the leader's caller via the ApplyFuture and is
// dropped on followers, so failures are also logged to Config.LogOutput to
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
					// Assign the leader-stamped absolute Unix
					// expiry directly: identical on every node
					// and stable across restart replay and
					// post-snapshot replay. Do NOT use WithTTL
					// here: it would restamp a fresh TTL from
					// each node's own wall clock at every apply.
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

// Snapshot starts a Raft snapshot. Raft labels the snapshot with its own
// FSM progression index I (not fsm.appliedIndex, which counts only
// successful commands); this method only retains the *DB handle and
// captures no state itself. The Badger state is streamed later by Persist,
// which Raft may run concurrently with Apply, so the backup may capture a
// later, transactionally complete prefix of command effects (through some
// J >= I) while a restore still replays log[I+1..].
//
// Convergence holds ONLY because every command is a blind write that never
// reads prior state and is idempotent under duplication — including
// absolute expiry timestamps with no apply-time clock — so replaying the
// overlapping sequence log[I+1..J] after the restore converges to the same
// final state even where the backup already contains those effects.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{db: f.db}, nil
}

// Restore replaces the entire local state with snapshot data. Raft invokes
// it at startup (recovering the node's own latest snapshot) and on a
// follower that has fallen too far behind (installing a snapshot streamed
// from the leader). After a successful restore Raft replays the log
// entries past the snapshot index through Apply as usual.
//
// For the normal layout (ValueDir co-located with Dir) the restore runs in
// two phases. Staging loads the snapshot into a fresh Badger instance in a
// temporary directory and closes it, while the old store keeps serving
// reads. The swap then runs under the store lock held exclusively: close
// the live store, rename live dir to backup dir, rename staging dir to
// live dir, open the restored database, publish it as db.store, remove the
// backup. A separate ValueDir skips staging and uses the destructive
// in-place path documented at restoreInPlace.
//
// The failure guarantees below are phase-specific to the staged path. A
// staging failure leaves the old store untouched. A swap failure before
// publish rolls the directories back and reopens the old data only
// best-effort, and can leave db.store == nil (reads fail with ErrClosed
// until a later restore succeeds). Either way the error marks this
// restore as failed for the invoking Raft path. Cleanup failures are
// logged or ignored; the swap is not crash-atomic: a crash mid-swap can
// leave either generation on disk, and the next restore attempts to clean
// up both leftover directories (a staging cleanup failure is returned;
// backup cleanup is best-effort).
//
// Restore runs on a Raft goroutine serialized with Apply and Snapshot, but
// it can overlap an already-running Persist, client reads, and Close; the
// swap's exclusive lock waits those out and excludes new ones until
// publish or rollback completes.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	db := f.db
	liveDir := db.badgerDir

	// The staged swap relocates the database with single directory
	// renames, which is only safe when the LSM tree and value log live in
	// the same directory. A custom ValueDir outside Dir breaks that
	// assumption, so such layouts restore in place instead.
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
	if err := stage.Load(rc, snapshotLoadMaxPendingWrites); err != nil {
		stage.Close()
		os.RemoveAll(stagingDir)
		return f.restoreErr("load snapshot into staging badger", err)
	}
	if err := stage.Close(); err != nil {
		os.RemoveAll(stagingDir)
		return f.restoreErr("close staging badger", err)
	}

	// ---------------- swap phase (exclusive store lock) ----------------
	// While storeMu is held exclusively, withStore/closeStore would
	// self-deadlock: this section touches db.store directly and uses the
	// *Locked helpers, which require the caller to own the lock.
	db.storeMu.Lock()
	defer db.storeMu.Unlock()

	// Best-effort cleanup of crash residue from an interrupted earlier
	// restore; a leftover backup directory is removed again below.
	os.RemoveAll(backupDir)
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
		f.rollbackAndReopenLocked(backupDir, liveDir)
		return f.restoreErr("move staging dir into place", err)
	}
	store, err := badger.Open(db.badgerOpts)
	if err != nil {
		os.RemoveAll(liveDir)
		f.rollbackAndReopenLocked(backupDir, liveDir)
		return f.restoreErr("open restored badger", err)
	}
	// Swap complete: drop the old generation (best-effort; a leftover is
	// removed at the start of the next restore).
	os.RemoveAll(backupDir)

	db.store = store
	// The snapshot index is unknown here (FSM.Restore receives none), so
	// the applied index restarts at 0 — not because the data is empty.
	f.appliedIndex.Store(0)
	return nil
}

// rollbackAndReopenLocked best-effort moves the backup directory back into
// the live location and reopens whatever data is on disk after a failed
// restore swap. Caller must hold db.storeMu exclusively.
func (f *fsm) rollbackAndReopenLocked(backupDir, liveDir string) {
	if err := os.Rename(backupDir, liveDir); err != nil {
		f.db.logger.Printf("fsm: roll back badger dir after failed restore: %v", err)
	}
	f.db.reopenLocked()
}

// restoreErr logs a Restore failure with its step context — keeping
// follower/background failures diagnosable locally — and returns the
// wrapped error to the invoking Raft restore path: at startup Raft may try
// another local snapshot or fail startup, and an InstallSnapshot is
// treated as failed and may be retried later.
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

// restoreInPlace is the restore path for layouts whose ValueDir lives
// outside Dir, where a single directory rename cannot relocate the
// database. Under the store lock held exclusively it closes the live
// store, wipes and recreates both configured directories, reopens Badger,
// and loads the snapshot directly. There is no staged rollback: a failure
// leaves the store unavailable (db.store == nil, so reads fail with
// ErrClosed until a later restore succeeds) and may leave partially
// restored files on disk.
func (f *fsm) restoreInPlace(rc io.ReadCloser) error {
	db := f.db

	// Exclusive storeMu held: withStore/closeStore would self-deadlock, so
	// this section touches db.store directly.
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
	if err := store.Load(rc, snapshotLoadMaxPendingWrites); err != nil {
		store.Close()
		return f.restoreErr("load snapshot", err)
	}
	db.store = store
	f.appliedIndex.Store(0)
	return nil
}

// fsmSnapshot streams a Badger backup into a Raft snapshot sink. It stores
// only the *DB; the then-current store is resolved inside Persist, under
// the shared store lock.
type fsmSnapshot struct {
	db *DB
}

// Persist writes a full Badger backup of the then-current store to sink.
// It holds the store lock in shared mode for the whole backup: that keeps
// the resolved Badger instance open while client writes and Applies
// proceed (Apply takes the same lock in shared mode), and a Restore swap
// or Close waits for the backup to finish.
//
// Sink ownership: on success Persist closes the sink; when the store is
// gone (node closing or being restored) or the backup fails, it cancels
// the sink instead, and a cancel failure supersedes the primary error
// because the snapshot is then in an unknown state.
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
		if cancelErr := sink.Cancel(); cancelErr != nil {
			return cancelErr
		}
		return fmt.Errorf("honeybadger: backup badger into snapshot: %w", err)
	}
	return sink.Close()
}

// Release is a no-op: the snapshot owns no resources.
func (s *fsmSnapshot) Release() {}
