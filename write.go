package honeybadger

import (
	"fmt"
	"time"
)

// Writes. All writes are replicated through Raft and applied to Badger by
// the FSM only after the log entry is committed. Arguments are validated
// before any Raft entry is submitted.

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
	return db.apply(command{Pairs: []commandPair{{Key: key, Value: value, ExpiresAtUnix: expiresAt}}})
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
	return db.apply(command{Deletes: [][]byte{key}})
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
	seenKeys := make(map[string]struct{}, len(mutations))
	setPairs := make([]commandPair, 0, len(mutations))
	var deleteKeys [][]byte
	for i, m := range mutations {
		if len(m.key) == 0 {
			return fmt.Errorf("%w: batch mutation %d: key must not be empty", ErrInvalidArgument, i)
		}
		if _, dup := seenKeys[string(m.key)]; dup {
			return fmt.Errorf("%w: batch mutation %d: duplicate key %q", ErrInvalidArgument, i, m.key)
		}
		seenKeys[string(m.key)] = struct{}{}
		switch m.kind {
		case mutSet:
			expiresAt, err := resolveSetOptions(m.opts)
			if err != nil {
				return fmt.Errorf("batch mutation %d: %w", i, err)
			}
			setPairs = append(setPairs, commandPair{Key: m.key, Value: m.value, ExpiresAtUnix: expiresAt})
		case mutDelete:
			deleteKeys = append(deleteKeys, m.key)
		default:
			return fmt.Errorf("%w: batch mutation %d: not created by SetOp/SetBytesOp/DeleteOp/DeleteBytesOp",
				ErrInvalidArgument, i)
		}
	}
	return db.apply(command{Pairs: setPairs, Deletes: deleteKeys})
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
