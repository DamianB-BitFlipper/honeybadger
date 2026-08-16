package honeybadger

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// Reads. Tier 1 (Get) is strictly consistent by default; Tier 2 takes an
// explicit ReadOptions with a safe zero value (ReadLinearizable) governing
// point reads, scans and views uniformly.

// prepareRead enforces the consistency side of ReadOptions: linearizable
// reads must run on the leader behind a Raft barrier; local reads pass.
func (db *DB) prepareRead(ro ReadOptions) error {
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
// Callers must have passed prepareRead first.
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
	if err := db.prepareRead(ro); err != nil {
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
	if err := db.prepareRead(opts.Read); err != nil {
		return nil, err
	}
	var entries []Entry
	err = db.withStore(func(b *badger.DB) error {
		return b.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
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
	if err := db.prepareRead(ro); err != nil {
		return err
	}
	return db.withStore(func(b *badger.DB) error {
		return b.View(fn)
	})
}

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
