package honeybadger

import (
	"bytes"
	"fmt"
	"slices"
	"time"
)

// Open options (sealed).

// OpenOption customizes how Open starts a node. It is a sealed interface:
// the only supported value is the one returned by NewCluster.
type OpenOption interface {
	// sealed: implementations live in this package only.
	openOption()
}

type newClusterOption struct{}

func (newClusterOption) openOption() {}

// NewCluster returns the OpenOption that forms a brand-new cluster: the
// opened node bootstraps a single-server Raft configuration containing
// itself and Open blocks until the first election completes, so the
// returned DB is immediately usable.
//
// Pass it for exactly the first node of a new cluster. All other nodes are
// opened without any option (plain Open never bootstraps or forms a
// cluster) and are added through DB.AddVoter on the leader.
//
// Re-opening a DataDir that already contains cluster state with
// NewCluster() is harmless: the stale bootstrap attempt is ignored and Open
// still waits for leadership, which makes it a safe choice for restarts of
// the bootstrap node.
func NewCluster() OpenOption { return newClusterOption{} }

// Set options (sealed).

// SetOption customizes a single write. It is a sealed interface: the only
// supported value is the one returned by WithTTL.
type SetOption interface {
	// sealed: implementations live in this package only.
	setOption()
}

type ttlOption struct{ ttl time.Duration }

func (ttlOption) setOption() {}

// WithTTL returns the SetOption that makes a written key expire ttl after
// the write is submitted on the leader. The TTL must be positive; writes
// with a non-positive TTL fail with ErrInvalidArgument before any Raft
// entry is submitted.
//
// The TTL is converted to an absolute expiry timestamp once, at write
// submission time on the leader, so every node applies the identical
// expiry and log replay after a restart can neither resurrect an expired
// key nor extend a live one. Note Badger tracks expirations with
// one-second granularity, so sub-second TTLs may expire almost
// immediately. Expired keys behave exactly like missing keys on read.
//
// Passing WithTTL more than once in a single call makes the last one win.
func WithTTL(ttl time.Duration) SetOption { return ttlOption{ttl} }

// resolveSetOptions validates opts and converts an optional TTL into the
// absolute Unix expiry replicated with the write command (0 = persist
// forever).
func resolveSetOptions(opts []SetOption) (expiresAt uint64, err error) {
	var ttl time.Duration
	hasTTL := false
	for _, o := range opts {
		switch o := o.(type) {
		case ttlOption:
			ttl = o.ttl
			hasTTL = true
		default:
			return 0, fmt.Errorf("%w: unknown set option %T", ErrInvalidArgument, o)
		}
	}
	if !hasTTL {
		return 0, nil
	}
	if ttl <= 0 {
		return 0, fmt.Errorf("%w: option WithTTL requires a positive TTL, got %s", ErrInvalidArgument, ttl)
	}
	return absExpiry(ttl), nil
}

// Batch mutations (sealed).

type mutKind uint8

const (
	mutSet mutKind = iota + 1
	mutDelete
)

// Mutation is a single operation inside a Batch: one SetOp/SetBytesOp or
// DeleteOp/DeleteBytesOp. It is a sealed value: only the constructors in
// this package produce valid Mutations, and Batch validates them (rejecting
// empty keys, non-positive TTLs and duplicate keys) before any Raft entry
// is submitted. The byte-slice constructors copy their arguments, so a
// Mutation is immutable once created.
type Mutation struct {
	kind  mutKind
	key   []byte
	value []byte
	opts  []SetOption
}

// SetOp returns a Mutation that stores key/value, optionally expiring after
// a TTL supplied via WithTTL.
func SetOp(key, value string, opts ...SetOption) Mutation {
	return Mutation{kind: mutSet, key: []byte(key), value: []byte(value), opts: slices.Clone(opts)}
}

// SetBytesOp is SetOp for byte slices. The key and value are copied.
func SetBytesOp(key, value []byte, opts ...SetOption) Mutation {
	return Mutation{kind: mutSet, key: bytes.Clone(key), value: bytes.Clone(value), opts: slices.Clone(opts)}
}

// DeleteOp returns a Mutation that removes key. Deleting a missing key is
// not an error.
func DeleteOp(key string) Mutation {
	return Mutation{kind: mutDelete, key: []byte(key)}
}

// DeleteBytesOp is DeleteOp for byte slices. The key is copied.
func DeleteBytesOp(key []byte) Mutation {
	return Mutation{kind: mutDelete, key: bytes.Clone(key)}
}

// Read consistency.

// ReadMode selects the consistency guarantee of a read. The zero value is
// ReadLinearizable, the safe default.
type ReadMode uint8

const (
	// ReadLinearizable is the strict, safe default: the read must run on
	// the leader (it fails with a *NotLeaderError on followers) and is
	// preceded by a Raft barrier, so all previously committed writes are
	// applied locally before the read. It never silently downgrades.
	ReadLinearizable ReadMode = iota
	// ReadLocal serves the read from this node's local Badger database
	// with no Raft round trip. It works on any node, leader or follower,
	// but a follower may briefly serve stale data while it catches up.
	// Choose it deliberately when staleness is acceptable.
	ReadLocal
)

// String returns "Linearizable" or "Local". Unknown values use
// "ReadMode(<n>)", e.g. "ReadMode(42)".
func (m ReadMode) String() string {
	switch m {
	case ReadLinearizable:
		return "Linearizable"
	case ReadLocal:
		return "Local"
	default:
		return fmt.Sprintf("ReadMode(%d)", uint8(m))
	}
}

// ReadOptions govern every read operation uniformly: GetWithOptions,
// GetBytes, ScanPrefixBytes and ViewBadger. The zero value is
// ReadOptions{} = a linearizable read with the configured apply timeout —
// always safe.
type ReadOptions struct {
	// Mode selects linearizable (default) or local reads.
	Mode ReadMode
	// Timeout bounds the Raft barrier performed by linearizable reads.
	// Zero means the configured Advanced.ApplyTimeout; a negative Timeout
	// is invalid and fails with ErrInvalidArgument. A nonnegative Timeout
	// has no effect with ReadLocal, which needs no barrier.
	Timeout time.Duration
}

// timeoutOr returns a positive ro.Timeout override, or def otherwise.
// Callers must reject a negative Timeout first (prepareRead does); here a
// negative value silently behaves like zero.
func (ro ReadOptions) timeoutOr(def time.Duration) time.Duration {
	if ro.Timeout > 0 {
		return ro.Timeout
	}
	return def
}

// Scan options.

// defaultScanLimit is the number of entries ScanPrefixBytes returns when
// ScanOptions.Limit is left at zero.
const defaultScanLimit = 100

// ScanOptions govern ScanPrefixBytes.
type ScanOptions struct {
	// Read selects the consistency guarantee, exactly as for point reads.
	// The zero value is a linearizable (leader-only) scan.
	Read ReadOptions
	// Limit caps the number of returned entries. Zero means the
	// conservative default of 100; a negative Limit is invalid and fails
	// with ErrInvalidArgument. A nonnegative Limit is ignored when
	// Unlimited is true.
	Limit int
	// Unlimited removes the limit entirely. It must be set explicitly, so
	// an accidental zero or negative Limit can never turn into an
	// unbounded scan.
	Unlimited bool
}

// resolveLimit validates the options and computes the effective limit
// (0 = no limit).
func (so ScanOptions) resolveLimit() (int, error) {
	if so.Limit < 0 {
		return 0, fmt.Errorf("%w: negative scan limit %d; use 0 for the default of %d or Unlimited: true",
			ErrInvalidArgument, so.Limit, defaultScanLimit)
	}
	if so.Unlimited {
		return 0, nil
	}
	if so.Limit == 0 {
		return defaultScanLimit, nil
	}
	return so.Limit, nil
}
