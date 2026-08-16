package honeybadger

import (
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	defaultApplyTimeout      = 10 * time.Second
	defaultSnapshotThreshold = 8192
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
