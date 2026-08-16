package honeybadger

import (
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
)

// ---------------------------------------------------------------------------
// Sentinel errors. These sentinels classify the common operational
// failures of this package — missing keys, wrong node, closed DB, missing
// leader, invalid arguments — so errors.Is always works for them. They do
// NOT classify every failure: errors from the storage engine, the Raft
// transport, snapshots, or user callbacks may be returned directly or
// wrapped with package context via %w; they are never remapped to a
// sentinel, and their original identity is always preserved for
// errors.Is/errors.As.
// ---------------------------------------------------------------------------

// ErrKeyNotFound is returned by reads when the requested key does not exist
// (or has expired).
var ErrKeyNotFound = errors.New("honeybadger: key not found")

// ErrNotLeader is returned by write, cluster-membership and linearizable
// read operations when they are invoked on a node that is not the current
// Raft leader. The returned error is a *NotLeaderError carrying the
// leader's ID and address when known; errors.Is(err, ErrNotLeader) always
// matches.
var ErrNotLeader = errors.New("honeybadger: not the leader")

// ErrClosed is returned by operations invoked on a DB that has been closed.
var ErrClosed = errors.New("honeybadger: database is closed")

// ErrNoLeader is returned (wrapped) by WaitForLeader when no cluster leader
// becomes known within the timeout, and by Open with NewCluster when the
// first election does not complete in time.
var ErrNoLeader = errors.New("honeybadger: no leader known")

// ErrInvalidArgument is returned (wrapped) when a call is rejected before
// any Raft entry is submitted: empty keys, non-positive TTLs, negative scan
// limits, unknown read modes, duplicate keys inside one Batch, malformed
// Mutations, or unknown options.
var ErrInvalidArgument = errors.New("honeybadger: invalid argument")

// NotLeaderError is the typed form of ErrNotLeader. It carries the ID and
// Raft address of the current leader when this node knows them, so callers
// can redirect the operation. The address is the leader's Raft transport
// address: a routing hint, not necessarily an application endpoint. Use
// errors.As to extract it and errors.Is to compare against ErrNotLeader.
type NotLeaderError struct {
	// LeaderID is the node ID of the current leader, or "" if unknown.
	LeaderID string
	// LeaderAddr is the Raft address of the current leader, or "" if unknown.
	LeaderAddr string
}

// Error implements error.
func (e *NotLeaderError) Error() string {
	switch {
	case e.LeaderID != "" && e.LeaderAddr != "":
		return fmt.Sprintf("%s (leader %s at %s)", ErrNotLeader, e.LeaderID, e.LeaderAddr)
	case e.LeaderAddr != "":
		return fmt.Sprintf("%s (leader at %s)", ErrNotLeader, e.LeaderAddr)
	default:
		return ErrNotLeader.Error()
	}
}

// Unwrap returns ErrNotLeader so errors.Is(err, ErrNotLeader) matches.
func (e *NotLeaderError) Unwrap() error { return ErrNotLeader }

// ---------------------------------------------------------------------------
// Cluster membership and status types.
// ---------------------------------------------------------------------------

// NodeRole is a node's suffrage in the Raft cluster configuration.
type NodeRole uint8

const (
	// RoleNone means the node is not (or not yet) part of the cluster
	// configuration — e.g. a freshly opened node that has not been added.
	RoleNone NodeRole = iota
	// RoleVoter is a full voting cluster member.
	RoleVoter
	// RoleNonvoter is a non-voting member (receives logs, never counts
	// toward quorum).
	RoleNonvoter
	// RoleStaging is a non-voter being staged before promotion to voter.
	RoleStaging
)

// String returns "None", "Voter", "Nonvoter", or "Staging". Unknown
// values stringify honestly, e.g. "NodeRole(9)".
func (r NodeRole) String() string {
	switch r {
	case RoleNone:
		return "None"
	case RoleVoter:
		return "Voter"
	case RoleNonvoter:
		return "Nonvoter"
	case RoleStaging:
		return "Staging"
	default:
		return fmt.Sprintf("NodeRole(%d)", uint8(r))
	}
}

// Node describes a single server in the Raft cluster.
type Node struct {
	// ID is the node's unique ID (Config.NodeID).
	ID string
	// RaftAddr is the node's advertised Raft transport address.
	RaftAddr string
	// Role is the node's suffrage in the cluster configuration. It is
	// RoleNone when the node's membership is not known (e.g. Status on a
	// node that has not been added to the cluster yet). AddVoter requires
	// RoleNone or RoleVoter here: joined nodes always become voters.
	Role NodeRole
}

// State is the Raft lifecycle state of this node.
type State uint8

const (
	// StateFollower means the node follows a leader.
	StateFollower State = iota
	// StateCandidate means the node is campaigning for leadership.
	StateCandidate
	// StateLeader means the node is the current cluster leader.
	StateLeader
	// StateShutdown means the node's Raft instance has been shut down.
	StateShutdown
)

// String returns "Follower", "Candidate", "Leader", or "Shutdown".
// Unknown values stringify honestly, e.g. "State(7)".
func (s State) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	case StateShutdown:
		return "Shutdown"
	default:
		return fmt.Sprintf("State(%d)", uint8(s))
	}
}

// stateFromRaft maps a raft.RaftState onto the exported State type.
func stateFromRaft(s raft.RaftState) State {
	switch s {
	case raft.Candidate:
		return StateCandidate
	case raft.Leader:
		return StateLeader
	case raft.Shutdown:
		return StateShutdown
	default:
		return StateFollower
	}
}

// Status is a typed snapshot of this node's view of the cluster, returned
// by DB.Status.
type Status struct {
	// Local describes this node: its configured ID and advertised Raft
	// address, and its role in the cluster configuration (RoleNone until
	// the node has been added to a cluster).
	Local Node
	// State is this node's current Raft state.
	State State
	// Leader is the current cluster leader as known by this node, or nil
	// if no leader is (yet) known.
	Leader *Node
	// AppliedIndex is the index of the last Raft log entry applied to the
	// local Badger database by the FSM. It resets to 0 when a snapshot
	// restore replaces the local state and climbs again as Raft replays
	// entries.
	AppliedIndex uint64
}

// Entry is a single key/value pair returned by ScanPrefixBytes. Keys and
// values are copies and stay valid after the call returns. Scans never
// report TTLs; use GetBytes for point reads.
type Entry struct {
	Key   []byte
	Value []byte
}
