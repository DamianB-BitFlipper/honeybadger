package honeybadger

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// Cluster membership and introspection.

// AddVoter adds node to the cluster as a voter. It is called on the
// leader, on behalf of the joining node — not by the joining node itself —
// and returns a *NotLeaderError otherwise. The joining node must already
// be running (opened without NewCluster). node.Role must be left at
// RoleNone (or set to RoleVoter); any other value is rejected with
// ErrInvalidArgument because added nodes always become voters.
//
// A successful return confirms the membership change was committed, not
// that the new voter has caught up: it replays missed log entries (or
// receives a snapshot) asynchronously. When you need it to serve current
// data, poll an application-level readiness signal (e.g. a local read of a
// key you just wrote). Comparing its Status().AppliedIndex against the
// leader's is a progress hint only: a failed command or a snapshot restore
// leaves or resets the counter without meaning the node is behind.
func (db *DB) AddVoter(node Node) error {
	if node.ID == "" {
		return fmt.Errorf("%w: AddVoter: node ID must not be empty", ErrInvalidArgument)
	}
	if node.RaftAddr == "" {
		return fmt.Errorf("%w: AddVoter: node RaftAddr must not be empty", ErrInvalidArgument)
	}
	if node.Role != RoleNone && node.Role != RoleVoter {
		return fmt.Errorf("%w: AddVoter: role must be RoleNone or RoleVoter, got %s", ErrInvalidArgument, node.Role)
	}
	if err := db.checkOpen(); err != nil {
		return err
	}
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	future := db.raft.AddVoter(raft.ServerID(node.ID), raft.ServerAddress(node.RaftAddr), 0, db.cfg.Advanced.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// RemoveNode removes the node with the given ID from the cluster
// configuration. It must be called on the leader and returns a
// *NotLeaderError otherwise.
func (db *DB) RemoveNode(id string) error {
	if id == "" {
		return fmt.Errorf("%w: RemoveNode: node ID must not be empty", ErrInvalidArgument)
	}
	if err := db.checkOpen(); err != nil {
		return err
	}
	if db.raft.State() != raft.Leader {
		return db.notLeaderErr()
	}
	future := db.raft.RemoveServer(raft.ServerID(id), 0, db.cfg.Advanced.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return db.notLeaderErr()
		}
		return err
	}
	return nil
}

// Members returns the servers in the current Raft cluster configuration.
// It works on any node (the configuration is replicated to followers), but
// a follower's view may lag the leader's by a configuration change or two.
func (db *DB) Members() ([]Node, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	future := db.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	servers := future.Configuration().Servers
	nodes := make([]Node, 0, len(servers))
	for _, server := range servers {
		nodes = append(nodes, Node{
			ID:       string(server.ID),
			RaftAddr: string(server.Address),
			Role:     roleFromRaft(server.Suffrage),
		})
	}
	return nodes, nil
}

// roleFromRaft maps a raft.ServerSuffrage onto the exported NodeRole type.
func roleFromRaft(suffrage raft.ServerSuffrage) NodeRole {
	switch suffrage {
	case raft.Voter:
		return RoleVoter
	case raft.Nonvoter:
		return RoleNonvoter
	case raft.Staging:
		return RoleStaging
	default:
		return RoleNone
	}
}

// Snapshot forces a local Raft snapshot of this node's state, compacting
// its log (one SnapshotThreshold worth of trailing log entries is
// retained). It may be called on any node, leader or follower; each node's
// snapshot covers its own applied state. It is rarely needed in practice
// because snapshots are taken automatically once the log grows past
// SnapshotThreshold, but it is useful for tests and maintenance.
func (db *DB) Snapshot() error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	return db.raft.Snapshot().Error()
}

// WaitForLeader blocks until this node knows of a cluster leader (or the
// timeout expires) and returns that leader. When no leader becomes known
// in time, the error wraps ErrNoLeader; after Close it fails with
// ErrClosed.
func (db *DB) WaitForLeader(timeout time.Duration) (Node, error) {
	return db.waitForLeader(timeout)
}

// leaderPollInterval is how often waitForLeader re-checks whether Raft has
// learned a cluster leader.
const leaderPollInterval = 20 * time.Millisecond

// waitForLeader is the internal implementation shared by WaitForLeader and
// the NewCluster startup wait.
func (db *DB) waitForLeader(timeout time.Duration) (Node, error) {
	deadline := time.Now().Add(timeout)
	for {
		if err := db.checkOpen(); err != nil {
			return Node{}, err
		}
		if leaderAddr, leaderID := db.raft.LeaderWithID(); leaderID != "" {
			return Node{ID: string(leaderID), RaftAddr: string(leaderAddr), Role: RoleVoter}, nil
		}
		if !time.Now().Before(deadline) {
			return Node{}, fmt.Errorf("%w after %s", ErrNoLeader, timeout)
		}
		time.Sleep(leaderPollInterval)
	}
}
