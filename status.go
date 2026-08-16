package honeybadger

import (
	"strconv"

	"github.com/hashicorp/raft"
)

// Status and statistics introspection.

// Status returns a typed snapshot of this node's view of the cluster: its
// own identity and role, its Raft state, the known leader (if any), and
// the index of the most recent command successfully applied to local
// storage. Like every operational method, it fails with ErrClosed after
// Close.
func (db *DB) Status() (Status, error) {
	if err := db.checkOpen(); err != nil {
		return Status{}, err
	}
	status := Status{
		Local: Node{
			ID:       db.cfg.NodeID,
			RaftAddr: string(db.transport.LocalAddr()),
			Role:     RoleNone,
		},
		State:        stateFromRaft(db.raft.State()),
		AppliedIndex: db.fsm.appliedIndex.Load(),
	}
	if leaderAddr, leaderID := db.raft.LeaderWithID(); leaderID != "" {
		status.Leader = &Node{ID: string(leaderID), RaftAddr: string(leaderAddr), Role: RoleVoter}
	}
	// Raft state does not expose this node's suffrage; read it from the
	// replicated cluster configuration.
	future := db.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return Status{}, err
	}
	for _, server := range future.Configuration().Servers {
		if server.ID == raft.ServerID(db.cfg.NodeID) {
			status.Local.Role = roleFromRaft(server.Suffrage)
			break
		}
	}
	return status, nil
}

// RawRaftStats returns the raw, stringly Raft statistics map (keys are
// Raft's own and not covered by any stability guarantee) plus a
// "honeybadger_applied_index" entry reporting the most recent command
// index whose local Badger transaction succeeded (same caveats as
// Status.AppliedIndex: a progress signal, not a freshness proof). Prefer
// Status for supported fields. Unlike operational methods, RawRaftStats is
// a passive snapshot that stays callable after Close and then returns the
// final statistics.
func (db *DB) RawRaftStats() map[string]string {
	stats := db.raft.Stats()
	stats["honeybadger_applied_index"] = strconv.FormatUint(db.fsm.appliedIndex.Load(), 10)
	return stats
}
