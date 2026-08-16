package honeybadger

import (
	"strconv"

	"github.com/hashicorp/raft"
)

// ---------------------------------------------------------------------------
// Status and statistics introspection.
// ---------------------------------------------------------------------------

// Status returns a typed snapshot of this node's view of the cluster: its
// own identity and role, its Raft state, the known leader (if any), and
// the index of the last log entry applied to local storage. Like every
// operational method, it fails with ErrClosed after Close.
func (db *DB) Status() (Status, error) {
	if err := db.checkOpen(); err != nil {
		return Status{}, err
	}
	st := Status{
		Local: Node{
			ID:       db.cfg.NodeID,
			RaftAddr: string(db.transport.LocalAddr()),
			Role:     RoleNone,
		},
		State:        stateFromRaft(db.raft.State()),
		AppliedIndex: db.fsm.appliedIndex.Load(),
	}
	if addr, id := db.raft.LeaderWithID(); id != "" {
		st.Leader = &Node{ID: string(id), RaftAddr: string(addr), Role: RoleVoter}
	}
	// Fill in the local role from the replicated configuration.
	future := db.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return Status{}, err
	}
	for _, s := range future.Configuration().Servers {
		if s.ID == raft.ServerID(db.cfg.NodeID) {
			st.Local.Role = roleFromRaft(s.Suffrage)
			break
		}
	}
	return st, nil
}

// RawRaftStats returns the raw, stringly Raft statistics map (keys are
// Raft's own and not covered by any stability guarantee) plus a
// "honeybadger_applied_index" entry reporting the last log index applied
// by the local FSM. Prefer Status for supported fields. Unlike operational
// methods, RawRaftStats is a passive snapshot that stays callable after
// Close and then returns the final statistics.
func (db *DB) RawRaftStats() map[string]string {
	stats := db.raft.Stats()
	stats["honeybadger_applied_index"] = strconv.FormatUint(db.fsm.appliedIndex.Load(), 10)
	return stats
}
