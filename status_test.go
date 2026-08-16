package honeybadger

import (
	"fmt"
	"testing"
)

// TestStatus verifies the typed status snapshot.
func TestStatus(t *testing.T) {
	port := freePort(t)
	db := testNode(t, port, true)

	st, err := db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Local.ID != fmt.Sprintf("node-%d", port) {
		t.Fatalf("Status.Local.ID = %q", st.Local.ID)
	}
	if st.Local.RaftAddr != fmt.Sprintf("127.0.0.1:%d", port) {
		t.Fatalf("Status.Local.RaftAddr = %q", st.Local.RaftAddr)
	}
	if st.Local.Role != RoleVoter {
		t.Fatalf("Status.Local.Role = %s, want Voter", st.Local.Role)
	}
	if st.State != StateLeader || st.State.String() != "Leader" {
		t.Fatalf("Status.State = %s (%q), want Leader", st.State, st.State)
	}
	if st.Leader == nil || st.Leader.ID != st.Local.ID || st.Leader.RaftAddr != st.Local.RaftAddr {
		t.Fatalf("Status.Leader = %+v, want the local node", st.Leader)
	}

	// AppliedIndex advances as writes are committed.
	before := st.AppliedIndex
	const writes = 5
	for i := 0; i < writes; i++ {
		if err := db.Set(fmt.Sprintf("ai-%d", i), "v"); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	st, err = db.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.AppliedIndex < before+writes {
		t.Fatalf("AppliedIndex = %d before, %d after %d writes", before, st.AppliedIndex, writes)
	}
	stats := db.RawRaftStats()
	if stats["honeybadger_applied_index"] != fmt.Sprintf("%d", st.AppliedIndex) {
		t.Fatalf("RawRaftStats()[honeybadger_applied_index] = %q, want %d",
			stats["honeybadger_applied_index"], st.AppliedIndex)
	}

	if StateFollower.String() != "Follower" || StateCandidate.String() != "Candidate" ||
		StateShutdown.String() != "Shutdown" {
		t.Fatalf("State.String wrong: %s %s %s", StateFollower, StateCandidate, StateShutdown)
	}
	if got := State(7).String(); got != "State(7)" {
		t.Fatalf("State(7).String() = %q, want honest unknown rendering", got)
	}
	if got := ReadMode(42).String(); got != "ReadMode(42)" {
		t.Fatalf("ReadMode(42).String() = %q, want honest unknown rendering", got)
	}
}
