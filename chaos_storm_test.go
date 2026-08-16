// Concurrent write-storm chaos tests. Both scenarios skip under -short.
package honeybadger

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestChaosWriteStormJoinDuring hammers the leader with 8 goroutines x 50
// disjoint Sets and joins a 4th node in the middle of the storm. Afterwards
// all 4 nodes must serve byte-identical values for all 400 keys, proving
// the late joiner also received every entry committed before it joined.
func TestChaosWriteStormJoinDuring(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos: skipping long-running scenario in -short mode")
	}
	nodes := chaosCluster(t, "storm", 0)

	const writers, perWriter = 8, 50
	expected := make(map[string]string, writers*perWriter)
	for w := 0; w < writers; w++ {
		for k := 0; k < perWriter; k++ {
			key := fmt.Sprintf("storm/w%d/k%03d", w, k)
			expected[key] = fmt.Sprintf("storm-val-%d-%03d", w, k)
		}
	}

	writeErrs := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for k := 0; k < perWriter; k++ {
				key := fmt.Sprintf("storm/w%d/k%03d", w, k)
				val := fmt.Sprintf("storm-val-%d-%03d", w, k)
				if err := chaosSetOnAnyLeader(nodes, key, val); err != nil && writeErrs[w] == nil {
					writeErrs[w] = err
				}
			}
		}(w)
	}
	close(start)

	// Join a 4th node while the storm is in flight.
	n4 := chaosOpen(t, "storm-4", false, 0)
	chaosJoin(t, nodes, n4)
	all := []*chaosNode{nodes[0], nodes[1], nodes[2], n4}

	wg.Wait()
	for w, err := range writeErrs {
		if err != nil {
			t.Fatalf("chaos: storm writer %d failed: %v", w, err)
		}
	}
	if _, err := n4.db.WaitForLeader(45 * time.Second); err != nil {
		t.Fatalf("chaos: late-joining node %q: %v", n4.cfg.NodeID, err)
	}

	chaosWaitConverged(t, chaosConvergeTO, "storm/", expected, all...)
}

// TestChaosMixedOpStorm interleaves Set/Delete/Batch/TTL operations on the
// leader while readers hammer the followers, then compares the full
// keyspace of every node against an in-test model of the expected state.
// TTLs are 120s so no key can expire before the comparison completes.
func TestChaosMixedOpStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos: skipping long-running scenario in -short mode")
	}
	nodes := chaosCluster(t, "mixed", 0)

	rng := rand.New(rand.NewSource(20260815))
	const keyspace, numOps = 150, 600
	keyFn := func(i int) string { return fmt.Sprintf("mixed/k%03d", i) }
	model := make(map[string]string, keyspace)

	// Concurrent readers on the followers for the duration of the storm:
	// local Get must only ever return a value or ErrKeyNotFound.
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	var readErrCount atomic.Int32
	firstErr := make(chan string, 1)
	for ni, n := range []*chaosNode{nodes[1], nodes[2]} {
		for r := 0; r < 2; r++ {
			readerWG.Add(1)
			go func(n *chaosNode, seed int64) {
				defer readerWG.Done()
				rr := rand.New(rand.NewSource(seed))
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, err := n.db.GetWithOptions(keyFn(rr.Intn(keyspace)), chaosLocal)
					if err != nil && !errors.Is(err, ErrKeyNotFound) {
						readErrCount.Add(1)
						select {
						case firstErr <- fmt.Sprintf("node %q: %v", n.cfg.NodeID, err):
						default:
						}
					}
				}
			}(n, int64(ni*10+r+1))
		}
	}

	for op := 0; op < numOps; op++ {
		switch roll := rng.Float64(); {
		case roll < 0.45: // Set
			k := keyFn(rng.Intn(keyspace))
			v := fmt.Sprintf("mixed-val-op%04d", op)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Set(k, v)
			})
			model[k] = v
		case roll < 0.65: // Delete (possibly of a missing key: not an error)
			k := keyFn(rng.Intn(keyspace))
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Delete(k)
			})
			delete(model, k)
		case roll < 0.85: // Batch: 3-8 sets + 1-3 deletes over distinct keys
			nSets := 3 + rng.Intn(6)
			nDels := 1 + rng.Intn(3)
			picked := map[int]bool{}
			pick := func() int {
				for {
					i := rng.Intn(keyspace)
					if !picked[i] {
						picked[i] = true
						return i
					}
				}
			}
			muts := make([]Mutation, 0, nSets+nDels)
			sets := make([][2]string, 0, nSets)
			for i := 0; i < nSets; i++ {
				k := keyFn(pick())
				v := fmt.Sprintf("mixed-val-op%04d-%d", op, i)
				muts = append(muts, SetOp(k, v))
				sets = append(sets, [2]string{k, v})
			}
			dels := make([]string, 0, nDels)
			for i := 0; i < nDels; i++ {
				k := keyFn(pick())
				muts = append(muts, DeleteOp(k))
				dels = append(dels, k)
			}
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Batch(muts...)
			})
			// Mirror the FSM's apply order: sets first, then deletes.
			for _, s := range sets {
				model[s[0]] = s[1]
			}
			for _, d := range dels {
				delete(model, d)
			}
		default: // Set with TTL, long enough to outlive the comparison
			k := keyFn(rng.Intn(keyspace))
			v := fmt.Sprintf("mixed-val-op%04d-ttl", op)
			chaosApplyOnLeader(t, 60*time.Second, nodes, func(db *DB) error {
				return db.Set(k, v, WithTTL(120*time.Second))
			})
			model[k] = v
		}
	}

	close(stop)
	readerWG.Wait()
	if readErrCount.Load() > 0 {
		t.Fatalf("chaos: followers served %d unexpected read errors during the storm, first: %s",
			readErrCount.Load(), <-firstErr)
	}

	// Final full-keyspace comparison across all nodes.
	chaosWaitConverged(t, chaosConvergeTO, "mixed/", model, nodes...)
}
