package trace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// The Phase 3 go/no-go gate, hardened per review: the stress tests drive
// INDEPENDENT store instances (one per goroutine) against the same schema,
// so their transactions genuinely overlap on the server. A single shared
// store would serialize every Append behind the store-wide mutex and never
// exercise the dialect locks. TestPGLocksAreLoadBearing closes the loop:
// with the lock methods stubbed out, the same contention must fail.
//
// Skipped unless KERNEL_PG_DSN names a server:
//
//	KERNEL_PG_DSN=postgres://spike:spike@127.0.0.1:54329/spike?sslmode=disable go test -run TestPG .
func newPGStore(t *testing.T) *SQLiteTraceStore {
	t.Helper()
	dsn, ok := pgTestDSN(t)
	if !ok {
		t.Skip("KERNEL_PG_DSN not set; PostgreSQL spike tests skipped")
	}
	ts, err := Open(StoreConfig{Backend: BackendPostgres, DSN: dsn})
	if err != nil {
		t.Fatalf("Open(Postgres): %v", err)
	}
	store := ts.(*SQLiteTraceStore)
	t.Cleanup(func() { store.Close(context.Background()) })
	return store
}

// newPGInstances opens n independent stores on one per-test schema. Each
// carries its own connection pool and its own mutex — exactly the shape of
// n separate processes appending to one database.
func newPGInstances(t *testing.T, n int) ([]*SQLiteTraceStore, func()) {
	t.Helper()
	dsn, ok := pgTestDSN(t)
	if !ok {
		t.Skip("KERNEL_PG_DSN not set; PostgreSQL spike tests skipped")
	}
	stores := make([]*SQLiteTraceStore, n)
	for i := range stores {
		ts, err := Open(StoreConfig{Backend: BackendPostgres, DSN: dsn})
		if err != nil {
			t.Fatalf("Open(Postgres) instance %d: %v", i, err)
		}
		stores[i] = ts.(*SQLiteTraceStore)
	}
	return stores, func() {
		for _, s := range stores {
			s.Close(context.Background())
		}
	}
}

// noLockDialect stubs the concurrency locks so a test can prove they are
// load-bearing: with them disabled, contended cross-instance appends must
// fail (ordinal collisions surface as path_entries duplicate keys, receipt
// collisions as commit_receipt duplicates, same-intent races as
// append_intents duplicate keys).
type noLockDialect struct{ dialect }

func (d noLockDialect) lockOwnerOrdinalRows(ctx context.Context, q queryer, ownerIDs []string) error {
	return nil
}

func (d noLockDialect) lockCommitSeqRow(ctx context.Context, q queryer) error {
	return nil
}

func (d noLockDialect) lockAppendIntent(ctx context.Context, q queryer, intentID string) error {
	return nil
}

func (d noLockDialect) lockLivenessRecompute(ctx context.Context, q queryer) error {
	return nil
}

func (d noLockDialect) lockExistingOwnerOrdinalRow(ctx context.Context, q queryer, ownerID string) error {
	return nil
}

// TestPGLocksAreLoadBearing is the mutation check for the whole gate: the
// contended append of TestPGAppendUnderContention, run against stores whose
// dialect locks are stubbed no-ops, must produce failures. If it ever
// passes cleanly, the locks no longer carry the guarantee and the other
// gate tests prove nothing.
func TestPGLocksAreLoadBearing(t *testing.T) {
	stores, cleanup := newPGInstances(t, 8)
	defer cleanup()
	for i, s := range stores {
		s.dialect = noLockDialect{s.dialect}
		_ = i
	}

	const rounds = 4
	totalFailures := 0
	for round := 0; round < rounds; round++ {
		// A fresh owner per round forces the ordinal-0 collision: without
		// the materialize-and-lock, every racer reads an absent row and
		// writes path ordinal 0.
		owner := fmt.Sprintf("exec:mutation:%d", round)
		var successes, failures int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				<-start
				_, err := stores[g].Append(context.Background(), trustedAppend, AppendBatch{
					AppendIntentID: fmt.Sprintf("intent:mutation:%d:%d", round, g),
					Groups: []AppendGroup{{
						TraceOwnerID: owner,
						FactDrafts:   []RecordDraft{draft("step", Capture, map[string]any{"g": g})},
					}},
				})
				if err != nil {
					atomic.AddInt64(&failures, 1)
				} else {
					atomic.AddInt64(&successes, 1)
				}
			}(g)
		}
		close(start)
		wg.Wait()

		if failures == 0 {
			t.Fatalf("round %d: contended appends all succeeded without dialect locks — the locks are not load-bearing and the gate is void", round)
		}
		if successes == 0 {
			t.Fatalf("round %d: no append succeeded; the mutation test lost its baseline", round)
		}
		totalFailures += int(failures)
	}
	t.Logf("mutation check: %d contended appends failed with locks stubbed, as required", totalFailures)
}

// TestPGAppendUnderContention hammers one owner path from 8 independent
// store instances. On SQLite the single-writer file lock serializes
// appends; on PostgreSQL the ordinal/commit-seq row locks must provide the
// same guarantees: gapless ordinals, unique receipts, zero failed appends.
func TestPGAppendUnderContention(t *testing.T) {
	stores, cleanup := newPGInstances(t, 8)
	defer cleanup()
	const (
		batches  = 25
		perBatch = 2
		owner    = "exec:contended"
	)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			store := stores[g]
			for b := 0; b < batches; b++ {
				receipt, err := store.Append(context.Background(), trustedAppend, AppendBatch{
					AppendIntentID: fmt.Sprintf("intent:contention:%d:%d", g, b),
					Groups: []AppendGroup{{
						TraceOwnerID: owner,
						FactDrafts: []RecordDraft{
							{
								KindLabel:     "step",
								Mode:          Capture,
								SchemaRef:     "shepherd2.conformance.step.v1",
								Payload:       map[string]any{"goroutine": g, "batch": b, "slot": 0},
								AppendLocalID: fmt.Sprintf("local:%d:%d", g, b),
							},
							{
								KindLabel:         "step",
								Mode:              Capture,
								SchemaRef:         "shepherd2.conformance.step.v1",
								Payload:           map[string]any{"goroutine": g, "batch": b, "slot": 1},
								CausedByLocalRefs: []string{fmt.Sprintf("local:%d:%d", g, b)},
							},
						},
					}},
				})
				if err != nil {
					t.Errorf("instance %d batch %d: Append: %v", g, b, err)
					return
				}
				if len(receipt.FactIDs) != perBatch {
					t.Errorf("instance %d batch %d: %d fact ids, want %d", g, b, len(receipt.FactIDs), perBatch)
				}
			}
		}(g)
	}
	wg.Wait()

	total := 8 * batches * perBatch

	// Gapless ordinals: the owner path must hold exactly `total` entries
	// numbered 0..total-1 with no gaps and no duplicates.
	slice, err := stores[0].ReadOwnerPrefix(context.Background(), reader, owner, 1<<30, ModeBoth)
	if err != nil {
		t.Fatalf("ReadOwnerPrefix: %v", err)
	}
	if got := len(slice.FactIDs()); got != total {
		t.Fatalf("owner path holds %d facts, want %d", got, total)
	}
	assertGaplessOrdinals(t, stores[0], owner, total)

	// Unique commit receipts: the locked commit-seq counter must never
	// have handed the same number out twice.
	var receipts int
	if err := stores[0].db.QueryRow("SELECT COUNT(DISTINCT commit_receipt) FROM path_entries WHERE path_ref = ?", owner).Scan(&receipts); err != nil {
		t.Fatalf("count distinct receipts: %v", err)
	}
	if receipts != total {
		t.Errorf("%d distinct commit receipts across %d entries", receipts, total)
	}
}

// TestPGAppendConcurrentSameIntent races eight independent stores appending
// the identical batch (same intent id, same content). Every caller must
// succeed with the same receipt, and the store must hold exactly one copy.
// This is the case the advisory intent lock exists for: without it, the
// check-then-insert idempotency read races and the loser dies on a
// duplicate-key error.
func TestPGAppendConcurrentSameIntent(t *testing.T) {
	stores, cleanup := newPGInstances(t, 8)
	defer cleanup()
	const goroutines = 8

	intent := "intent:race-same"
	mk := func() AppendBatch {
		return AppendBatch{
			AppendIntentID: intent,
			Groups: []AppendGroup{{
				TraceOwnerID: "exec:same-intent",
				FactDrafts:   []RecordDraft{draft("step", Capture, map[string]any{"n": 1})},
			}},
		}
	}

	type result struct {
		receipt AppendReceipt
		err     error
	}
	results := make(chan result, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			r, err := stores[i].Append(context.Background(), trustedAppend, mk())
			results <- result{r, err}
		}(i)
	}

	var first AppendReceipt
	for i := 0; i < goroutines; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("racing same-intent append failed: %v", res.err)
		}
		if i == 0 {
			first = res.receipt
			continue
		}
		if !sameIDs(res.receipt.FactIDs, first.FactIDs) {
			t.Fatalf("replay receipt diverged: %v vs %v", res.receipt.FactIDs, first.FactIDs)
		}
		if res.receipt.AppendIntentID != first.AppendIntentID {
			t.Fatalf("replay intent diverged: %q vs %q", res.receipt.AppendIntentID, first.AppendIntentID)
		}
	}

	// Exactly one retained copy of the fact, despite eight racing callers.
	slice, err := stores[0].ReadOwnerPrefix(context.Background(), reader, "exec:same-intent", 1<<30, ModeBoth)
	if err != nil {
		t.Fatalf("ReadOwnerPrefix: %v", err)
	}
	if got := len(slice.FactIDs()); got != 1 {
		t.Errorf("owner path holds %d facts after %d racing same-intent appends, want 1", got, goroutines)
	}
}

// TestPGMixedOwnersUnderContention runs concurrent batches whose owner sets
// overlap in different orders — the deadlock scenario the sorted lock order
// exists to prevent. Every append must succeed: PostgreSQL detects lock
// cycles and aborts a transaction, so a deadlock here surfaces as a failed
// append, which this test refuses to tolerate.
func TestPGMixedOwnersUnderContention(t *testing.T) {
	stores, cleanup := newPGInstances(t, 8)
	defer cleanup()
	owners := []string{"exec:mix:a", "exec:mix:b", "exec:mix:c", "exec:mix:d"}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			store := stores[g]
			// Each goroutine touches two owners in a rotation, so
			// concurrent batches overlap pairwise in differing orders.
			a := owners[g%len(owners)]
			b := owners[(g+1)%len(owners)]
			for i := 0; i < 10; i++ {
				_, err := store.Append(context.Background(), trustedAppend, AppendBatch{
					AppendIntentID: fmt.Sprintf("intent:mix:%d:%d", g, i),
					Groups: []AppendGroup{
						{TraceOwnerID: a, FactDrafts: []RecordDraft{draft("step", Capture, map[string]any{"g": g})}},
						{TraceOwnerID: b, FactDrafts: []RecordDraft{draft("step", Capture, map[string]any{"g": g})}},
					},
				})
				if err != nil {
					errs <- fmt.Errorf("goroutine %d iter %d: %w", g, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("mixed-owner contention: %v", err)
	}

	// Each owner is touched by its own modulo pair and the preceding pair —
	// 4 instances — so 4 x 10 batches x 1 fact = 40 per owner.
	for _, owner := range owners {
		slice, err := stores[0].ReadOwnerPrefix(context.Background(), reader, owner, 1<<30, ModeBoth)
		if err != nil {
			t.Fatalf("ReadOwnerPrefix(%s): %v", owner, err)
		}
		if got := len(slice.FactIDs()); got != 40 {
			t.Errorf("owner %s holds %d facts, want 40", owner, got)
		}
		assertGaplessOrdinals(t, stores[0], owner, 40)
	}
}

// TestPGEvictionAppendCrossInstanceRace races eviction against append on
// the same owner from independent instances. Whichever wins, the final
// state must be consistent: the owner path and its tombstone are mutually
// exclusive, and a refused append reports OwnerEvictedError.
func TestPGEvictionAppendCrossInstanceRace(t *testing.T) {
	stores, cleanup := newPGInstances(t, 2)
	defer cleanup()
	const rounds = 5

	for round := 0; round < rounds; round++ {
		owner := fmt.Sprintf("exec:race-evict:%d", round)

		// Prime the path so eviction has something to delete.
		if _, err := stores[0].Append(context.Background(), trustedAppend, AppendBatch{
			AppendIntentID: fmt.Sprintf("intent:race-evict-prime:%d", round),
			Groups: []AppendGroup{{
				TraceOwnerID: owner,
				FactDrafts:   []RecordDraft{draft("step", Capture, map[string]any{"round": round})},
			}},
		}); err != nil {
			t.Fatalf("round %d: prime append: %v", round, err)
		}

		evictedErr := make(chan error, 1)
		appendedErr := make(chan error, 1)
		go func() {
			_, err := stores[0].EvictOwnerPath(context.Background(), trustedAppend, owner, false)
			evictedErr <- err
		}()
		go func() {
			_, err := stores[1].Append(context.Background(), trustedAppend, AppendBatch{
				AppendIntentID: fmt.Sprintf("intent:race-evict-append:%d", round),
				Groups: []AppendGroup{{
					TraceOwnerID: owner,
					FactDrafts:   []RecordDraft{draft("step", Capture, map[string]any{"round": round, "after": true})},
				}},
			})
			appendedErr <- err
		}()

		if err := <-evictedErr; err != nil {
			t.Fatalf("round %d: EvictOwnerPath: %v", round, err)
		}
		// The append serializes on the owner's ordinal row and re-checks
		// the tombstone inside the locked window, so it must be refused —
		// never silently committed into a path that eviction just deleted.
		err := <-appendedErr
		var ownerEvicted *OwnerEvictedError
		if !errors.As(err, &ownerEvicted) {
			t.Fatalf("round %d: racing append error = %v, want OwnerEvictedError", round, err)
		}

		// The invariant: tombstone present, entries gone. An append that
		// committed after eviction's check would have revived the path —
		// exactly what the lock window exists to prevent.
		var tombstone, entries int
		if err := stores[0].db.QueryRow("SELECT COUNT(*) FROM evicted_paths WHERE trace_owner_id = ?", owner).Scan(&tombstone); err != nil {
			t.Fatalf("round %d: count tombstones: %v", round, err)
		}
		if err := stores[0].db.QueryRow("SELECT COUNT(*) FROM path_entries WHERE path_ref = ?", owner).Scan(&entries); err != nil {
			t.Fatalf("round %d: count entries: %v", round, err)
		}
		if tombstone == 0 {
			t.Fatalf("round %d: eviction reported success but left no tombstone", round)
		}
		if entries > 0 {
			t.Fatalf("round %d: owner %s holds a tombstone AND %d live entries — an append revived an evicted path", round, owner, entries)
		}
	}
}

// assertGaplessOrdinals checks that owner's path_entries hold exactly the
// ordinals 0..total-1, each once, with next_ordinal sitting just past the
// end — the append invariant under contention.
func assertGaplessOrdinals(t *testing.T, store *SQLiteTraceStore, owner string, total int) {
	t.Helper()
	rows, err := store.db.Query("SELECT path_ordinal FROM path_entries WHERE path_ref = ? ORDER BY path_ordinal", owner)
	if err != nil {
		t.Fatalf("query path ordinals for %s: %v", owner, err)
	}
	defer rows.Close()
	ordinals := make(map[int]bool, total)
	for rows.Next() {
		var ord int
		if err := rows.Scan(&ord); err != nil {
			t.Fatalf("scan ordinal: %v", err)
		}
		if ordinals[ord] {
			t.Errorf("owner %s: duplicate ordinal %d", owner, ord)
		}
		ordinals[ord] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ordinals: %v", err)
	}
	for i := 0; i < total; i++ {
		if !ordinals[i] {
			t.Errorf("owner %s: ordinal gap — %d missing", owner, i)
		}
	}
	if len(ordinals) != total {
		t.Errorf("owner %s: %d distinct ordinals, want %d", owner, len(ordinals), total)
	}
	var next int
	if err := store.db.QueryRow("SELECT next_ordinal FROM owner_ordinals WHERE trace_owner_id = ?", owner).Scan(&next); err != nil {
		t.Fatalf("read next_ordinal for %s: %v", owner, err)
	}
	if next != total {
		t.Errorf("owner %s: next_ordinal = %d, want %d", owner, next, total)
	}
}
