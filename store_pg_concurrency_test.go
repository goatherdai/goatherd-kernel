package trace

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// The Phase 3 go/no-go gate: append invariants must hold on PostgreSQL
// under real connection-pool concurrency — gapless per-owner ordinals,
// unique commit receipts, idempotent same-intent racing, no deadlock
// across overlapping owners. Skipped unless KERNEL_PG_DSN names a server:
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

// TestPGAppendUnderContention hammers one owner path from 8 goroutines
// through a real connection pool. On SQLite the pool-of-1 serializes
// appends; on PostgreSQL the ordinal/commit-seq row locks must provide the
// same guarantees: gapless ordinals, unique receipts, zero failed appends.
func TestPGAppendUnderContention(t *testing.T) {
	store := newPGStore(t)
	const (
		goroutines = 8
		batches    = 25
		perBatch   = 2
		owner      = "exec:contended"
	)

	var failures atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
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
					failures.Add(1)
					t.Errorf("goroutine %d batch %d: Append: %v", g, b, err)
					return
				}
				if len(receipt.FactIDs) != perBatch {
					t.Errorf("goroutine %d batch %d: %d fact ids, want %d", g, b, len(receipt.FactIDs), perBatch)
				}
			}
		}(g)
	}
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("%d appends failed under contention", failures.Load())
	}

	total := goroutines * batches * perBatch

	// Gapless ordinals: the owner path must hold exactly `total` entries
	// numbered 0..total-1 with no gaps and no duplicates.
	slice, err := store.ReadOwnerPrefix(context.Background(), reader, owner, 1<<30, ModeBoth)
	if err != nil {
		t.Fatalf("ReadOwnerPrefix: %v", err)
	}
	if got := len(slice.FactIDs()); got != total {
		t.Fatalf("owner path holds %d facts, want %d", got, total)
	}
	assertGaplessOrdinals(t, store, owner, total)

	// Unique commit receipts: the locked commit-seq counter must never
	// have handed the same number out twice.
	var receipts int
	if err := store.db.QueryRow("SELECT COUNT(DISTINCT commit_receipt) FROM path_entries WHERE path_ref = ?", owner).Scan(&receipts); err != nil {
		t.Fatalf("count distinct receipts: %v", err)
	}
	if receipts != total {
		t.Errorf("%d distinct commit receipts across %d entries", receipts, total)
	}

	// The ordinal row must sit exactly past the last allocated ordinal.
	var next int
	if err := store.db.QueryRow("SELECT next_ordinal FROM owner_ordinals WHERE trace_owner_id = ?", owner).Scan(&next); err != nil {
		t.Fatalf("read next_ordinal: %v", err)
	}
	if next != total {
		t.Errorf("next_ordinal = %d, want %d", next, total)
	}
}

// TestPGAppendConcurrentSameIntent races eight appends of the identical
// batch (same intent id, same content) through the pool. Every caller must
// succeed with the same receipt, and the store must hold exactly one copy.
// This is the case the advisory intent lock exists for: without it, the
// check-then-insert idempotency read races and the loser dies on a
// duplicate-key error.
func TestPGAppendConcurrentSameIntent(t *testing.T) {
	store := newPGStore(t)
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
		go func() {
			r, err := store.Append(context.Background(), trustedAppend, mk())
			results <- result{r, err}
		}()
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
	slice, err := store.ReadOwnerPrefix(context.Background(), reader, "exec:same-intent", 1<<30, ModeBoth)
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
	store := newPGStore(t)
	owners := []string{"exec:mix:a", "exec:mix:b", "exec:mix:c", "exec:mix:d"}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
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

	// Every owner path must be gapless. Each owner is touched by its own
	// modulo pair and the preceding pair — 4 goroutines — so 4 × 10
	// batches × 1 fact = 40 per owner.
	for _, owner := range owners {
		slice, err := store.ReadOwnerPrefix(context.Background(), reader, owner, 1<<30, ModeBoth)
		if err != nil {
			t.Fatalf("ReadOwnerPrefix(%s): %v", owner, err)
		}
		if got := len(slice.FactIDs()); got != 40 {
			t.Errorf("owner %s holds %d facts, want 40", owner, got)
		}
		assertGaplessOrdinals(t, store, owner, 40)
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
