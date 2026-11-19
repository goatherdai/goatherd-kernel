package trace

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Regression tests for the PostgreSQL concurrency review, written against
// newMemStore so they run on both backends (SQLite when KERNEL_PG_DSN is
// unset, PostgreSQL when it is).

// TestAppendEmptyGroupDoesNotMaterializeOwner pins finding 4: an append
// whose group carries no drafts allocates nothing, so the owner must stay
// unknown to eviction. On a materializing backend the ordinal row would
// otherwise flip EvictOwnerPath's "no such owner path" check.
func TestAppendEmptyGroupDoesNotMaterializeOwner(t *testing.T) {
	store := newMemStore(t)

	receipt, err := store.Append(context.Background(), trustedAppend, AppendBatch{
		AppendIntentID: "intent:empty-group",
		Groups:         []AppendGroup{{TraceOwnerID: "exec:empty"}},
	})
	if err != nil {
		t.Fatalf("Append with empty group: %v", err)
	}
	if len(receipt.FactIDs) != 0 {
		t.Fatalf("empty group allocated %d facts, want 0", len(receipt.FactIDs))
	}

	_, err = store.EvictOwnerPath(context.Background(), trustedAppend, "exec:empty", false)
	if err == nil {
		t.Fatalf("eviction of an owner that never allocated succeeded; unknown paths must not gain tombstones")
	}
	if !strings.Contains(err.Error(), "no such owner path") {
		t.Errorf("eviction error %v does not report the path as unknown", err)
	}

	// The same for a mixed batch: the empty group's owner stays unknown
	// even though its sibling group allocated.
	if _, err := store.Append(context.Background(), trustedAppend, AppendBatch{
		AppendIntentID: "intent:mixed-groups",
		Groups: []AppendGroup{
			{TraceOwnerID: "exec:empty"},
			{TraceOwnerID: "exec:real", FactDrafts: []RecordDraft{draft("step", Capture, map[string]any{"n": 1})}},
		},
	}); err != nil {
		t.Fatalf("Append mixed groups: %v", err)
	}
	if _, err := store.EvictOwnerPath(context.Background(), trustedAppend, "exec:empty", false); err == nil {
		t.Errorf("eviction succeeded for the empty-group owner in a mixed batch")
	}
	if _, err := store.EvictOwnerPath(context.Background(), trustedAppend, "exec:real", true); err != nil {
		t.Errorf("eviction of the allocating owner failed: %v", err)
	}
}

// TestCollectGarbageSurvivesBinaryCheckpointPayload pins finding 3: the
// snapshot column is opaque caller bytes with no text-encoding contract.
// A payload that is not valid UTF-8 must not abort the sweep, and a
// record id embedded in it must still confer liveness.
func TestCollectGarbageSurvivesBinaryCheckpointPayload(t *testing.T) {
	store := newMemStore(t)

	receipt := appendDrafts(t, store, "intent:binary-snapshot", "exec:binary",
		draft("step", Capture, map[string]any{"n": 1}))
	factID := receipt.FactIDs[0]

	// Invalid UTF-8 framing bytes around the id: exactly the shape a
	// non-JSON encoder could produce.
	snapshot := append([]byte{0x00, 0xFF, 0xFE, 0x80}, append([]byte("id:"), []byte(factID+"\xff")...)...)
	if err := PersistCheckpointBlob(context.Background(), store, "cp:binary", "scope:binary", 1, WorkspaceState{}, snapshot, time.Now()); err != nil {
		t.Fatalf("PersistCheckpointBlob: %v", err)
	}

	// Unreachable by path (evicted); reachable only through the snapshot scan.
	if _, err := store.EvictOwnerPath(context.Background(), trustedAppend, "exec:binary", true); err != nil {
		t.Fatalf("EvictOwnerPath: %v", err)
	}

	gc, err := store.CollectGarbage(context.Background(), trustedAppend)
	if err != nil {
		t.Fatalf("CollectGarbage with a non-UTF-8 snapshot payload: %v", err)
	}
	if gc.DeletedRecords != 0 {
		t.Errorf("GC reclaimed %d records; the snapshot-referenced record must stay live", gc.DeletedRecords)
	}
	// Assert against the records table directly: after eviction the record
	// has no path entry, so the path-join reader legitimately reports it
	// unknown even though the row is retained.
	var present int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM records WHERE record_id = ?", factID).Scan(&present); err != nil {
		t.Fatalf("count records: %v", err)
	}
	if present != 1 {
		t.Errorf("snapshot-referenced record %s did not survive GC", factID)
	}
}

// TestEvictionRefusesWithoutAllocation covers the eviction side of the
// lock contract directly: locking an owner's ordinal row must not create
// one, so an owner with no allocation state is still refused.
func TestEvictionRefusesWithoutAllocation(t *testing.T) {
	store := newMemStore(t)
	if err := store.dialect.lockExistingOwnerOrdinalRow(context.Background(), store.db, "exec:never-appended"); err != nil {
		t.Fatalf("lockExistingOwnerOrdinalRow on absent row: %v", err)
	}
	var count int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM owner_ordinals WHERE trace_owner_id = 'exec:never-appended'").Scan(&count); err != nil {
		t.Fatalf("count owner_ordinals: %v", err)
	}
	if count != 0 {
		t.Fatalf("locking an absent ordinal row materialized %d rows; eviction must not make owners known", count)
	}
}
