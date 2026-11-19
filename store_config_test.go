package trace

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Tests for the backend-neutral open path: StoreConfig handling, backend
// dispatch, and the option surface Open must honor on every backend.

func TestOpenDefaultsToSQLiteAndServesTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.sqlite")
	store, err := Open(StoreConfig{DSN: path})
	if err != nil {
		t.Fatalf("Open(default backend): %v", err)
	}
	defer store.Close(context.Background())

	sqlStore, ok := store.(*SQLiteTraceStore)
	if !ok {
		t.Fatalf("Open returned %T, want *SQLiteTraceStore", store)
	}
	if sqlStore.substrateRef != DefaultSubstrateRef {
		t.Errorf("substrateRef = %q, want default %q", sqlStore.substrateRef, DefaultSubstrateRef)
	}

	receipt := appendDrafts(t, sqlStore, "intent:cfg", "exec:cfg",
		draft("step", Capture, map[string]any{"value": 1}))
	slice, err := store.ReadOwnerPrefix(context.Background(), reader, "exec:cfg", 99, ModeBoth)
	if err != nil {
		t.Fatalf("ReadOwnerPrefix: %v", err)
	}
	if len(slice.FactIDs()) != len(receipt.FactIDs) {
		t.Errorf("read %d facts, want %d", len(slice.FactIDs()), len(receipt.FactIDs))
	}
}

func TestOpenUnknownBackendFailsLoudly(t *testing.T) {
	_, err := Open(StoreConfig{Backend: "boltdb", DSN: filepath.Join(t.TempDir(), "x.sqlite")})
	if err == nil {
		t.Fatalf("Open(unknown backend) succeeded, want error")
	}
	// The error must name what went wrong and what is supported, so a
	// mistyped config value is diagnosable without source access.
	for _, want := range []string{"boltdb", string(BackendSQLite)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

func TestOpenReadOnlyViaConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ro.sqlite")
	write, err := OpenSQLite(StoreConfig{DSN: path})
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	appendDrafts(t, write, "intent:ro", "exec:ro",
		draft("step", Capture, map[string]any{"value": 1}))
	write.Close(context.Background())

	read, err := Open(StoreConfig{DSN: path, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open(read-only): %v", err)
	}
	defer read.Close(context.Background())

	slice, err := read.ReadOwnerPrefix(context.Background(), reader, "exec:ro", 99, ModeBoth)
	if err != nil {
		t.Fatalf("ReadOwnerPrefix on read-only store: %v", err)
	}
	if len(slice.FactIDs()) != 1 {
		t.Errorf("read %d facts, want 1", len(slice.FactIDs()))
	}

	// A read-only open refuses to create a store: a missing file is an
	// error, never a fresh database.
	if _, err := Open(StoreConfig{DSN: filepath.Join(t.TempDir(), "missing.sqlite"), ReadOnly: true}); err == nil {
		t.Errorf("Open(read-only, missing file) succeeded, want error")
	}
}

func TestOpenAppliesLimitsViaConfig(t *testing.T) {
	store, err := OpenSQLite(StoreConfig{
		DSN:                 ":memory:",
		BlobLimit:           16,
		CheckpointBlobLimit: 32,
	})
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close(context.Background())

	if got := BlobLimitOrDefault(store); got != 16 {
		t.Errorf("BlobLimitOrDefault = %d, want 16", got)
	}
	if got := CheckpointBlobLimitOrDefault(store); got != 32 {
		t.Errorf("CheckpointBlobLimitOrDefault = %d, want 32", got)
	}
	if _, _, err := GetBlob(context.Background(), store, "owner", "kind", "key"); err == nil {
		t.Fatalf("GetBlob on absent blob succeeded")
	}
	if _, err := PutBlob(context.Background(), store, "owner", "kind", "key", make([]byte, 17)); err == nil {
		t.Errorf("PutBlob over config limit succeeded, want ErrBlobTooLarge")
	}
}

func TestOpenAttachesBusViaConfig(t *testing.T) {
	bus := NewEffectBus(8)
	store, err := OpenSQLite(StoreConfig{DSN: ":memory:", Bus: bus})
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close(context.Background())
	defer bus.Close()

	events := bus.Subscribe("config-test")
	appendDrafts(t, store, "intent:bus", "exec:bus",
		draft("step", Capture, map[string]any{"value": 1}))

	select {
	case ev := <-events:
		if ev.TraceOwnerID != "exec:bus" {
			t.Errorf("event owner = %q, want exec:bus", ev.TraceOwnerID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no effect event arrived on the configured bus")
	}
}

// explicitSubstrateBatch is a batch whose group carries its own substrate
// reference, so record ids do not depend on the store's configured default.
func explicitSubstrateBatch(intent string) AppendBatch {
	b := AppendBatch{
		AppendIntentID: intent,
		Groups:         []AppendGroup{{TraceOwnerID: "exec:substrate", FactDrafts: []RecordDraft{draft("step", Capture, map[string]any{"value": 1})}}},
	}
	b.Groups[0].RetainedContext = &RetainedContext{
		SubstrateRef: "fixed.substrate.v1",
		Containment:  ContainContained,
	}
	return b
}

// TestSubstrateRefOverride pins the two sides of the override: a store-level
// override rekeys facts derived from the default substrate (the witness body
// digests over it), while an explicit per-batch substrate keeps ids identical
// across differently-configured stores. The default store must produce the
// same ids it always has.
func TestSubstrateRefOverride(t *testing.T) {
	defaultBatch := func(intent string) AppendBatch {
		return AppendBatch{
			AppendIntentID: intent,
			Groups:         []AppendGroup{{TraceOwnerID: "exec:substrate", FactDrafts: []RecordDraft{draft("step", Capture, map[string]any{"value": 1})}}},
		}
	}

	def, err := OpenSQLite(StoreConfig{DSN: ":memory:"})
	if err != nil {
		t.Fatalf("OpenSQLite(default): %v", err)
	}
	defer def.Close(context.Background())

	over, err := OpenSQLite(StoreConfig{DSN: ":memory:", SubstrateRef: "postgres.remote.v1"})
	if err != nil {
		t.Fatalf("OpenSQLite(override): %v", err)
	}
	defer over.Close(context.Background())

	// Nil retained context: the substrate flows from the store config, so
	// the two stores allocate different witness and fact ids.
	defReceipt, err := def.Append(context.Background(), trustedAppend, defaultBatch("intent:default"))
	if err != nil {
		t.Fatalf("Append(default): %v", err)
	}
	overReceipt, err := over.Append(context.Background(), trustedAppend, defaultBatch("intent:default"))
	if err != nil {
		t.Fatalf("Append(override): %v", err)
	}
	if sameIDs(defReceipt.FactIDs, overReceipt.FactIDs) {
		t.Errorf("substrate override did not rekey default-context facts: both %v", defReceipt.FactIDs)
	}

	// Explicit retained context: the batch carries its own substrate, so
	// both stores derive the same ids regardless of configuration.
	defReceipt, err = def.Append(context.Background(), trustedAppend, explicitSubstrateBatch("intent:explicit"))
	if err != nil {
		t.Fatalf("Append(default, explicit context): %v", err)
	}
	overReceipt, err = over.Append(context.Background(), trustedAppend, explicitSubstrateBatch("intent:explicit"))
	if err != nil {
		t.Fatalf("Append(override, explicit context): %v", err)
	}
	if !sameIDs(defReceipt.FactIDs, overReceipt.FactIDs) {
		t.Errorf("explicit substrate diverged across stores: %v vs %v", defReceipt.FactIDs, overReceipt.FactIDs)
	}
}

func sameIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
