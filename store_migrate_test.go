package trace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Phase 4: schema version stamping and the SQLite→PostgreSQL migration
// exporter. The migration contract under test: a moved ledger is
// indistinguishable from the original to every reader — counts, digests,
// receipts, ordinals, blobs — and future appends extend it without forking.

func TestSchemaVersionStampedOnCreate(t *testing.T) {
	store := newMemStore(t)
	var raw string
	if err := store.db.QueryRow("SELECT value FROM meta WHERE key = ?", schemaVersionKey).Scan(&raw); err != nil {
		t.Fatalf("read schema version stamp: %v", err)
	}
	if raw != "1" {
		t.Errorf("schema version stamp = %q, want \"1\"", raw)
	}
}

func TestNewerSchemaVersionRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.sqlite")
	store, err := NewSQLiteTraceStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Simulate a database written by a future kernel.
	if _, err := store.db.Exec("UPDATE meta SET value = '99' WHERE key = ?", schemaVersionKey); err != nil {
		t.Fatalf("stamp future version: %v", err)
	}
	store.Close(context.Background())

	// Both open paths refuse: a read-write open must not silently serve —
	// or worse, extend — a schema this binary does not understand.
	if _, err := NewSQLiteTraceStore(path); err == nil {
		t.Errorf("read-write open of a newer-schema database succeeded, want refusal")
	}
	if _, err := OpenSQLiteTraceStoreReadOnly(path); err == nil {
		t.Errorf("read-only open of a newer-schema database succeeded, want refusal")
	}
}

// migrateFixture builds a ledger exercising every migrated table: facts on
// two owners with causal edges, a retained context, a frontier, named
// blobs, a checkpoint payload, an eviction tombstone, and a materialization
// receipt.
func migrateFixture(t *testing.T, store *SQLiteTraceStore) AppendReceipt {
	t.Helper()
	receipt, err := store.Append(context.Background(), trustedAppend, AppendBatch{
		AppendIntentID: "intent:migrate:main",
		Groups: []AppendGroup{{
			TraceOwnerID: "exec:migrate",
			FactDrafts: []RecordDraft{
				{
					KindLabel:     "step",
					Mode:          Capture,
					SchemaRef:     "shepherd2.conformance.step.v1",
					Payload:       map[string]any{"slot": 0},
					AppendLocalID: "local:slot0",
				},
				{
					KindLabel:         "step",
					Mode:              Capture,
					SchemaRef:         "shepherd2.conformance.step.v1",
					Payload:           map[string]any{"slot": 1},
					CausedByLocalRefs: []string{"local:slot0"},
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("fixture append: %v", err)
	}
	_ = receipt
	if _, err := store.Append(context.Background(), trustedAppend, AppendBatch{
		AppendIntentID: "intent:migrate:second",
		Groups: []AppendGroup{{
			TraceOwnerID: "exec:migrate",
			FactDrafts: []RecordDraft{
				draft("finish", Capture, map[string]any{"ok": true}),
			},
		}},
	}); err != nil {
		t.Fatalf("fixture second append: %v", err)
	}
	if _, err := PutBlob(context.Background(), store, "exec:migrate", "snapshot", "head", []byte("blob-bytes")); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if err := PersistCheckpointBlob(context.Background(), store, "cp:migrate", "scope:migrate", 1, WorkspaceState{}, []byte("snapshot"), time.Now()); err != nil {
		t.Fatalf("PersistCheckpointBlob: %v", err)
	}
	if err := RecordCompletedMaterialization(store, "materialize:migrate", "digest:migrate", `{"ok":true}`); err != nil {
		t.Fatalf("RecordCompletedMaterialization: %v", err)
	}
	// A frontier over the owner path: exercises the frontiers table and the
	// frontier-fact join the resolver depends on.
	prefix, err := store.ReadOwnerPrefix(context.Background(), reader, "exec:migrate", 99, ModeBoth)
	if err != nil {
		t.Fatalf("fixture read: %v", err)
	}
	through := prefix.FactIDs()[len(prefix.FactIDs())-1]
	if _, err := store.PublishFrontier(context.Background(), trustedAppend, FrontierSpec{
		FrontierID:         "frontier:migrate",
		TargetTraceOwnerID: "exec:migrate",
		ThroughFactID:      through,
		AppendIntentID:     "intent:migrate:frontier",
	}); err != nil {
		t.Fatalf("fixture frontier: %v", err)
	}
	return receipt
}

// TestMigrateSQLiteToPostgres is the end-to-end contract: every table
// copies, the moved ledger reads identically, idempotent replay of a
// migrated intent returns the stored receipt, and a new append on the
// target extends the ledger with continuing ordinals instead of forking.
func TestMigrateSQLiteToPostgres(t *testing.T) {
	dsn, ok := pgTestDSN(t)
	if !ok {
		t.Skip("KERNEL_PG_DSN not set; migration test skipped")
	}
	srcPath := filepath.Join(t.TempDir(), "src.sqlite")
	src, err := NewSQLiteTraceStore(srcPath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	migrateFixture(t, src)

	// A second owner that gets evicted: the tombstone and its emptied path
	// must migrate too.
	evicted := "exec:migrate-evicted"
	appendDrafts(t, src, "intent:migrate:evicted", evicted, draft("step", Capture, map[string]any{"n": 1}))
	if _, err := src.EvictOwnerPath(context.Background(), trustedAppend, evicted, true); err != nil {
		t.Fatalf("fixture eviction: %v", err)
	}

	srcFacts, err := src.FactCount(context.Background())
	if err != nil {
		t.Fatalf("FactCount: %v", err)
	}
	ownerSlice, err := src.ReadOwnerPrefix(context.Background(), reader, "exec:migrate", 99, ModeBoth)
	if err != nil {
		t.Fatalf("source owner read: %v", err)
	}
	ownerCount := len(ownerSlice.FactIDs())
	src.Close(context.Background())

	report, err := MigrateSQLiteToPostgres(srcPath, StoreConfig{Backend: BackendPostgres, DSN: dsn})
	if err != nil {
		t.Fatalf("MigrateSQLiteToPostgres: %v", err)
	}
	if report.FactCount != srcFacts {
		t.Errorf("migrated FactCount = %d, want %d", report.FactCount, srcFacts)
	}
	for _, table := range []string{"records", "path_entries", "frontiers", "evicted_paths", "named_blobs",
		"checkpoint_snapshots", "materialization_intents", "append_intents"} {
		if report.Rows[table] == 0 {
			t.Errorf("table %s migrated 0 rows; fixture should have populated it", table)
		}
	}

	// The moved ledger reads identically through a fresh open.
	dst, err := OpenSQLite(StoreConfig{Backend: BackendPostgres, DSN: dsn})
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	defer dst.Close(context.Background())

	slice, err := dst.ReadOwnerPrefix(context.Background(), reader, "exec:migrate", 99, ModeBoth)
	if err != nil {
		t.Fatalf("ReadOwnerPrefix on migrated store: %v", err)
	}
	if len(slice.FactIDs()) != ownerCount {
		t.Errorf("migrated owner path holds %d facts, want %d", len(slice.FactIDs()), ownerCount)
	}
	if blob, digest, err := GetBlob(context.Background(), dst, "exec:migrate", "snapshot", "head"); err != nil || string(blob) != "blob-bytes" {
		t.Errorf("GetBlob on migrated store = %q, %v (digest %q)", blob, err, digest)
	}
	blobs, corrupt, err := LoadCheckpointBlobs(context.Background(), dst)
	if err != nil || len(blobs) != 1 || len(corrupt) != 0 {
		t.Errorf("LoadCheckpointBlobs on migrated store = %d blobs, %d corrupt, %v", len(blobs), len(corrupt), err)
	}

	// Replay of a migrated intent: the caller re-sends the identical batch
	// and gets back the stored receipt — the migration preserved the
	// idempotency ledger, not just the rows.
	if _, err := dst.Append(context.Background(), trustedAppend, AppendBatch{
		AppendIntentID: "intent:migrate:second",
		Groups: []AppendGroup{{
			TraceOwnerID: "exec:migrate",
			FactDrafts:   []RecordDraft{draft("finish", Capture, map[string]any{"ok": true})},
		}},
	}); err != nil {
		t.Fatalf("replay of migrated intent: %v", err)
	}

	// New appends extend the ledger: the next ordinal continues after the
	// migrated prefix instead of restarting at 0.
	receipt, err := dst.Append(context.Background(), trustedAppend, AppendBatch{
		AppendIntentID: "intent:migrate:post",
		Groups: []AppendGroup{{
			TraceOwnerID: "exec:migrate",
			FactDrafts:   []RecordDraft{draft("step", Capture, map[string]any{"post": true})},
		}},
	})
	if err != nil {
		t.Fatalf("post-migration append: %v", err)
	}
	assertGaplessOrdinals(t, dst, "exec:migrate", ownerCount+1)
	_ = receipt
}

func TestMigrateRefusesNonEmptyTarget(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "src.sqlite")
	src, err := NewSQLiteTraceStore(srcPath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	migrateFixture(t, src)
	src.Close(context.Background())

	dstPath := filepath.Join(t.TempDir(), "dst.sqlite")
	dst, err := NewSQLiteTraceStore(dstPath)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	appendDrafts(t, dst, "intent:target-occupied", "exec:occupied", draft("step", Capture, map[string]any{}))
	dst.Close(context.Background())

	_, err = MigrateSQLiteToPostgres(srcPath, StoreConfig{Backend: BackendSQLite, DSN: dstPath})
	if err == nil {
		t.Fatalf("migration into a non-empty target succeeded, want refusal")
	}
	if !strings.Contains(err.Error(), "never a merge") {
		t.Errorf("refusal %q does not carry the move-not-merge contract", err.Error())
	}
}

func TestMigrateMissingSourceFailsLoudly(t *testing.T) {
	dstPath := filepath.Join(t.TempDir(), "dst.sqlite")
	if _, err := MigrateSQLiteToPostgres(filepath.Join(t.TempDir(), "absent.sqlite"), StoreConfig{Backend: BackendSQLite, DSN: dstPath}); err == nil {
		t.Fatalf("migration from a missing source succeeded, want the loud read-only failure")
	}
	// The failed run must not have materialized a target.
	if _, err := os.Stat(dstPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("failed migration created a target store: %v", err)
	}
}
