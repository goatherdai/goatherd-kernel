package trace

import (
	"context"
	"testing"
)

// Unit tests for the SQLite dialect's seam behaviors: conflict-clause
// statements must preserve the database/sql semantics the store relies on
// (RowsAffected on ignore), and the rewrite must refuse statements that do
// not meet the portable-INSERT contract.

func TestInsertOrIgnoreRowsAffected(t *testing.T) {
	store := newMemStore(t)
	defer store.Close(context.Background())

	// The schema seeds meta via insertOrIgnore, so the table exists; use a
	// scratch table with the same shape as the callers use.
	if err := store.dialect.execScript(store.db, `CREATE TABLE IF NOT EXISTS gate_scratch(k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create scratch table: %v", err)
	}
	stmt := "INSERT INTO gate_scratch(k, v) VALUES (?, ?)"

	res, err := store.dialect.insertOrIgnore(store.db, stmt, "k1", "v1")
	if err != nil {
		t.Fatalf("first insertOrIgnore: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("first insert rows affected = %d, %v; want 1, nil", n, err)
	}

	// A conflicting key must be a silent no-op reporting zero rows: the
	// materialization ledger decides replays off exactly this signal.
	res, err = store.dialect.insertOrIgnore(store.db, stmt, "k1", "other")
	if err != nil {
		t.Fatalf("conflicting insertOrIgnore: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("conflict rows affected = %d, %v; want 0, nil", n, err)
	}

	var v string
	if err := store.db.QueryRow("SELECT v FROM gate_scratch WHERE k = 'k1'").Scan(&v); err != nil || v != "v1" {
		t.Fatalf("conflicting insert mutated the row: v = %q, %v", v, err)
	}
}

func TestInsertOrReplaceReplacesRow(t *testing.T) {
	store := newMemStore(t)
	defer store.Close(context.Background())

	if err := store.dialect.execScript(store.db, `CREATE TABLE IF NOT EXISTS gate_scratch(k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create scratch table: %v", err)
	}
	stmt := "INSERT INTO gate_scratch(k, v) VALUES (?, ?)"

	// The checkpoint persistence path rewrites a checkpoint by (id): the
	// second write must fully replace the first.
	if _, err := store.dialect.insertOrReplace(store.db, stmt, []string{"k"}, "k1", "old"); err != nil {
		t.Fatalf("first insertOrReplace: %v", err)
	}
	if _, err := store.dialect.insertOrReplace(store.db, stmt, []string{"k"}, "k1", "new"); err != nil {
		t.Fatalf("second insertOrReplace: %v", err)
	}

	var n int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM gate_scratch").Scan(&n); err != nil || n != 1 {
		t.Fatalf("replace left %d rows (%v), want exactly 1", n, err)
	}
	var v string
	if err := store.db.QueryRow("SELECT v FROM gate_scratch WHERE k = 'k1'").Scan(&v); err != nil || v != "new" {
		t.Fatalf("replace stored v = %q (%v), want \"new\"", v, err)
	}
}

func TestConflictClauseRewriteRejectsNonPortableStatements(t *testing.T) {
	store := newMemStore(t)
	defer store.Close(context.Background())

	cases := []string{
		"UPDATE gate_scratch SET v = 'x'", // not an INSERT
		"SELECT 1",                        // not an INSERT
		"INSERT OR IGNORE INTO gate_scratch(k, v) VALUES (1, 1)", // carries its own conflict clause
		"INSERT OR REPLACE INTO gate_scratch(k, v) VALUES (1, 1)",
	}
	for _, stmt := range cases {
		if _, err := store.dialect.insertOrIgnore(store.db, stmt); err == nil {
			t.Errorf("insertOrIgnore(%q) succeeded, want error", stmt)
		}
	}
}

func TestListTablesReflectsSchema(t *testing.T) {
	store := newMemStore(t)
	defer store.Close(context.Background())

	tables, err := store.dialect.listTables(store.db)
	if err != nil {
		t.Fatalf("listTables: %v", err)
	}
	for _, want := range expectedTables {
		if !tables[want] {
			t.Errorf("listTables missing expected table %q", want)
		}
	}
	if tables["no_such_table_here"] {
		t.Errorf("listTables reported a table that cannot exist")
	}
}

func TestTempStagingTablesThroughDialect(t *testing.T) {
	store := newMemStore(t)
	defer store.Close(context.Background())

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()

	// Drop-before-create must tolerate absence, mirroring the GC sweep's
	// recovery from a skipped prior cleanup.
	if err := store.dialect.dropTempTableIfExists(tx, gcDeadRecordsTable); err != nil {
		t.Fatalf("drop of absent temp table: %v", err)
	}
	if err := store.dialect.createTempTableIfNotExists(tx, gcDeadRecordsTable, "record_id TEXT PRIMARY KEY"); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	// Idempotent re-create.
	if err := store.dialect.createTempTableIfNotExists(tx, gcDeadRecordsTable, "record_id TEXT PRIMARY KEY"); err != nil {
		t.Fatalf("re-create temp table: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO " + gcDeadRecordsTable + "(record_id) VALUES ('sha256:00')"); err != nil {
		t.Fatalf("insert into staged table: %v", err)
	}
	var n int
	if err := tx.QueryRow("SELECT COUNT(*) FROM " + gcDeadRecordsTable).Scan(&n); err != nil || n != 1 {
		t.Fatalf("staged rows = %d (%v), want 1", n, err)
	}
	if err := store.dialect.dropTempTableIfExists(tx, gcDeadRecordsTable); err != nil {
		t.Fatalf("final drop: %v", err)
	}
}
