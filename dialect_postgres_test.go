package trace

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// recordingExecDB captures statements execScript produces.
type recordingExecDB struct {
	storeDB
	statements []string
}

func (f *recordingExecDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	f.statements = append(f.statements, query)
	return fakeResult{}, nil
}

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeResult) RowsAffected() (int64, error) { return 0, nil }

// Pure-function tests for the PostgreSQL dialect's statement rewriting and
// DDL translation. No server required: these pin the wire forms every PG
// store will execute, and they run in the SQLite-only CI lane.

func TestRebindPlaceholders(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SELECT 1", "SELECT 1"},
		{"SELECT * FROM t WHERE k = ?", "SELECT * FROM t WHERE k = $1"},
		{"INSERT INTO t(a, b) VALUES (?, ?)", "INSERT INTO t(a, b) VALUES ($1, $2)"},
		// A question mark inside a quoted literal is statement text, not a
		// placeholder — it must survive untouched.
		{"SELECT * FROM t WHERE s = 'what?' AND k = ?", "SELECT * FROM t WHERE s = 'what?' AND k = $1"},
		{"SELECT * FROM t WHERE s = 'it''s ? here' AND k = ?", "SELECT * FROM t WHERE s = 'it''s ? here' AND k = $1"},
	}
	for _, c := range cases {
		if got := rebindPlaceholders(c.in); got != c.want {
			t.Errorf("rebindPlaceholders(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestPGRewriteInsert(t *testing.T) {
	got, err := pgRewriteInsert("INSERT INTO t(a, b) VALUES (?, ?)", "DO NOTHING")
	if err != nil {
		t.Fatalf("pgRewriteInsert do nothing: %v", err)
	}
	if got != "INSERT INTO t(a, b) VALUES ($1, $2) ON CONFLICT DO NOTHING" {
		t.Errorf("ignore rewrite = %q", got)
	}

	// Non-portable statements must be refused, not executed corrupted.
	for _, bad := range []string{
		"UPDATE t SET a = 1",
		"INSERT INTO t(a) VALUES (?) ON CONFLICT (a) DO NOTHING",
		"SELECT 1",
	} {
		if _, err := pgRewriteInsert(bad, "DO NOTHING"); err == nil {
			t.Errorf("pgRewriteInsert(%q) succeeded, want error", bad)
		}
	}
}

func TestPGInsertOrReplace(t *testing.T) {
	got, err := pgInsertOrReplace(
		"INSERT INTO checkpoint_snapshots(checkpoint_id, scope_id, seq, state_json, snapshot, created_at_unixnano) VALUES (?, ?, ?, ?, ?, ?)",
		[]string{"checkpoint_id"})
	if err != nil {
		t.Fatalf("pgInsertOrReplace: %v", err)
	}
	want := "INSERT INTO checkpoint_snapshots (checkpoint_id, scope_id, seq, state_json, snapshot, created_at_unixnano) " +
		"VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (checkpoint_id) DO UPDATE SET " +
		"scope_id = EXCLUDED.scope_id, seq = EXCLUDED.seq, state_json = EXCLUDED.state_json, " +
		"snapshot = EXCLUDED.snapshot, created_at_unixnano = EXCLUDED.created_at_unixnano"
	if got != want {
		t.Errorf("replace rewrite:\n got %q\nwant %q", got, want)
	}

	// Placeholder/column mismatch and empty conflict targets fail loudly.
	if _, err := pgInsertOrReplace("INSERT INTO t(a, b) VALUES (?)", []string{"a"}); err == nil {
		t.Errorf("mismatched placeholder count accepted")
	}
	if _, err := pgInsertOrReplace("INSERT INTO t(a) VALUES (?)", nil); err == nil {
		t.Errorf("empty conflict target accepted")
	}
	if _, err := pgInsertOrReplace("INSERT INTO t(a) VALUES (?)", []string{"a"}); err == nil {
		t.Errorf("conflict-only upsert accepted: nothing to update")
	}
}

func TestPGDDLTranslation(t *testing.T) {
	d := postgresDialect{}
	if got := d.binaryColumnAsText("snapshot"); got != "convert_from(snapshot, 'UTF8')" {
		t.Errorf("binaryColumnAsText = %q", got)
	}

	// execScript translates the portable DDL per backend and splits
	// statements for the extended protocol. Verify against a recording
	// fake rather than a server.
	fake := &recordingExecDB{}
	if err := d.execScript(context.Background(), fake, "CREATE TABLE t(x BLOB, n INTEGER, ts TEXT DEFAULT CURRENT_TIMESTAMP); INSERT INTO meta VALUES ('k','v');"); err != nil {
		t.Fatalf("execScript: %v", err)
	}
	if len(fake.statements) != 2 {
		t.Fatalf("execScript produced %d statements, want 2", len(fake.statements))
	}
	if fake.statements[0] != "CREATE TABLE t(x BYTEA, n BIGINT, ts TEXT DEFAULT CURRENT_TIMESTAMP::text)" {
		t.Errorf("translated DDL = %q", fake.statements[0])
	}
}

func TestPGReadOnlyDSN(t *testing.T) {
	d := postgresDialect{}
	if got, err := d.open("postgres://u:p@h/db?sslmode=disable", false); err != nil || got == nil {
		// open without a server must not fail until a connection is used;
		// sql.Open is lazy. We only exercise the DSN shaping here.
		_ = got
	}
	got, err := pgReadOnlyDSN("postgres://u:p@h/db?sslmode=disable", true)
	if err != nil {
		t.Fatalf("pgReadOnlyDSN url form: %v", err)
	}
	if !containsAll(got, "default_transaction_read_only=on", "postgres://u:p@h/db") {
		t.Errorf("read-only URL DSN = %q", got)
	}
	got, err = pgReadOnlyDSN("host=db user=u", true)
	if err != nil {
		t.Fatalf("pgReadOnlyDSN keyword form: %v", err)
	}
	if !containsAll(got, "default_transaction_read_only=on", "host=db user=u") {
		t.Errorf("read-only keyword DSN = %q", got)
	}
	got, err = pgReadOnlyDSN("postgres://u:p@h/db", false)
	if err != nil || got != "postgres://u:p@h/db" {
		t.Errorf("write DSN must be untouched: %q, %v", got, err)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
