package trace

// The PostgreSQL dialect: the backend-specific half of the trace store for
// pgx-backed stores. Owns the driver registration, DSN handling, catalog
// probe, conflict-clause syntax, DDL translation, temporary-table staging,
// and the row/advisory locks that serialize ordinal, commit-sequence, and
// intent allocation under real connection-pool concurrency.
//
// The store's statements arrive in portable database/sql form (`?`
// placeholders); the dialect rebinds them to PostgreSQL's `$n` form with a
// quote-aware scanner, so a literal `?` inside a quoted string survives.

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// postgresDialect is the dialect for PostgreSQL stores.
type postgresDialect struct{}

func (postgresDialect) name() string { return string(BackendPostgres) }

func (postgresDialect) open(dsn string, readOnly bool) (*sql.DB, error) {
	dsn, err := pgReadOnlyDSN(dsn, readOnly)
	if err != nil {
		return nil, err
	}
	// Real connection pool: PostgreSQL, unlike SQLite, serves concurrent
	// connections natively. Correctness does not depend on pool size —
	// ordinal/commit-seq/intent serialization comes from the dialect's
	// row and advisory locks, not from serialization at the pool.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (postgresDialect) bind(db *sql.DB) storeDB {
	return rebindingDB{db}
}

// rebindingDB and rebindingTx translate portable `?` placeholders to
// PostgreSQL's positional `$n` at the execution boundary. Dialect methods
// receive these wrappers too, so their statements stay portable as well;
// rebinding is idempotent (rebound text holds no `?`).
type rebindingDB struct{ db *sql.DB }

func (p rebindingDB) Close() error { return p.db.Close() }

func (p rebindingDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (storeTx, error) {
	tx, err := p.db.BeginTx(ctx, opts)
	return rebindingTx{tx}, err
}

func (p rebindingDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, rebindPlaceholders(query), args...)
}

func (p rebindingDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return p.db.QueryContext(ctx, rebindPlaceholders(query), args...)
}

func (p rebindingDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return p.db.QueryRowContext(ctx, rebindPlaceholders(query), args...)
}

func (p rebindingDB) Exec(query string, args ...any) (sql.Result, error) {
	return p.db.Exec(rebindPlaceholders(query), args...)
}

func (p rebindingDB) Query(query string, args ...any) (*sql.Rows, error) {
	return p.db.Query(rebindPlaceholders(query), args...)
}

func (p rebindingDB) QueryRow(query string, args ...any) *sql.Row {
	return p.db.QueryRow(rebindPlaceholders(query), args...)
}

type rebindingTx struct{ tx *sql.Tx }

func (p rebindingTx) Commit() error { return p.tx.Commit() }

func (p rebindingTx) Rollback() error { return p.tx.Rollback() }

func (p rebindingTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.tx.ExecContext(ctx, rebindPlaceholders(query), args...)
}

func (p rebindingTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return p.tx.QueryContext(ctx, rebindPlaceholders(query), args...)
}

func (p rebindingTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return p.tx.QueryRowContext(ctx, rebindPlaceholders(query), args...)
}

func (p rebindingTx) Exec(query string, args ...any) (sql.Result, error) {
	return p.tx.Exec(rebindPlaceholders(query), args...)
}

func (p rebindingTx) Query(query string, args ...any) (*sql.Rows, error) {
	return p.tx.Query(rebindPlaceholders(query), args...)
}

func (p rebindingTx) QueryRow(query string, args ...any) *sql.Row {
	return p.tx.QueryRow(rebindPlaceholders(query), args...)
}

// pgReadOnlyDSN appends default_transaction_read_only=on for read-only
// opens: the store contract forbids writes from a read-only open, and on a
// server backend that is a session property, not a URI mode.
func pgReadOnlyDSN(dsn string, readOnly bool) (string, error) {
	if !readOnly {
		return dsn, nil
	}
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse DSN: %w", err)
		}
		q := u.Query()
		q.Set("default_transaction_read_only", "on")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	// Keyword/value form: "host=... user=...".
	return dsn + " default_transaction_read_only=on", nil
}

func (postgresDialect) listTables(ctx context.Context, q queryer) (map[string]bool, error) {
	// current_schema() resolves to the first *existing* schema on the
	// connection's search_path — the schema this store's tables live in.
	rows, err := q.QueryContext(ctx, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan schema table name: %w", err)
		}
		tables[name] = true
	}
	return tables, rows.Err()
}

// rebindPlaceholders converts portable `?` placeholders to PostgreSQL's
// positional `$n` form, skipping quoted literals so a `?` inside statement
// text survives untouched.
func rebindPlaceholders(stmt string) string {
	var b strings.Builder
	inQuote := false
	n := 0
	for i := 0; i < len(stmt); i++ {
		c := stmt[i]
		if c == '\'' {
			inQuote = !inQuote
			b.WriteByte(c)
			continue
		}
		if c == '?' && !inQuote {
			n++
			b.WriteByte('$')
			b.WriteString(fmt.Sprint(n))
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// insertConflictPattern parses the portable INSERT forms the store uses:
// table, column list, and placeholder list. Nothing else about the shape is
// assumed; anything that does not parse fails loudly rather than executing
// a corrupted statement.
var insertConflictPattern = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^()]*)\)\s*VALUES\s*\(([^()]*)\)\s*$`)

// embeddedConflictPattern detects a statement that already carries a
// conflict clause, which the dialect's rewrite must never double.
var embeddedConflictPattern = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`)

func (postgresDialect) insertOrIgnore(ctx context.Context, q queryer, stmt string, args ...any) (sql.Result, error) {
	rewritten, err := pgRewriteInsert(stmt, "DO NOTHING")
	if err != nil {
		return nil, err
	}
	return q.ExecContext(ctx, rewritten, args...)
}

func (postgresDialect) insertOrReplace(ctx context.Context, q queryer, stmt string, conflictCols []string, args ...any) (sql.Result, error) {
	rewritten, err := pgInsertOrReplace(stmt, conflictCols)
	if err != nil {
		return nil, err
	}
	return q.ExecContext(ctx, rewritten, args...)
}

// pgRewriteInsert appends PostgreSQL's conflict clause to a plain portable
// INSERT. The store supplies only portable statements, so the anchor is the
// leading keyword; anything else is a caller bug and fails loudly.
func pgRewriteInsert(stmt, conflictAction string) (string, error) {
	trimmed := strings.TrimLeft(stmt, " \t\r\n")
	if !strings.HasPrefix(trimmed, "INSERT ") {
		return "", fmt.Errorf("dialect conflict clause expects a plain INSERT statement, got %q", firstLine(stmt))
	}
	if embeddedConflictPattern.MatchString(trimmed) {
		return "", fmt.Errorf("dialect conflict statement already carries a conflict clause: %q", firstLine(stmt))
	}
	stmt = strings.TrimRight(strings.TrimRight(stmt, " \t\r\n"), ";")
	return rebindPlaceholders(stmt) + " ON CONFLICT " + conflictAction, nil
}

// pgInsertOrReplace rewrites a portable INSERT into PostgreSQL's
// ON CONFLICT (cols) DO UPDATE SET col = EXCLUDED.col, ... form — the
// upsert shape a full-row replace needs, since PostgreSQL has no
// REPLACE shorthand.
func pgInsertOrReplace(stmt string, conflictCols []string) (string, error) {
	m := insertConflictPattern.FindStringSubmatch(strings.TrimRight(strings.TrimRight(stmt, " \t\r\n"), ";"))
	if m == nil {
		return "", fmt.Errorf("dialect replace statement does not parse as a portable INSERT: %q", firstLine(stmt))
	}
	table, colList, valueList := m[1], m[2], m[3]
	cols := splitIdentList(colList)
	if len(cols) == 0 {
		return "", fmt.Errorf("dialect replace statement has no columns: %q", firstLine(stmt))
	}
	if strings.Count(valueList, "?") != len(cols) {
		return "", fmt.Errorf("dialect replace statement has %d columns but %d placeholders", len(cols), strings.Count(valueList, "?"))
	}

	lower := func(ss []string) []string {
		out := make([]string, len(ss))
		for i, s := range ss {
			out[i] = strings.ToLower(strings.TrimSpace(s))
		}
		return out
	}
	allCols := lower(cols)
	conflict := lower(conflictCols)
	if len(conflict) == 0 {
		return "", fmt.Errorf("dialect replace statement needs at least one conflict column")
	}
	sort.Strings(conflict)
	inConflict := make(map[string]bool, len(conflict))
	for _, c := range conflict {
		inConflict[c] = true
	}

	var updates []string
	for _, c := range allCols {
		if !inConflict[c] {
			updates = append(updates, c+" = EXCLUDED."+c)
		}
	}
	if len(updates) == 0 {
		return "", fmt.Errorf("dialect replace statement has no non-conflict columns to update")
	}

	pg := "INSERT INTO " + table + " (" + strings.Join(allCols, ", ") + ") VALUES (" + valueList + ")" +
		" ON CONFLICT (" + strings.Join(conflict, ", ") + ") DO UPDATE SET " + strings.Join(updates, ", ")
	return rebindPlaceholders(pg), nil
}

// splitIdentList splits a comma-separated column list.
func splitIdentList(list string) []string {
	var out []string
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// pgDDLOverrides translates the portable schema DDL's SQLite-flavored bits:
// binary columns, 64-bit integers (SQLite INTEGER is 8 bytes; PostgreSQL
// INTEGER is 4 — unix-nano timestamps need BIGINT), and the text render of
// CURRENT_TIMESTAMP defaults (a text column cannot take a raw timestamptz
// default in PostgreSQL).
var pgDDLOverrides = []struct{ from, to string }{
	{"BLOB", "BYTEA"},
	{"INTEGER", "BIGINT"},
	{"DEFAULT CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP::text"},
}

func (postgresDialect) execScript(ctx context.Context, db storeDB, script string) error {
	for _, o := range pgDDLOverrides {
		script = strings.ReplaceAll(script, o.from, o.to)
	}
	// The extended query protocol forbids multiple statements per round
	// trip; split the script. The schema DDL contains no statement bodies
	// with embedded semicolons (no function bodies, no literals with `;`).
	for _, part := range strings.Split(script, ";") {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec DDL %q: %w", firstLine(stmt), err)
		}
	}
	return nil
}

func (postgresDialect) createTempTableIfNotExists(ctx context.Context, q queryer, name string, columnDDL string) error {
	_, err := q.ExecContext(ctx, "CREATE TEMP TABLE IF NOT EXISTS "+name+"("+columnDDL+")")
	return err
}

func (postgresDialect) dropTempTableIfExists(ctx context.Context, q queryer, name string) error {
	// pg_temp is PostgreSQL's session temp schema; qualifying the drop
	// keeps it from touching a persistent table of the same name.
	_, err := q.ExecContext(ctx, "DROP TABLE IF EXISTS pg_temp."+name)
	return err
}

// lockOwnerOrdinalRows materializes each owner's ordinal row and takes a
// transaction-length write lock on it. Materialization first is what makes
// the lock real: FOR UPDATE on an absent row locks nothing, so two first
// appends to a fresh owner would otherwise both read ordinal 0. With the
// row pinned, one transaction's append blocks the other's allocation and
// ordinals stay gapless.
//
// The retry covers a concurrent eviction deleting the row between this
// transaction's INSERT (which skipped, conflicting with the doomed row)
// and its SELECT (which then finds nothing): the next iteration inserts
// afresh. Each retry makes progress — once the tombstone is committed the
// row survives and the caller's tombstone check refuses the append — so a
// small bound is enough and no backoff is needed.
func (postgresDialect) lockOwnerOrdinalRows(ctx context.Context, q queryer, ownerIDs []string) error {
	for _, owner := range ownerIDs {
		for attempt := 0; ; attempt++ {
			if _, err := q.ExecContext(ctx,
				`INSERT INTO owner_ordinals(trace_owner_id, next_ordinal) VALUES (?, 0)
				 ON CONFLICT (trace_owner_id) DO NOTHING`, owner); err != nil {
				return fmt.Errorf("materialize owner ordinal row: %w", err)
			}
			var next int
			err := q.QueryRowContext(ctx,
				`SELECT next_ordinal FROM owner_ordinals WHERE trace_owner_id = ? FOR UPDATE`, owner,
			).Scan(&next)
			if err == sql.ErrNoRows {
				if attempt >= 2 {
					return fmt.Errorf("lock owner ordinal row: row for %q vanished %d times under concurrent eviction", owner, attempt+1)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("lock owner ordinal row: %w", err)
			}
			break
		}
	}
	return nil
}

// lockCommitSeqRow pins the commit-sequence meta row for the transaction,
// so concurrent appends cannot read the same counter value and mint
// duplicate commit receipts.
func (postgresDialect) lockCommitSeqRow(ctx context.Context, q queryer) error {
	var value string
	if err := q.QueryRowContext(ctx,
		`SELECT value FROM meta WHERE key = 'next_commit_seq' FOR UPDATE`).Scan(&value); err != nil {
		return fmt.Errorf("lock commit seq row: %w", err)
	}
	return nil
}

// lockAppendIntent takes a transaction-scoped advisory lock keyed on the
// intent id, so two concurrent first-time appends of the same intent
// serialize: the loser's idempotency read then finds the winner's committed
// receipt and replays it, exactly as SQLite's single-connection store does.
// The lock releases with the transaction.
func (postgresDialect) lockAppendIntent(ctx context.Context, q queryer, intentID string) error {
	if _, err := q.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(?, 0))`, intentID); err != nil {
		return fmt.Errorf("lock append intent: %w", err)
	}
	return nil
}

// lockLivenessRecompute takes the transaction-scoped advisory lock shared
// by appends and garbage collection, so a GC sweep's dead set cannot go
// stale against an append committing reachability mid-sweep. Both sides
// take it first in their transactions; the fixed key serializes them
// against each other while same-kind transactions queue harmlessly.
func (postgresDialect) lockLivenessRecompute(ctx context.Context, q queryer) error {
	if _, err := q.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(?, 0))`, gcCoordinationLockKey); err != nil {
		return fmt.Errorf("lock liveness recompute: %w", err)
	}
	return nil
}

// gcCoordinationLockKey keys the append/GC coordination advisory lock. A
// constant: the lock serializes kinds of transactions, not rows.
const gcCoordinationLockKey = "goatherd-kernel:liveness-recompute"

// lockExistingOwnerOrdinalRow locks an owner's ordinal row without
// creating it, for callers (eviction) that must serialize against appends
// but must not make an unknown owner look known — a materialized row would
// flip EvictOwnerPath's "no such owner path" check.
func (postgresDialect) lockExistingOwnerOrdinalRow(ctx context.Context, q queryer, ownerID string) error {
	var next int
	err := q.QueryRowContext(ctx,
		`SELECT next_ordinal FROM owner_ordinals WHERE trace_owner_id = ? FOR UPDATE`, ownerID,
	).Scan(&next)
	if err == sql.ErrNoRows {
		return nil // nothing to lock; the owner has no allocation state
	}
	if err != nil {
		return fmt.Errorf("lock owner ordinal row: %w", err)
	}
	return nil
}
