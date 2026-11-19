package trace

// The dialect seam: everything a storage backend family contributes to the
// trace store beyond portable database/sql. The store's statements are
// otherwise vanilla SQL — content-addressed rows, application-managed
// sequences, no vendor functions — so the seam is small by design:
//
//   - connection setup (driver registration, pragmas, pool sizing, URI forms)
//   - catalog probes (which tables exist)
//   - conflict-clause syntax for the two upsert shapes the store needs
//   - multi-statement DDL execution
//   - temporary staging tables (the GC sweep)
//
// A new backend implements dialect plus the TraceStore plumbing it inherits
// from the shared store methods; nothing outside dialect_*.go may reference
// backend specifics (the leakage gate in dialect_gate_test.go enforces this).

import (
	"context"
	"database/sql"
	"fmt"
)

// Backend names a storage backend family. The empty value selects the
// default (BackendSQLite).
type Backend string

// BackendSQLite is the embedded SQLite backend: a pure-Go driver, a
// single-writer database file or an in-process store.
const BackendSQLite Backend = "sqlite"

// BackendPostgres is the PostgreSQL backend (pgx driver, real connection
// pool, MVCC concurrency).
const BackendPostgres Backend = "postgres"

// queryer covers *sql.DB and *sql.Tx — the two execution contexts the
// store's statements run against. Context-aware variants, so cancellation
// and deadlines reach every statement the dialect executes.
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// storeDB and storeTx are the execution surfaces the store uses. The
// dialect binds the driver's raw handles through them, so the store's
// statement text stays portable (`?` placeholders) while the wire form is
// backend-owned — the PostgreSQL dialect rebinds `?` to `$n` here, at the
// single point every statement crosses.
type storeDB interface {
	Close() error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (storeTx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type storeTx interface {
	Commit() error
	Rollback() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// passthroughBinding is the SQLite binding: statements go to database/sql
// exactly as written.
type passthroughDB struct{ db *sql.DB }

func (p passthroughDB) Close() error { return p.db.Close() }

func (p passthroughDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (storeTx, error) {
	tx, err := p.db.BeginTx(ctx, opts)
	return passthroughTx{tx}, err
}

func (p passthroughDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, query, args...)
}

func (p passthroughDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return p.db.QueryContext(ctx, query, args...)
}

func (p passthroughDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return p.db.QueryRowContext(ctx, query, args...)
}

func (p passthroughDB) Exec(query string, args ...any) (sql.Result, error) {
	return p.db.Exec(query, args...)
}

func (p passthroughDB) Query(query string, args ...any) (*sql.Rows, error) {
	return p.db.Query(query, args...)
}

func (p passthroughDB) QueryRow(query string, args ...any) *sql.Row {
	return p.db.QueryRow(query, args...)
}

type passthroughTx struct{ tx *sql.Tx }

func (p passthroughTx) Commit() error { return p.tx.Commit() }

func (p passthroughTx) Rollback() error { return p.tx.Rollback() }

func (p passthroughTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.tx.ExecContext(ctx, query, args...)
}

func (p passthroughTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return p.tx.QueryContext(ctx, query, args...)
}

func (p passthroughTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return p.tx.QueryRowContext(ctx, query, args...)
}

func (p passthroughTx) Exec(query string, args ...any) (sql.Result, error) {
	return p.tx.Exec(query, args...)
}

func (p passthroughTx) Query(query string, args ...any) (*sql.Rows, error) {
	return p.tx.Query(query, args...)
}

func (p passthroughTx) QueryRow(query string, args ...any) *sql.Row {
	return p.tx.QueryRow(query, args...)
}

// dialect encapsulates the backend-specific behaviors of the trace store.
// Statement-executing methods take ctx so cancellation reaches the SQL
// layer; `open` is the exception (the legacy constructors carry no context
// at open time). Callers with no context of their own — open-time DDL,
// the context-free ledger API — pass context.Background().
type dialect interface {
	// name returns the backend name, used in errors and diagnostics.
	name() string

	// open connects to the store named by dsn. readOnly must open without
	// any write capability and must never create a schema. The returned
	// pool is pre-configured for the backend's concurrency model (for the
	// SQLite dialect: a single connection, serializing every statement).
	open(dsn string, readOnly bool) (*sql.DB, error)

	// bind wraps the driver's handle in the store's execution surface,
	// translating statement text at that single boundary (placeholder
	// form, protocol quirks). The store keeps writing portable SQL.
	bind(db *sql.DB) storeDB

	// listTables returns the set of table names in the store's catalog.
	// Open-time only; replaces backend-specific catalog probes.
	listTables(ctx context.Context, q queryer) (map[string]bool, error)

	// insertOrIgnore executes a plain portable INSERT statement under
	// ignore-on-conflict semantics: a row violating a uniqueness constraint
	// leaves the table unchanged and reports zero rows affected. stmt must
	// begin with the INSERT keyword and carry no conflict clause of its own.
	insertOrIgnore(ctx context.Context, q queryer, stmt string, args ...any) (sql.Result, error)

	// insertOrReplace executes a plain portable INSERT statement under
	// replace-on-conflict semantics: a row colliding with conflictCols is
	// fully overwritten by the new row. stmt must begin with the INSERT
	// keyword and carry no conflict clause of its own.
	insertOrReplace(ctx context.Context, q queryer, stmt string, conflictCols []string, args ...any) (sql.Result, error)

	// execScript runs a multi-statement DDL script through the bound handle.
	execScript(ctx context.Context, db storeDB, script string) error

	// createTempTableIfNotExists creates a session-local staging table with
	// the given column DDL. The table is visible by its unqualified name for
	// the life of the connection.
	createTempTableIfNotExists(ctx context.Context, q queryer, name string, columnDDL string) error

	// dropTempTableIfExists drops a staging table created by
	// createTempTableIfNotExists; absence is not an error.
	dropTempTableIfExists(ctx context.Context, q queryer, name string) error

	// lockOwnerOrdinalRows materializes the ordinal row for every owner id
	// (creating it at next_ordinal 0 when absent) and takes a write lock on
	// each, held for the caller's transaction, so concurrent appends to one
	// owner allocate gapless ordinals. No-op on backends whose connection
	// model already serializes writes (SQLite's single connection).
	lockOwnerOrdinalRows(ctx context.Context, q queryer, ownerIDs []string) error

	// lockCommitSeqRow takes a write lock on the meta row backing the
	// global commit-sequence counter, so concurrent appends allocate unique
	// commit receipts. No-op on serializing backends.
	lockCommitSeqRow(ctx context.Context, q queryer) error

	// lockAppendIntent serializes concurrent first-time appends of one
	// append intent id for the transaction's duration, so the
	// check-then-insert idempotency read cannot race: on a serializing
	// backend this is a no-op. Locking an intent already committed by a
	// concurrent transaction must block until it commits, then return; the
	// caller's idempotency read then finds the stored receipt.
	lockAppendIntent(ctx context.Context, q queryer, intentID string) error

	// lockLivenessRecompute coordinates liveness-affecting transactions —
	// appends (which make records reachable) and garbage collection (which
	// reclaims unreachable ones) — for the transaction's duration. GC
	// computes its dead set from reads taken inside its transaction; an
	// append committing a path entry for a not-yet-reachable record after
	// those reads would otherwise lose the entry to the sweep while its
	// caller holds a successful receipt. Held by both sides, this makes
	// their read/modify windows mutually exclusive. No-op on serializing
	// backends.
	lockLivenessRecompute(ctx context.Context, q queryer) error

	// lockExistingOwnerOrdinalRow takes a transaction-length write lock on
	// an owner's ordinal row if it exists (creating nothing), so eviction
	// and append serialize on the same row before either reads owner state.
	// A missing row locks nothing and reports no error. No-op on
	// serializing backends.
	lockExistingOwnerOrdinalRow(ctx context.Context, q queryer, ownerID string) error
}

// dialectFor resolves a Backend to its dialect. The empty backend selects
// the default. Unknown backends fail with an error naming the supported set,
// so a mistyped config value fails loudly at open rather than silently
// opening the wrong store.
func dialectFor(b Backend) (dialect, error) {
	switch b {
	case "", BackendSQLite:
		return sqliteDialect{}, nil
	case BackendPostgres:
		return postgresDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported store backend %q: supported backends: %s, %s", string(b), BackendSQLite, BackendPostgres)
	}
}
