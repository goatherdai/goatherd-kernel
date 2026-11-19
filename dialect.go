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

// queryer covers *sql.DB and *sql.Tx — the two execution contexts the
// store's statements run against. Context-aware variants, so cancellation
// and deadlines reach every statement the dialect executes.
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

	// execScript runs a multi-statement DDL script on db.
	execScript(ctx context.Context, db *sql.DB, script string) error

	// createTempTableIfNotExists creates a session-local staging table with
	// the given column DDL. The table is visible by its unqualified name for
	// the life of the connection.
	createTempTableIfNotExists(ctx context.Context, q queryer, name string, columnDDL string) error

	// dropTempTableIfExists drops a staging table created by
	// createTempTableIfNotExists; absence is not an error.
	dropTempTableIfExists(ctx context.Context, q queryer, name string) error
}

// dialectFor resolves a Backend to its dialect. The empty backend selects
// the default. Unknown backends fail with an error naming the supported set,
// so a mistyped config value fails loudly at open rather than silently
// opening the wrong store.
func dialectFor(b Backend) (dialect, error) {
	switch b {
	case "", BackendSQLite:
		return sqliteDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported store backend %q: supported backends: %s", string(b), BackendSQLite)
	}
}
