package trace

// The SQLite dialect: the backend-specific half of the trace store. Every
// SQLite-only concern lives here — the driver registration, the pragmas,
// the single-writer pool, the read-only URI form, the catalog probe, and
// the conflict-clause syntax. The rest of the package speaks portable SQL
// through the dialect interface (see dialect.go).

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

// sqliteDialect is the dialect for embedded SQLite stores.
type sqliteDialect struct{}

func (sqliteDialect) name() string { return string(BackendSQLite) }

func (sqliteDialect) open(dsn string, readOnly bool) (*sql.DB, error) {
	path := dsn
	if readOnly {
		// The URI filename form requires percent-encoding: paths containing
		// SQLite URI metacharacters ('?', '#') must not be read as options.
		path = "file:" + url.PathEscape(dsn) + "?mode=ro"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite is single-writer

	if readOnly {
		// No journal-mode pragma: switching the journal mode writes to the
		// database file, which a read-only connection must not do. The busy
		// timeout and foreign-keys pragmas are connection-local and safe.
		if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
			db.Close()
			return nil, fmt.Errorf("set busy timeout: %w", err)
		}
		if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
			db.Close()
			return nil, fmt.Errorf("enable foreign keys: %w", err)
		}
		return db, nil
	}

	// One pooled connection serializes every statement in-process: reads
	// queue behind an open write transaction at the pool, so in-process
	// SQLITE_BUSY never arises and the busy timeout below only governs
	// cross-process lock contention. WAL is still worth setting because it
	// is a persistent file property — cross-process readers (the trace UI's
	// read-only opener, the Python reference) get snapshot reads instead of
	// blocking on the writer's lock.
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	return db, nil
}

func (sqliteDialect) listTables(q queryer) (map[string]bool, error) {
	rows, err := q.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)
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

func (sqliteDialect) insertOrIgnore(q queryer, stmt string, args ...any) (sql.Result, error) {
	rewritten, err := rewriteInsert(stmt, "OR IGNORE")
	if err != nil {
		return nil, err
	}
	return q.Exec(rewritten, args...)
}

func (sqliteDialect) insertOrReplace(q queryer, stmt string, conflictCols []string, args ...any) (sql.Result, error) {
	rewritten, err := rewriteInsert(stmt, "OR REPLACE")
	if err != nil {
		return nil, err
	}
	return q.Exec(rewritten, args...)
}

func (sqliteDialect) execScript(db *sql.DB, script string) error {
	_, err := db.Exec(script)
	return err
}

func (sqliteDialect) createTempTableIfNotExists(q queryer, name string, columnDDL string) error {
	_, err := q.Exec("CREATE TEMP TABLE IF NOT EXISTS " + name + "(" + columnDDL + ")")
	return err
}

func (sqliteDialect) dropTempTableIfExists(q queryer, name string) error {
	// The temp schema qualifier resolves against this connection's temp
	// tables without colliding with a persistent table of the same name.
	_, err := q.Exec("DROP TABLE IF EXISTS temp." + name)
	return err
}

// rewriteInsert prefixes the conflict keyword into a plain INSERT statement:
// "INSERT INTO t ..." becomes "INSERT OR IGNORE INTO t ..." (or OR REPLACE).
// The store supplies only portable statements, so the rewrite is anchored to
// the leading keyword; anything else is a caller bug and fails loudly.
func rewriteInsert(stmt, conflict string) (string, error) {
	trimmed := strings.TrimLeft(stmt, " \t\r\n")
	if !strings.HasPrefix(trimmed, "INSERT ") {
		return "", fmt.Errorf("dialect conflict clause expects a plain INSERT statement, got %q", firstLine(stmt))
	}
	if strings.HasPrefix(trimmed, "INSERT OR") {
		return "", fmt.Errorf("dialect conflict statement already carries a conflict clause: %q", firstLine(stmt))
	}
	const keyword = "INSERT"
	idx := strings.Index(stmt, keyword)
	return stmt[:idx] + "INSERT " + conflict + stmt[idx+len(keyword):], nil
}

// firstLine renders the first line of a statement for error messages.
func firstLine(stmt string) string {
	if i := strings.IndexByte(stmt, '\n'); i >= 0 {
		return strings.TrimSpace(stmt[:i])
	}
	return strings.TrimSpace(stmt)
}
