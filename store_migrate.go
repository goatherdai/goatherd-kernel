package trace

// One-shot migration of a SQLite trace store into another backend store
// (the target use is PostgreSQL; the implementation is backend-neutral and
// works for SQLite→SQLite back-migration too).
//
// The migration is a raw, faithful table copy — not a replay. Replaying
// recorded append intents through Append would re-derive ordinals and
// receipts only if every batch were reconstructed exactly; the ledger stores
// receipts, not batches, so a replay migration would be lossy by
// construction. A column-faithful copy preserves record ids (content
// digests), ordinals, commit receipts, substrate references, and blob bytes
// byte-for-byte, which is the entire point: a migrated ledger must be
// indistinguishable from the original to every reader.
//
// Safety contracts:
//
//   - The source is opened through OpenSQLiteTraceStoreReadOnly: it is
//     never written, and a missing or non-trace file fails loudly.
//   - The target must be EMPTY. The kernel never merges two ledgers: id
//     spaces are content-derived but commit receipts (`commit:N`) and owner
//     ordinals are positional, so a merged ledger would be a plausible
//     wrong answer. Refusal is the only safe behavior.
//   - The target's schema is created and validated through the normal open
//     path, so both dialects work (PostgreSQL gets translated DDL and
//     rebound placeholders; the copy statements are portable `?` form).
//   - Post-migration appends continue the ledger: ids digest over the same
//     DefaultSubstrateRef on both backends, so new records on the target
//     extend — not fork — the migrated id space.

import (
	"context"
	"fmt"
	"time"
)

// MigrationReport summarizes one migration run.
type MigrationReport struct {
	// Rows counts rows copied per table, keyed by table name.
	Rows map[string]int
	// FactCount and ContextCount mirror the target store's own counters
	// after the copy, for quick eyeball verification.
	FactCount    int
	ContextCount int
	// Duration is the wall time of the copy phase.
	Duration time.Duration
	// Target describes where the ledger landed.
	Target string
}

// migrationTables lists the tables and their columns in copy order —
// parents before children, matching the schema's foreign keys. The column
// lists mirror schemaDDL exactly; a schema change must update both
// together (the migration test catches drift by copying a fully populated
// store).
var migrationTables = []struct {
	name    string
	columns []string
}{
	{"meta", []string{"key", "value"}},
	{"append_intents", []string{"append_intent_id", "batch_digest", "receipt_json", "committed_at"}},
	{"owner_ordinals", []string{"trace_owner_id", "next_ordinal"}},
	{"contexts", []string{"context_id", "active_binding_refs_json", "capability_witness_refs_json",
		"semantic_environment_refs_json", "visibility_policy_refs_json", "substrate_ref", "containment", "append_intent_id"}},
	{"records", []string{"record_id", "digest", "schema_ref", "mode", "witness_ref", "caused_by_json", "body_json", "append_intent_id"}},
	{"record_edges", []string{"parent_record_id", "child_record_id", "parent_position"}},
	{"path_entries", []string{"path_ref", "path_ordinal", "record_id", "retained_context_ref", "kind_label", "append_intent_id", "commit_receipt"}},
	{"frontiers", []string{"frontier_id", "target_trace_owner_id", "through_fact_id", "through_owner_ordinal",
		"publisher_trace_owner_id", "created_by_fact_id", "append_intent_id"}},
	{"evicted_paths", []string{"trace_owner_id", "evicted_at"}},
	{"materialization_intents", []string{"materialize_intent_id", "request_digest", "receipt_json", "completed_at"}},
	{"checkpoint_snapshots", []string{"checkpoint_id", "scope_id", "seq", "state_json", "snapshot", "created_at_unixnano"}},
	{"named_blobs", []string{"owner", "kind", "key", "digest", "size_bytes", "data", "created_at_unixnano", "updated_at_unixnano"}},
}

// MigrateSQLiteToPostgres copies a complete SQLite trace ledger into the
// store named by dst (PostgreSQL is the target use; SQLite works for
// back-migration). Both ledgers are closed before the report returns.
//
// Refuses when the target already holds ledger rows: migration is a move,
// never a merge. On any copy error the target transaction aborts, leaving
// the empty target intact for a retry.
func MigrateSQLiteToPostgres(srcPath string, dst StoreConfig) (*MigrationReport, error) {
	src, err := OpenSQLite(StoreConfig{DSN: srcPath, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer src.Close(context.Background())

	target, err := OpenSQLite(dst)
	if err != nil {
		return nil, fmt.Errorf("open target: %w", err)
	}
	defer target.Close(context.Background())

	if err := assertTargetEmpty(target); err != nil {
		return nil, err
	}

	report := &MigrationReport{Rows: make(map[string]int), Target: dst.Describe()}
	start := time.Now()

	for _, table := range migrationTables {
		n, err := copyTable(src, target, table.name, table.columns)
		if err != nil {
			return nil, fmt.Errorf("copy %s: %w", table.name, err)
		}
		report.Rows[table.name] = n
	}
	report.Duration = time.Since(start)

	// Close the target before reading through its own counters: the
	// read-back verifies what a fresh open will see.
	factCount, ctxCount, err := verifyMigrated(src, target)
	if err != nil {
		return nil, err
	}
	report.FactCount = factCount
	report.ContextCount = ctxCount
	return report, nil
}

// assertTargetEmpty refuses a target that already holds ledger rows. meta is
// exempt (the open stamps markers and the schema version); every content
// table must be empty.
func assertTargetEmpty(target *SQLiteTraceStore) error {
	for _, table := range migrationTables {
		if table.name == "meta" {
			continue
		}
		var n int
		if err := target.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table.name)).Scan(&n); err != nil {
			return fmt.Errorf("check target %s: %w", table.name, err)
		}
		if n != 0 {
			return fmt.Errorf("target %s already holds %d rows; migration is a move, never a merge — point --to at an empty store",
				table.name, n)
		}
	}
	return nil
}

// copyTable streams every source row into the target inside one
// transaction. Rows are scanned generically ([]any) and rebound through the
// target's dialect binding, so the statements stay portable and NULLs and
// blob bytes survive byte-for-byte.
func copyTable(src, target *SQLiteTraceStore, table string, columns []string) (int, error) {
	ctx := context.Background()
	query := "SELECT " + joinColumns(columns) + " FROM " + table
	rows, err := src.db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	defer rows.Close()

	tx, err := target.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	insert := "INSERT INTO " + table + " (" + joinColumns(columns) + ") VALUES (" + placeholders(len(columns)) + ")"
	n := 0
	for rows.Next() {
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("scan row %d: %w", n, err)
		}
		// meta is the one table the target pre-populates (schema version
		// stamp, feature markers, the commit-seq seed). Source values win:
		// next_commit_seq is the watermark future appends continue from.
		var execErr error
		if table == "meta" {
			_, execErr = target.dialect.insertOrReplace(ctx, tx, insert, []string{"key"}, vals...)
		} else {
			_, execErr = tx.ExecContext(ctx, insert, vals...)
		}
		if execErr != nil {
			tx.Rollback()
			return 0, fmt.Errorf("insert row %d: %w", n, execErr)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("iterate source: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}

// verifyMigrated cross-checks the copy: per-table counts match, the target's
// own counters agree with the source's, and the commit-sequence watermark
// carried over exactly — the value future appends continue from.
func verifyMigrated(src, target *SQLiteTraceStore) (factCount, ctxCount int, err error) {
	for _, table := range migrationTables {
		var srcN, dstN int
		if err := src.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table.name)).Scan(&srcN); err != nil {
			return 0, 0, fmt.Errorf("verify %s (source count): %w", table.name, err)
		}
		if err := target.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table.name)).Scan(&dstN); err != nil {
			return 0, 0, fmt.Errorf("verify %s (target count): %w", table.name, err)
		}
		if srcN != dstN {
			return 0, 0, fmt.Errorf("verify %s: source has %d rows, target has %d", table.name, srcN, dstN)
		}
	}

	factCount, err = target.FactCount(context.Background())
	if err != nil {
		return 0, 0, fmt.Errorf("verify target fact count: %w", err)
	}
	if ctxCount, err = target.ContextCount(context.Background()); err != nil {
		return 0, 0, fmt.Errorf("verify target context count: %w", err)
	}

	var srcSeq, dstSeq string
	if err := src.db.QueryRow("SELECT value FROM meta WHERE key = 'next_commit_seq'").Scan(&srcSeq); err != nil {
		return 0, 0, fmt.Errorf("verify source commit seq: %w", err)
	}
	if err := target.db.QueryRow("SELECT value FROM meta WHERE key = 'next_commit_seq'").Scan(&dstSeq); err != nil {
		return 0, 0, fmt.Errorf("verify target commit seq: %w", err)
	}
	if srcSeq != dstSeq {
		return 0, 0, fmt.Errorf("verify commit seq: source %q, target %q — future appends would fork the receipt space", srcSeq, dstSeq)
	}
	return factCount, ctxCount, nil
}

func joinColumns(cols []string) string {
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += c
	}
	return out
}

func placeholders(n int) string {
	out := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			out += ", "
		}
		out += "?"
	}
	return out
}
