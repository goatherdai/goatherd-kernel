# Storage backends

The trace store's backend is configuration, not code: the ledger schema,
record ids, receipts, and laws are identical everywhere; only the dialect
(connection, catalog, conflict syntax, concurrency control) differs.

| | SQLite (`backend: sqlite`, default) | PostgreSQL (`backend: postgres`) |
|---|---|---|
| DSN | file path, or `:memory:` | connection URL (`postgres://user:pass@host/db?sslmode=disable`) |
| Driver | `modernc.org/sqlite` (pure Go) | `jackc/pgx/v5` stdlib |
| Concurrency | single-writer file lock; one pooled connection serializes appends | row locks + advisory locks; cross-instance appends serialize on allocation points (see below) |
| Read-only opens | `file:…?mode=ro` URI, never writes | `default_transaction_read_only=on` session parameter |
| Schema checks | marker-gated tables + version stamp in `meta` | same scheme via `information_schema` |
| Viewer | `traceui -db trace.sqlite` | `traceui -backend postgres -db 'postgres://…'` — use a read-only role |

Both backends are exercised by the same law suite: set
`KERNEL_PG_DSN=postgres://…` and the full suite (plus the cross-instance
concurrency gate) runs against PostgreSQL; unset, everything stays SQLite.

## Choosing

- **SQLite** is the zero-ops default: one file per store, snapshot reads for
  cross-process viewers (WAL), no server to run. Covers a single machine.
- **PostgreSQL** is for a shared ledger: many machines appending to one
  trace, server-side backups, and role-based access. Record and context ids
  are backend-independent (content digests), so the *same* logical ledger is
  portable between them with `tracectl migrate`.

## Appending under PostgreSQL: what the locks guarantee

SQLite gets its append guarantees from a single writer. PostgreSQL gets them
from explicit coordination inside each append transaction:

- the owner's **ordinal row** is materialized and locked (`INSERT … ON
  CONFLICT DO NOTHING` + `SELECT … FOR UPDATE`), so concurrent appends to one
  owner allocate gapless ordinals;
- the **commit-sequence row** is locked, so commit receipts stay unique;
- an **advisory lock on the append intent id** serializes same-intent racing,
  so the loser replays the winner's receipt instead of erroring;
- a **liveness-recompute advisory lock** shared with `CollectGarbage` keeps a
  sweep's dead set from consuming entries an append commits mid-sweep.

Cross-instance appends therefore serialize on the allocation rows (the
`kernel:witness` row in particular) — PostgreSQL's win is a *shared ledger*,
not append parallelism. Relaxing that (per-owner pipelines, `RETURNING`-based
allocation) would change commit-visible ordinal monotonicity and is
deliberately deferred.

## Backup and restore

- **SQLite**: stop writers (or rely on WAL), copy the file pair
  (`trace.db` + `-wal`/`-shm`) — or use `sqlite3 .backup`. Restore is a file
  copy; the read-only opener validates the schema on next use.
- **PostgreSQL**: `pg_dump`/`pg_restore` as usual; the ledger is ordinary
  tables. A database stamped by a *newer* kernel is refused at open (schema
  version gate in `meta.schema:version`), so restore-then-upgrade follows
  the usual order: restore, upgrade the kernel, open.

## Migrating an existing ledger (SQLite → PostgreSQL)

One-shot, move-not-merge:

```sh
tracectl migrate \
  --from ~/.yaah/trace.db \
  --to-backend postgres \
  --to 'postgres://trace:s3cret@db.internal:5432/traces'
tracectl verify --backend postgres --dsn 'postgres://trace:…@db.internal:5432/traces'
traceui -backend postgres -db 'postgres://trace_reader:…@db.internal:5432/traces'
```

Contracts:

- the source opens **read-only** and is never modified;
- the target must be **empty** — migration is a move, never a merge (a
  merged ledger would be a plausible wrong answer: commit receipts and
  ordinals are positional);
- the copy preserves record ids, ordinals, receipts, substrate references,
  and blob bytes byte-for-byte, and verifies per-table counts, the target's
  own counters, and the commit-sequence watermark;
- future appends on the target extend the migrated id space: record ids
  digest over the same default substrate on both backends;
- after migrating, point the writer at PostgreSQL
  (`shepherd_trace.backend: postgres` in yaah) and keep the SQLite file as
  the historical archive — it still opens read-only, unchanged.

## Viewer credentials (PostgreSQL)

Create a read-only role and hand it to `traceui`; the viewer's open sets
`default_transaction_read_only=on` regardless, so even the write-capable
DSN cannot modify the ledger through the viewer:

```sql
CREATE ROLE trace_reader LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE traces TO trace_reader;
GRANT USAGE ON SCHEMA public TO trace_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO trace_reader;
```

## Decisions on record

- **pgx stays an in-package import** (not a build tag or separate module):
  SQLite-only consumers carry `pgx` in `go.sum` but no runtime cost, and the
  dialect needs no indirection. Revisit only if a consumer must build with
  the module graph excluding pgx.
- **Serialized appends are the contract** (see above); parallel append
  pipelines are future work gated on relaxing commit-visible ordinal
  monotonicity.
