package trace

// SQLite trace store: the store type, construction, options, and the
// error taxonomy. The append, read, frontier, witness, context, and record
// seams live in their own files in this package.

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrUnknownFrontier is the expected "no retained cutoff for this frontier id"
// condition, distinguished from real read faults so callers can retry it
// deliberately instead of spinning on every error.
var ErrUnknownFrontier = errors.New("unknown frontier")

// unknownFrontierError keeps the store's message shape while matching the
// sentinel through errors.Is.
type unknownFrontierError struct{ frontierID string }

func (e *unknownFrontierError) Error() string {
	return fmt.Sprintf("unknown frontier id: %q", e.frontierID)
}

func (e *unknownFrontierError) Is(target error) bool { return target == ErrUnknownFrontier }

// TraceStoreError is the base error for trace store law violations.
type TraceStoreError struct {
	msg string
}

func (e *TraceStoreError) Error() string { return e.msg }

// AppendIntentConflictError is raised when an append intent is retried with a different batch.
type AppendIntentConflictError struct {
	msg string
}

func (e *AppendIntentConflictError) Error() string { return e.msg }

// UnknownFactError is raised when a retained fact references a missing fact.
type UnknownFactError struct {
	msg string
}

func (e *UnknownFactError) Error() string { return e.msg }

const witnessTraceOwnerID = "kernel:witness"

// SQLiteTraceStore is the canonical TraceStore implementation backed by
// SQLite. All backend-specific behavior is delegated to its dialect; the
// store methods themselves speak portable SQL through that seam, which is
// what keeps the storage family swappable (see dialect.go, store_config.go).
type SQLiteTraceStore struct {
	path string
	db   storeDB
	mu   sync.Mutex
	// dialect is the backend seam resolved at open; every catalog probe,
	// conflict-clause statement, and connection concern routes through it.
	dialect dialect
	// substrateRef is recorded in retained contexts, witness bodies, and
	// frontier facts that do not carry an explicit substrate reference.
	// DefaultSubstrateRef unless overridden at open.
	substrateRef string
	bus          *EffectBus // optional; nil = no real-time publishing
	// checkpointBlobLimit bounds the caller snapshot bytes persisted per
	// checkpoint (plan 05 §4). Zero means DefaultCheckpointBlobLimit.
	checkpointBlobLimit int64
	// blobLimit bounds one named blob written by PutBlob. Zero means
	// DefaultBlobLimit.
	blobLimit int64
	// hasEvictedPaths records whether the evicted_paths table exists in
	// this database, fixed once at open: createSchema always creates it on
	// a read-write open, and the read-only opener verifies the schema. The
	// tombstone probe uses it to skip the catalog check on hot paths.
	hasEvictedPaths bool
	// hasNamedBlobs records whether the named_blobs table exists, fixed at
	// open. A database written before named blobs opens read-only without
	// it (behind the schema marker gate); blob reads then report absence
	// rather than a raw "no such table" store fault.
	hasNamedBlobs bool
}

// WithBus attaches an effect bus to the store. When set, every successful
// Append publishes an EffectEvent for each retained record. This enables
// real-time supervision without modifying the store's persistence behavior.
//
// The bus is optional — existing code that doesn't set a bus sees no change.
//
// WithBus is not synchronized against Append: attach the bus as part of
// store construction, before the store serves its first append. Attaching
// while appends are in flight is a data race on the bus field.
func (s *SQLiteTraceStore) WithBus(bus *EffectBus) *SQLiteTraceStore {
	s.bus = bus
	return s
}

// WithCheckpointBlobLimit bounds the opaque snapshot bytes persisted per
// checkpoint. A checkpoint whose snapshot exceeds the limit fails loudly at
// creation — the host externalizes large state instead of silently growing
// the trace database. Zero restores the default.
func (s *SQLiteTraceStore) WithCheckpointBlobLimit(limit int64) *SQLiteTraceStore {
	if limit <= 0 {
		limit = DefaultCheckpointBlobLimit
	}
	s.checkpointBlobLimit = limit
	return s
}

// DefaultCheckpointBlobLimit is the default per-checkpoint snapshot cap:
// 1 MiB, generous for serialized conversation history, small enough that a
// long session cannot balloon the trace database unnoticed.
const DefaultCheckpointBlobLimit = 1 << 20

// NewSQLiteTraceStore creates a new trace store at the given path.
// Use ":memory:" for an in-memory store.
//
// It is the SQLite-specific shorthand for Open: new code should prefer
// Open(StoreConfig{DSN: ...}) so the backend stays swappable.
func NewSQLiteTraceStore(path string) (*SQLiteTraceStore, error) {
	return OpenSQLite(StoreConfig{Backend: BackendSQLite, DSN: path})
}

// OpenSQLiteTraceStoreReadOnly opens an existing trace database for reading
// only. It is the viewer's opener: unlike NewSQLiteTraceStore it never
// writes — no journal-mode pragma runs and no schema is created — so a
// missing, empty, or non-trace file fails loudly instead of materializing
// a valid-looking empty trace. The database must already hold the full
// trace schema, which is validated without DDL.
//
// The path names a file, never ":memory:".
func OpenSQLiteTraceStoreReadOnly(path string) (*SQLiteTraceStore, error) {
	if err := validateReadOnlyDSN(path); err != nil {
		return nil, err
	}
	return OpenSQLite(StoreConfig{Backend: BackendSQLite, DSN: path, ReadOnly: true})
}

// validateReadOnlyDSN rejects a read-only DSN that cannot name an existing
// database file.
func validateReadOnlyDSN(dsn string) error {
	if dsn == "" || dsn == ":memory:" {
		return fmt.Errorf("read-only open requires an existing database file, not %q", dsn)
	}
	return nil
}

// openWithDialect is the shared open path for every backend: connect through
// the dialect, then either create the schema or validate an existing one.
func openWithDialect(d dialect, cfg StoreConfig) (*SQLiteTraceStore, error) {
	if cfg.ReadOnly {
		if err := validateReadOnlyDSN(cfg.DSN); err != nil {
			return nil, err
		}
	}

	db, err := d.open(cfg.DSN, cfg.ReadOnly)
	if err != nil {
		if cfg.ReadOnly {
			return nil, fmt.Errorf("open sqlite read-only: %w", err)
		}
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	bound := d.bind(db)

	s := &SQLiteTraceStore{
		path:         cfg.DSN,
		db:           bound,
		dialect:      d,
		bus:          cfg.Bus,
		substrateRef: DefaultSubstrateRef,
	}
	if cfg.SubstrateRef != "" {
		s.substrateRef = cfg.SubstrateRef
	}
	if cfg.BlobLimit > 0 {
		s.WithBlobLimit(cfg.BlobLimit)
	}
	if cfg.CheckpointBlobLimit > 0 {
		s.WithCheckpointBlobLimit(cfg.CheckpointBlobLimit)
	}

	if cfg.ReadOnly {
		tables, err := s.validateExistingSchema()
		if err != nil {
			db.Close()
			return nil, err
		}
		// Evicted paths can only exist if the retention schema does, and
		// named blobs only if the blob schema does; one catalog read at
		// open lets the later probes skip the lookup entirely.
		s.hasEvictedPaths = tables["evicted_paths"]
		s.hasNamedBlobs = tables["named_blobs"]
		return s, nil
	}

	s.hasEvictedPaths = true
	s.hasNamedBlobs = true
	if err := s.createSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return s, nil
}

// Close closes the underlying database connection.
func (s *SQLiteTraceStore) Close(ctx context.Context) error {
	return s.db.Close()
}
