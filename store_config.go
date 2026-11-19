package trace

// Store configuration and the backend-neutral open path.
//
// Open(StoreConfig{...}) is the seam callers program against: it names the
// backend and its data source without spelling the driver out. The concrete
// constructors (NewSQLiteTraceStore, OpenSQLiteTraceStoreReadOnly) remain as
// compatibility shims over this path.

// DefaultSubstrateRef is the substrate reference recorded in retained
// contexts, witness bodies, and frontier facts when the caller does not
// supply one. It is stable across releases: record ids and context ids
// digest over it, so changing the default would fork id space.
const DefaultSubstrateRef = "sqlite.local.v1"

// StoreConfig describes how to open a trace store. The zero value opens the
// default backend with default limits and no bus.
type StoreConfig struct {
	// Backend selects the storage backend. Empty selects BackendSQLite.
	// Unknown values fail at Open with an error naming the supported set.
	Backend Backend

	// DSN is the backend data source name. For SQLite it is a database file
	// path, or ":memory:" for an in-process store. Required for a write
	// open; a read-only open additionally requires an existing database
	// file (":memory:" is refused).
	DSN string

	// ReadOnly opens an existing store without any write capability. Unlike
	// a write open it never creates the schema and never touches journal
	// state: a missing, empty, or non-trace database fails loudly instead of
	// materializing a valid-looking empty trace. The schema is validated
	// without DDL.
	ReadOnly bool

	// Bus attaches an effect bus. When set, every successful Append (and
	// frontier publish) publishes an EffectEvent for each retained record,
	// enabling real-time supervision without modifying persistence.
	Bus *EffectBus

	// BlobLimit bounds the bytes accepted by one PutBlob call. Zero or
	// negative restores DefaultBlobLimit.
	BlobLimit int64

	// CheckpointBlobLimit bounds the opaque snapshot bytes persisted per
	// checkpoint. Zero or negative restores DefaultCheckpointBlobLimit.
	CheckpointBlobLimit int64

	// SubstrateRef overrides the substrate reference recorded in retained
	// contexts, witness bodies, and frontier facts that do not carry an
	// explicit one. Empty selects DefaultSubstrateRef. Because context and
	// record ids digest over this value, overriding it forks id space: only
	// set it when the store is intentionally a different substrate.
	SubstrateRef string
}

// Open opens a trace store configured by cfg, returning the backend
// contract. This is the backend-neutral entry point: callers program against
// TraceStore and the StoreConfig fields, not the driver.
func Open(cfg StoreConfig) (TraceStore, error) {
	return OpenSQLite(cfg)
}

// OpenSQLite opens the SQLite-backed store per cfg, returning the concrete
// store type for callers that need the full surface (the builder options,
// the package-level blob and checkpoint helpers).
func OpenSQLite(cfg StoreConfig) (*SQLiteTraceStore, error) {
	d, err := dialectFor(cfg.Backend)
	if err != nil {
		return nil, err
	}
	return openWithDialect(d, cfg)
}
