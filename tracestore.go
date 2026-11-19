package trace

import "context"

// TraceStore is the backend contract: the method set every storage backend
// implements, independent of where the rows live. Consumers should program
// against this interface; the concrete constructors exist for callers that
// need the full concrete surface (the builder options and the package-level
// blob, checkpoint, and materialization helpers).
//
// The write and read seams are wide because the store *is* the kernel's
// protocol surface; narrower consumer-facing seams (a TraceSink of append +
// publish + close, for example) are defined by the consumers themselves, so
// each consumer keeps depending on only what it uses.
//
// All methods are safe for concurrent use.
type TraceStore interface {
	// Append appends a semantic batch or returns the prior receipt for its
	// intent. Idempotent per AppendIntentID.
	Append(ctx context.Context, auth AppendContext, batch AppendBatch) (AppendReceipt, error)

	// PreviewRecordIDs returns the durable record IDs this batch would
	// allocate if committed.
	PreviewRecordIDs(ctx context.Context, auth AppendContext, batch AppendBatch) ([]string, error)

	// PublishFrontier publishes a retained owner-prefix frontier through the
	// append path. PublishCut is an alias.
	PublishFrontier(ctx context.Context, auth AppendContext, spec FrontierSpec) (Frontier, error)
	PublishCut(ctx context.Context, auth AppendContext, spec FrontierSpec) (Frontier, error)

	// ResolveFrontier resolves a published frontier into a graph-shaped
	// trace slice. ResolveCut is an alias.
	ResolveFrontier(ctx context.Context, auth ReadContext, frontierID string, modeFilter ModeFilter) (Slice, error)
	ResolveCut(ctx context.Context, auth ReadContext, cutID string, modeFilter ModeFilter) (Slice, error)

	// ReadOwnerCutoff reads a published frontier by ID.
	ReadOwnerCutoff(ctx context.Context, frontierID string) (Frontier, error)

	// Reads. Visibility filtering follows the auth context; modeFilter
	// narrows by record mode.
	ReadFact(ctx context.Context, auth ReadContext, factID string) (VisibleRecord, error)
	ReadOwnerPrefix(ctx context.Context, auth ReadContext, ownerID string, through int, modeFilter ModeFilter) (Slice, error)
	ReadPathPrefix(ctx context.Context, auth ReadContext, pathRef string, through int, modeFilter ModeFilter) (Slice, error)
	ReadCausalClosure(ctx context.Context, auth ReadContext, roots []string, modeFilter ModeFilter, closurePolicy string) (Slice, error)
	FactCount(ctx context.Context) (int, error)
	ContextCount(ctx context.Context) (int, error)

	// Retention. EvictOwnerPath removes an owner path's reachability;
	// ReopenOwnerPath deletes the tombstone; CollectGarbage reclaims
	// unreachable records.
	EvictOwnerPath(ctx context.Context, auth AppendContext, ownerID string, ackDanglingFrontiers bool) (EvictionReceipt, error)
	ReopenOwnerPath(ctx context.Context, auth AppendContext, ownerID string) error
	CollectGarbage(ctx context.Context, auth AppendContext) (GarbageReceipt, error)

	// Close releases the backend's resources. Like database/sql, Close
	// waits for in-flight use to end, so it is safe to call from the
	// consumer's shutdown path.
	Close(ctx context.Context) error
}

// The SQLite store implements the backend contract; the assertion is
// compile-time so a signature drift fails the build, not a caller.
var _ TraceStore = (*SQLiteTraceStore)(nil)
