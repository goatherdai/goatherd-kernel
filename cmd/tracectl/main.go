// tracectl is the trace-store operations CLI: one-shot ledger migration
// between storage backends, plus store verification. The library entry
// points live in the root package (MigrateSQLiteToPostgres); this command
// is the operator surface.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/goatherdai/goatherd-kernel"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "migrate":
		migrate(os.Args[2:])
	case "verify":
		verify(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `tracectl — trace store operations

Usage:
  tracectl migrate --from <sqlite path> [--to-backend sqlite|postgres] --to <DSN>
  tracectl verify --backend sqlite|postgres --dsn <DSN>

migrate copies a complete SQLite trace ledger into the target store. The
target must be empty: migration is a move, never a merge. Record ids,
ordinals, commit receipts, and blob bytes are preserved byte-for-byte, so
every reader sees the same ledger on the new backend.

verify opens a store read-only and reports its schema generation, fact and
context counts, and per-owner path summaries.
`)
}

func migrate(args []string) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	from := fs.String("from", "", "source: path to the SQLite trace database (opened read-only)")
	backend := fs.String("to-backend", "postgres", "target backend: sqlite or postgres")
	to := fs.String("to", "", "target DSN: connection URL (postgres) or file path (sqlite)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *from == "" || *to == "" {
		fmt.Fprintln(os.Stderr, "tracectl migrate: --from and --to are required")
		os.Exit(2)
	}

	report, err := trace.MigrateSQLiteToPostgres(*from, trace.StoreConfig{
		Backend: trace.Backend(*backend),
		DSN:     *to,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "tracectl migrate: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintf(w, "table\trows\n")
	for _, t := range orderedTables(report) {
		fmt.Fprintf(w, "%s\t%d\n", t, report.Rows[t])
	}
	w.Flush()
	fmt.Printf("\nfacts: %d  contexts: %d  duration: %s\ntarget: %s\n",
		report.FactCount, report.ContextCount, report.Duration, report.Target)
}

func verify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	backend := fs.String("backend", "sqlite", "backend: sqlite or postgres")
	dsn := fs.String("dsn", "", "store DSN")
	readOnly := fs.Bool("readonly", true, "open read-only (default true; writes are never needed to verify)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "tracectl verify: --dsn is required")
		os.Exit(2)
	}

	sc := trace.StoreConfig{Backend: trace.Backend(*backend), DSN: *dsn, ReadOnly: *readOnly}
	store, err := trace.Open(sc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tracectl verify: %v\n", err)
		os.Exit(1)
	}
	defer store.Close(mustContext())

	facts, ferr := store.FactCount(mustContext())
	ctxs, cerr := store.ContextCount(mustContext())
	if ferr != nil || cerr != nil {
		fmt.Fprintf(os.Stderr, "tracectl verify: counts failed (fact: %v, context: %v)\n", ferr, cerr)
		os.Exit(1)
	}
	fmt.Printf("backend: %s\nschema: current for this kernel\nfacts: %d  contexts: %d\n",
		sc.Describe(), facts, ctxs)
}

// orderedTables returns the report's table names in copy order, so the
// printed report reads the way the migration ran.
func orderedTables(report *trace.MigrationReport) []string {
	order := []string{
		"meta", "append_intents", "owner_ordinals", "contexts", "records",
		"record_edges", "path_entries", "frontiers", "evicted_paths",
		"materialization_intents", "checkpoint_snapshots", "named_blobs",
	}
	var out []string
	for _, t := range order {
		if _, ok := report.Rows[t]; ok {
			out = append(out, t)
		}
	}
	return out
}

func mustContext() context.Context {
	return context.Background()
}
