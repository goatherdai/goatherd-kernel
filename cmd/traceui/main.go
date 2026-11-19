// Command traceui is a standalone read-only viewer for a shepherd trace
// database: it serves the traceui package's HTTP API over an existing trace
// file, for pointing a browser client at a session after (or during) a run.
//
// Because the effect bus lives in the writer's process, a standalone viewer
// has no bus: /api/events answers 503 and clients should poll /api/records.
// For a live stream, embed the traceui.Service in the writer instead.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/goatherdai/goatherd-kernel"
	"github.com/goatherdai/goatherd-kernel/traceui"
)

func main() {
	backend := flag.String("backend", "sqlite", "trace store backend: sqlite or postgres")
	db := flag.String("db", "", "trace store DSN: file path (sqlite) or connection URL (postgres)")
	addr := flag.String("addr", "127.0.0.1:8321", "listen address")
	flag.Parse()

	if *db == "" {
		fmt.Fprintln(os.Stderr, "traceui: -db is required")
		flag.Usage()
		os.Exit(2)
	}

	// Read-only opener on either backend: no schema creation and a schema
	// check against the existing store — a viewer pointed at a wrong path
	// fails loudly, never modifies a valid trace, and never invents an
	// empty one. On PostgreSQL the read-only open sets
	// default_transaction_read_only=on, so the viewer's credentials can be
	// a role that cannot write.
	store, err := trace.Open(trace.StoreConfig{
		Backend:  trace.Backend(*backend),
		DSN:      *db,
		ReadOnly: true,
	})
	if err != nil {
		log.Fatalf("traceui: open %s: %v", *db, err)
	}
	defer store.Close(context.Background())

	svc, err := traceui.New(traceui.Config{Store: store})
	if err != nil {
		log.Fatalf("traceui: %v", err)
	}

	srv := &http.Server{
		Addr:    *addr,
		Handler: svc.Handler(),
		// The header timeout bounds slow clients without bounding the
		// responses themselves: /api/events streams for as long as the
		// client stays connected, so whole-request read/write timeouts
		// would cut healthy streams.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("traceui: serving %s on http://%s (no bus: poll /api/records)", *db, *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("traceui: serve: %v", err)
	}
}
