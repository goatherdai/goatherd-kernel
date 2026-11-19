package trace

import (
	"os"
	"strings"
	"testing"
)

// The backend leakage gate: production files outside dialect_*.go must not
// speak backend dialect. This is what keeps the seam honest — a new statement
// that reaches for sqlite_master or a PRAGMA fails this test instead of
// silently binding the store to SQLite. Test files are exempt: they may open
// raw databases to build migration fixtures.
func TestNoBackendDialectLeakage(t *testing.T) {
	allowed := map[string]bool{
		"dialect_sqlite.go": true, // the SQLite dialect itself
	}
	// Patterns that indicate backend-specific code or driver wiring.
	patterns := []string{
		"modernc.org/sqlite",
		"sqlite_master",
		"PRAGMA ",
		"INSERT OR ",
		"journal_mode",
		"busy_timeout",
		"sql.Open(",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if allowed[name] {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			for _, pattern := range patterns {
				if strings.Contains(line, pattern) {
					t.Errorf("%s:%d: backend dialect leaked outside the seam (found %q): %s",
						name, i+1, pattern, strings.TrimSpace(line))
				}
			}
		}
	}
}
