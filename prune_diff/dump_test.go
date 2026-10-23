package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestDumpCorpus writes the corpus SQL (PRUNE_DIFF_DUMP=<file>); a tool, not a test.
func TestDumpCorpus(t *testing.T) {
	out := os.Getenv("PRUNE_DIFF_DUMP")
	if out == "" {
		t.Skip("set PRUNE_DIFF_DUMP")
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ex := func(string) string { return "fut = 0 AND seq <= 100" }
	tbl := func(m string) string { return "prune_diff." + m }
	for _, q := range buildCorpus("m1", "m2", time.Now()) {
		if q.kind != kindCountSum {
			continue
		}
		fmt.Fprintf(f, "%s/%s\t%v\t%s\n", q.shape, q.offset, q.prunable, q.from(tbl, ex))
	}
}
