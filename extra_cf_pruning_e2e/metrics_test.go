package main

import (
	"strings"
	"testing"
)

func TestParseMetrics(t *testing.T) {
	input := `
# HELP ignored ignored
tikv_storage_extra_cf_pruning_datasets_total{source="stable",phase="selected"} 3
tikv_storage_extra_cf_pruning_datasets_total{phase="selected",source="l0"} 2
tikv_storage_extra_cf_pruning_datasets_total{phase="filtered",source="l0"} 9
tikv_storage_extra_cf_conflict_check_total{mode="both",strategy="unified",outcome="write_conflict"} 7
tikv_storage_extra_cf_conflict_check_total{mode="legacy",strategy="unified",outcome="write_conflict"} 11
`
	snapshot, err := parseMetrics(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.selected["stable"] != 3 {
		t.Fatalf("stable selected = %v, want 3", snapshot.selected["stable"])
	}
	if snapshot.selected["l0"] != 2 {
		t.Fatalf("l0 selected = %v, want 2", snapshot.selected["l0"])
	}
	if snapshot.bothWriteConflict != 7 {
		t.Fatalf("both write conflicts = %v, want 7", snapshot.bothWriteConflict)
	}
}

func TestParseLabelsWithCommaInQuotedValue(t *testing.T) {
	labels := parseLabels(`a="1,2",b="x\"y",source="memtable"`)
	if labels["a"] != "1,2" {
		t.Fatalf("a = %q, want %q", labels["a"], "1,2")
	}
	if labels["b"] != `x"y` {
		t.Fatalf("b = %q, want quoted value", labels["b"])
	}
	if labels["source"] != "memtable" {
		t.Fatalf("source = %q, want memtable", labels["source"])
	}
}

func TestCounterDeltaHandlesReset(t *testing.T) {
	if got := counterDelta(10, 15); got != 5 {
		t.Fatalf("delta = %v, want 5", got)
	}
	if got := counterDelta(10, 2); got != 2 {
		t.Fatalf("reset delta = %v, want 2", got)
	}
}
