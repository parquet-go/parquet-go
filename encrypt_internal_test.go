package parquet

import (
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

// TestValidateRowGroupOrdinalsAllZeroBackfilled covers the "writer omitted the
// optional Ordinal field" case: every value is zero, so the validator should
// back-fill sequential ordinals and not return an error.
func TestValidateRowGroupOrdinalsAllZeroBackfilled(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 0}, {Ordinal: 0}, {Ordinal: 0}}
	if err := validateRowGroupOrdinals(rgs); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	for i, rg := range rgs {
		if int(rg.Ordinal) != i {
			t.Errorf("rgs[%d].Ordinal = %d, want %d", i, rg.Ordinal, i)
		}
	}
}

// TestValidateRowGroupOrdinalsSequentialAccepted covers explicit, sequential,
// in-order ordinals: the validator must accept them unchanged.
func TestValidateRowGroupOrdinalsSequentialAccepted(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 0}, {Ordinal: 1}, {Ordinal: 2}}
	if err := validateRowGroupOrdinals(rgs); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	for i, rg := range rgs {
		if int(rg.Ordinal) != i {
			t.Errorf("rgs[%d].Ordinal = %d (mutated), want %d", i, rg.Ordinal, i)
		}
	}
}

// TestValidateRowGroupOrdinalsMismatchRejected verifies that files with
// non-sequential row group ordinals (e.g. [0,2]) are rejected.  Downstream
// code — page-index lookup and AAD construction for column encryption —
// assumes rg.Ordinal == slice index, so accepting [0,2] would silently
// produce wrong AADs or out-of-range page-index lookups.  Regression for
// codex finding 5.
func TestValidateRowGroupOrdinalsMismatchRejected(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 0}, {Ordinal: 2}}
	err := validateRowGroupOrdinals(rgs)
	if err == nil {
		t.Fatal("expected validateRowGroupOrdinals([0,2]) to fail, got nil")
	}
	if !strings.Contains(err.Error(), "does not match its position") {
		t.Errorf("expected 'does not match its position' in error, got %q", err.Error())
	}
}

// TestValidateRowGroupOrdinalsOutOfOrderRejected covers a different mismatch:
// every ordinal is unique and within range, but the order does not match the
// slice — also incompatible with the rg.Ordinal == slice-index assumption.
func TestValidateRowGroupOrdinalsOutOfOrderRejected(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 1}, {Ordinal: 0}}
	err := validateRowGroupOrdinals(rgs)
	if err == nil {
		t.Fatal("expected validateRowGroupOrdinals([1,0]) to fail, got nil")
	}
}
