package parquet

import (
	"fmt"
	"testing"
)

func TestDecimalInt64PanicMessage(t *testing.T) {
	for _, precision := range []int{0, 19} {
		t.Run(fmt.Sprintf("precision=%d", precision), func(t *testing.T) {
			var p any
			func() {
				defer func() { p = recover() }()
				Decimal(0, precision, Int64Type)
			}()
			want := "DECIMAL annotated with Int64 must have precision >= 1 and <= 18, got " + fmt.Sprintf("%d", precision)
			if p == nil {
				t.Fatal("expected Decimal() to panic for out-of-range Int64 precision")
			}
			if got := fmt.Sprintf("%s", p); got != want {
				t.Errorf("wrong panic message:\n got: %s\nwant: %s", got, want)
			}
		})
	}
}
