package parquet

import (
	"strings"
	"testing"
)

func TestValueLevel(t *testing.T) {
	v := ValueOf(int64(42)).Level(1, 2, 3)
	if got := v.RepetitionLevel(); got != 1 {
		t.Errorf("repetition level: got %d want 1", got)
	}
	if got := v.DefinitionLevel(); got != 2 {
		t.Errorf("definition level: got %d want 2", got)
	}
	if got := v.Column(); got != 3 {
		t.Errorf("column index: got %d want 3", got)
	}
}

func TestValueLevelSetters(t *testing.T) {
	v := ValueOf(int64(42)).Level(1, 2, 3)

	r := v
	if r.SetRepetitionLevel(5); r.RepetitionLevel() != 5 ||
		r.DefinitionLevel() != 2 || r.Column() != 3 {
		t.Errorf("SetRepetitionLevel mutated the wrong fields: %+v", r)
	}
	d := v
	if d.SetDefinitionLevel(6); d.DefinitionLevel() != 6 ||
		d.RepetitionLevel() != 1 || d.Column() != 3 {
		t.Errorf("SetDefinitionLevel mutated the wrong fields: %+v", d)
	}
	c := v
	if c.SetColumnIndex(7); c.Column() != 7 ||
		c.RepetitionLevel() != 1 || c.DefinitionLevel() != 2 {
		t.Errorf("SetColumnIndex mutated the wrong fields: %+v", c)
	}
}

func TestValueLevelOutOfRange(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
		want string
	}{
		{"Level repetition", func() { Value{}.Level(-1, 0, 0) }, "repetition level out of range: -1 not in [0:255]"},
		{"Level definition", func() { Value{}.Level(0, 256, 0) }, "definition level out of range: 256 not in [0:255]"},
		{"Level column", func() { Value{}.Level(0, 0, 65535) }, "column index out of range: 65535 not in [0:65534]"},
		{"SetRepetitionLevel", func() { v := Value{}; v.SetRepetitionLevel(256) }, "repetition level out of range: 256 not in [0:255]"},
		{"SetDefinitionLevel", func() { v := Value{}; v.SetDefinitionLevel(-1) }, "definition level out of range: -1 not in [0:255]"},
		{"SetColumnIndex", func() { v := Value{}; v.SetColumnIndex(-1) }, "column index out of range: -1 not in [0:65534]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected a panic")
				}
				if got := r.(error).Error(); !strings.Contains(got, test.want) {
					t.Errorf("got %q, want %q", got, test.want)
				}
			}()
			test.fn()
		})
	}
}
