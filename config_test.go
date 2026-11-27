package parquet

import (
	"os"
	"reflect"
	"testing"
)

func TestConfig(t *testing.T) {
	type RowType struct {
		Id        string
		FirstName string
		LastName  string
	}
	tcs := []struct {
		name string
		cfg  *WriterConfig
	}{
		{
			name: "Check MaxRowsPerRowGroup Coalesces",
			cfg: func() *WriterConfig {
				conf := DefaultWriterConfig()
				conf.MaxRowsPerRowGroup = 10
				return conf
			}(),
		},
		{
			name: "Check DataPageStatistics Coalesces",
			cfg: func() *WriterConfig {
				conf := DefaultWriterConfig()
				conf.DataPageStatistics = true
				return conf
			}(),
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := NewGenericWriter[RowType](os.Stdout, tc.cfg)
			if reflect.DeepEqual(tc.cfg, writer.base.config) {
				t.Fatalf("expected %+v, got %+v\n", tc.cfg, writer.base.config)
			}
		})
	}
}

func TestFileConfigPreservesOptimisticRead(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		cfg, err := NewFileConfig(OptimisticRead(!enabled), &FileConfig{OptimisticRead: enabled})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OptimisticRead != enabled {
			t.Fatalf("OptimisticRead = %v, want %v", cfg.OptimisticRead, enabled)
		}
	}
}
