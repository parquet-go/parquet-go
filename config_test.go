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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			writer := NewGenericWriter[RowType](os.Stdout, tc.cfg)
			if reflect.DeepEqual(tc.cfg, writer.base.config) {
				t.Fatalf("expected %+v, got %+v\n", tc.cfg, writer.base.config)
			}
		})
	}
}

func TestFileConfigConfigureFile(t *testing.T) {
	// Passing a *FileConfig as an option must carry every boolean field.
	src := DefaultFileConfig()
	v := reflect.ValueOf(src).Elem()
	for i := range v.NumField() {
		if f := v.Field(i); f.Kind() == reflect.Bool {
			f.SetBool(true)
		}
	}
	got, err := NewFileConfig(src)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, src) {
		t.Fatalf("want %+v, got %+v", src, got)
	}
}
