package parquet

import (
	"errors"
	"io"
	"testing"
)

type seekErrRowGroup struct{ err error }

func (g *seekErrRowGroup) NumRows() int64                  { return 1 }
func (g *seekErrRowGroup) ColumnChunks() []ColumnChunk     { return nil }
func (g *seekErrRowGroup) Schema() *Schema                 { return nil }
func (g *seekErrRowGroup) SortingColumns() []SortingColumn { return nil }
func (g *seekErrRowGroup) Rows() Rows                      { return seekErrRows{err: g.err} }

type seekErrRows struct{ err error }

func (r seekErrRows) ReadRows([]Row) (int, error) { return 0, nil }
func (r seekErrRows) SeekToRow(int64) error       { return r.err }
func (r seekErrRows) Close() error                { return nil }
func (r seekErrRows) Schema() *Schema             { return nil }

func TestSeekRowGroupPropagatesSeekError(t *testing.T) {
	g := &seekRowGroup{base: &seekErrRowGroup{err: io.ErrUnexpectedEOF}, seek: 3}
	rows := g.Rows()
	if _, err := rows.ReadRows(nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadRows: got %v, want the SeekToRow error", err)
	}
}

type seekErrChunk struct{ emptyColumnChunk }

func (c *seekErrChunk) Pages() Pages { return seekErrPages{} }

type seekErrPages struct{}

func (seekErrPages) ReadPage() (Page, error) { return nil, nil }
func (seekErrPages) SeekToRow(int64) error   { return io.ErrUnexpectedEOF }
func (seekErrPages) Close() error            { return nil }

func TestSeekColumnChunkPropagatesSeekError(t *testing.T) {
	c := &seekColumnChunk{base: &seekErrChunk{}, seek: 4}
	pages := c.Pages()
	if _, err := pages.ReadPage(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadPage: got %v, want the SeekToRow error", err)
	}
}
