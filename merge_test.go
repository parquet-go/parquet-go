package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const (
	numRowGroups = 3
	rowsPerGroup = benchmarkNumRows
)

type wrappedRowGroup struct {
	parquet.RowGroup
	rowsCallback func(parquet.Rows) parquet.Rows
}

func (r wrappedRowGroup) Rows() parquet.Rows {
	return r.rowsCallback(r.RowGroup.Rows())
}

type wrappedRows struct {
	parquet.Rows
	closed bool
}

func (r *wrappedRows) Close() error {
	r.closed = true
	return r.Rows.Close()
}

func TestMergeRowGroups(t *testing.T) {
	tests := []struct {
		scenario string
		options  []parquet.RowGroupOption
		input    []parquet.RowGroup
		output   parquet.RowGroup
	}{
		{
			scenario: "no row groups",
			options: []parquet.RowGroupOption{
				parquet.SchemaOf(Person{}),
			},
			output: sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SchemaOf(Person{}),
				},
			),
		},

		{
			scenario: "a single row group",
			input: []parquet.RowGroup{
				sortedRowGroup(nil,
					Person{FirstName: "some", LastName: "one", Age: 30},
					Person{FirstName: "some", LastName: "one else", Age: 31},
					Person{FirstName: "and", LastName: "you", Age: 32},
				),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "some", LastName: "one", Age: 30},
				Person{FirstName: "some", LastName: "one else", Age: 31},
				Person{FirstName: "and", LastName: "you", Age: 32},
			),
		},

		{
			scenario: "two row groups without ordering",
			input: []parquet.RowGroup{
				sortedRowGroup(nil, Person{FirstName: "some", LastName: "one", Age: 30}),
				sortedRowGroup(nil, Person{FirstName: "some", LastName: "one else", Age: 31}),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "some", LastName: "one", Age: 30},
				Person{FirstName: "some", LastName: "one else", Age: 31},
			),
		},

		{
			scenario: "three row groups without ordering",
			input: []parquet.RowGroup{
				sortedRowGroup(nil, Person{FirstName: "some", LastName: "one", Age: 30}),
				sortedRowGroup(nil, Person{FirstName: "some", LastName: "one else", Age: 31}),
				sortedRowGroup(nil, Person{FirstName: "question", LastName: "answer", Age: 42}),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "some", LastName: "one", Age: 30},
				Person{FirstName: "some", LastName: "one else", Age: 31},
				Person{FirstName: "question", LastName: "answer", Age: 42},
			),
		},

		{
			scenario: "row groups sorted by ascending last name",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Han", LastName: "Solo"},
					Person{FirstName: "Luke", LastName: "Skywalker"},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Obiwan", LastName: "Kenobi"},
				),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
				Person{FirstName: "Luke", LastName: "Skywalker"},
				Person{FirstName: "Han", LastName: "Solo"},
			),
		},
		{
			scenario: "reproduce issue #66, merging rows with an empty row group",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Han", LastName: "Solo"},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SchemaOf(Person{}),
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
							),
						),
					},
				),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "Han", LastName: "Solo"},
			),
		},
		{
			scenario: "row groups sorted by descending last name",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Descending("LastName"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Descending("LastName"),
							),
						),
					},
					Person{FirstName: "Han", LastName: "Solo"},
					Person{FirstName: "Luke", LastName: "Skywalker"},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Descending("LastName"),
							),
						),
					},
					Person{FirstName: "Obiwan", LastName: "Kenobi"},
				),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "Han", LastName: "Solo"},
				Person{FirstName: "Luke", LastName: "Skywalker"},
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
			),
		},

		{
			scenario: "row groups sorted by ascending last and first name",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
						parquet.Ascending("FirstName"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
								parquet.Ascending("FirstName"),
							),
						),
					},
					Person{FirstName: "Luke", LastName: "Skywalker"},
					Person{FirstName: "Han", LastName: "Solo"},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
								parquet.Ascending("FirstName"),
							),
						),
					},
					Person{FirstName: "Obiwan", LastName: "Kenobi"},
					Person{FirstName: "Anakin", LastName: "Skywalker"},
				),
			},
			output: sortedRowGroup(nil,
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
				Person{FirstName: "Anakin", LastName: "Skywalker"},
				Person{FirstName: "Luke", LastName: "Skywalker"},
				Person{FirstName: "Han", LastName: "Solo"},
			),
		},

		{
			scenario: "row groups with conversion to a different schema",
			options: []parquet.RowGroupOption{
				parquet.SchemaOf(LastNameOnly{}),
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Han", LastName: "Solo"},
					Person{FirstName: "Luke", LastName: "Skywalker"},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Obiwan", LastName: "Kenobi"},
					Person{FirstName: "Anakin", LastName: "Skywalker"},
				),
			},
			output: sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingRowGroupConfig(
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
						),
					),
				},
				LastNameOnly{LastName: "Solo"},
				LastNameOnly{LastName: "Skywalker"},
				LastNameOnly{LastName: "Skywalker"},
				LastNameOnly{LastName: "Kenobi"},
			),
		},
	}

	for _, adapter := range []struct {
		scenario string
		function func(parquet.RowGroup) parquet.RowGroup
	}{
		{scenario: "buffer", function: selfRowGroup},
		{scenario: "file", function: fileRowGroup},
	} {
		t.Run(adapter.scenario, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.scenario, func(t *testing.T) {
					input := make([]parquet.RowGroup, len(test.input))
					for i := range test.input {
						input[i] = adapter.function(test.input[i])
					}

					merged, err := parquet.MergeRowGroups(test.input, test.options...)
					if err != nil {
						t.Fatal(err)
					}
					if merged.NumRows() != test.output.NumRows() {
						t.Fatalf("the number of rows mismatch: want=%d got=%d", merged.NumRows(), test.output.NumRows())
					}
					if merged.Schema() != test.output.Schema() {
						t.Fatalf("the row group schemas mismatch:\n%v\n%v", test.output.Schema(), merged.Schema())
					}

					options := []parquet.RowGroupOption{parquet.SchemaOf(Person{})}
					options = append(options, test.options...)
					// We test two views of the resulting row group: the one originally
					// returned by MergeRowGroups, and one where the merged row group
					// has been copied into a new buffer. The intent is to exercise both
					// the row-by-row read as well as optimized code paths when CopyRows
					// bypasses the ReadRow/WriteRow calls and the row group is written
					// directly to the buffer by calling WriteRowsTo/WriteRowGroup.
					mergedCopy := parquet.NewBuffer(options...)

					totalRows := test.output.NumRows()
					numRows, err := copyRowsAndClose(mergedCopy, merged.Rows())
					if err != nil {
						t.Fatal(err)
					}
					if numRows != totalRows {
						t.Fatalf("wrong number of rows copied: want=%d got=%d", totalRows, numRows)
					}

					for _, merge := range []struct {
						scenario string
						rowGroup parquet.RowGroup
					}{
						{scenario: "self", rowGroup: merged},
						{scenario: "copy", rowGroup: mergedCopy},
					} {
						t.Run(merge.scenario, func(t *testing.T) {
							var expectedRows = test.output.Rows()
							var mergedRows = merge.rowGroup.Rows()
							var row1 = make([]parquet.Row, 1)
							var row2 = make([]parquet.Row, 1)
							var numRows int64

							defer expectedRows.Close()
							defer mergedRows.Close()

							for {
								_, err1 := expectedRows.ReadRows(row1)
								n, err2 := mergedRows.ReadRows(row2)

								if err1 != err2 {
									// ReadRows may or may not return io.EOF
									// when it reads the last row, so we test
									// that the reference RowReader has also
									// reached the end.
									if err1 == nil && err2 == io.EOF {
										_, err1 = expectedRows.ReadRows(row1[:0])
									}
									if err1 != io.EOF {
										t.Fatalf("errors mismatched while comparing row %d/%d: want=%v got=%v", numRows, totalRows, err1, err2)
									}
								}

								if n != 0 {
									if !row1[0].Equal(row2[0]) {
										t.Errorf("row at index %d/%d mismatch: want=%+v got=%+v", numRows, totalRows, row1[0], row2[0])
									}
									numRows++
								}

								if err1 != nil {
									break
								}
							}

							if numRows != totalRows {
								t.Errorf("expected to read %d rows but %d were found", totalRows, numRows)
							}
						})
					}

				})
			}
		})
	}
}

func TestMergeRowGroupsCursorsAreClosed(t *testing.T) {
	type model struct {
		A int
	}

	schema := parquet.SchemaOf(model{})
	options := []parquet.RowGroupOption{
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending(schema.Columns()[0]...),
			),
		),
	}

	prng := rand.New(rand.NewSource(0))
	rowGroups := make([]parquet.RowGroup, numRowGroups)
	rows := make([]*wrappedRows, 0, numRowGroups)

	for i := range rowGroups {
		rowGroups[i] = wrappedRowGroup{
			RowGroup: sortedRowGroup(options, randomRowsOf(prng, rowsPerGroup, model{})...),
			rowsCallback: func(r parquet.Rows) parquet.Rows {
				wrapped := &wrappedRows{Rows: r}
				rows = append(rows, wrapped)
				return wrapped
			},
		}
	}

	m, err := parquet.MergeRowGroups(rowGroups, options...)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		mergedRows := m.Rows()
		defer mergedRows.Close()

		// Add 1 more slot to the buffer to force an io.EOF on the first read.
		rbuf := make([]parquet.Row, (numRowGroups*rowsPerGroup)+1)
		if _, err := mergedRows.ReadRows(rbuf); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}()

	for i, wrapped := range rows {
		if !wrapped.closed {
			t.Fatalf("RowGroup %d not closed", i)
		}
	}
}

func TestMergeRowGroupsSeekToRow(t *testing.T) {
	type model struct {
		A int
	}

	schema := parquet.SchemaOf(model{})
	options := []parquet.RowGroupOption{
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending(schema.Columns()[0]...),
			),
		),
	}

	rowGroups := make([]parquet.RowGroup, numRowGroups)

	counter := 0
	for i := range rowGroups {
		rows := make([]interface{}, 0, rowsPerGroup)
		for j := 0; j < rowsPerGroup; j++ {
			rows = append(rows, model{A: counter})
			counter++
		}
		rowGroups[i] = sortedRowGroup(options, rows...)
	}

	m, err := parquet.MergeRowGroups(rowGroups, options...)
	if err != nil {
		t.Fatal(err)
	}

	func() {
		mergedRows := m.Rows()
		defer mergedRows.Close()

		rbuf := make([]parquet.Row, 1)
		cursor := int64(0)
		for {
			if err := mergedRows.SeekToRow(cursor); err != nil {
				t.Fatal(err)
			}

			if _, err := mergedRows.ReadRows(rbuf); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatal(err)
			}
			v := model{}
			if err := schema.Reconstruct(&v, rbuf[0]); err != nil {
				t.Fatal(err)
			}
			if v.A != int(cursor) {
				t.Fatalf("expected value %d, got %d", cursor, v.A)
			}

			cursor++
		}
	}()
}

func BenchmarkMergeRowGroups(b *testing.B) {
	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			schema := parquet.SchemaOf(test.model)

			options := []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending(schema.Columns()[0]...),
					),
				),
			}

			prng := rand.New(rand.NewSource(0))
			rowGroups := make([]parquet.RowGroup, numRowGroups)

			for i := range rowGroups {
				rowGroups[i] = sortedRowGroup(options, randomRowsOf(prng, rowsPerGroup, test.model)...)
			}

			for n := 1; n <= numRowGroups; n++ {
				b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
					mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n], options...)
					if err != nil {
						b.Fatal(err)
					}

					rows := mergedRowGroup.Rows()
					rbuf := make([]parquet.Row, benchmarkRowsPerStep)
					defer func() { rows.Close() }()

					benchmarkRowsPerSecond(b, func() int {
						n, err := rows.ReadRows(rbuf)
						if err != nil {
							if !errors.Is(err, io.EOF) {
								b.Fatal(err)
							}
							rows.Close()
							rows = mergedRowGroup.Rows()
						}
						return n
					})
				})
			}
		})
	}
}

func BenchmarkMergeFiles(b *testing.B) {
	rowGroupBuffers := make([]bytes.Buffer, numRowGroups)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			schema := parquet.SchemaOf(test.model)

			sortingOptions := []parquet.SortingOption{
				parquet.SortingColumns(
					parquet.Ascending(schema.Columns()[0]...),
				),
			}

			options := []parquet.RowGroupOption{
				schema,
				parquet.SortingRowGroupConfig(
					sortingOptions...,
				),
			}

			buffer := parquet.NewBuffer(options...)

			prng := rand.New(rand.NewSource(0))
			files := make([]*parquet.File, numRowGroups)
			rowGroups := make([]parquet.RowGroup, numRowGroups)

			for i := range files {
				for _, row := range randomRowsOf(prng, rowsPerGroup, test.model) {
					buffer.Write(row)
				}
				sort.Sort(buffer)
				rowGroupBuffers[i].Reset()
				writer := parquet.NewWriter(&rowGroupBuffers[i],
					schema,
					parquet.SortingWriterConfig(
						sortingOptions...,
					),
				)
				_, err := copyRowsAndClose(writer, buffer.Rows())
				if err != nil {
					b.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
				r := bytes.NewReader(rowGroupBuffers[i].Bytes())
				f, err := parquet.OpenFile(r, r.Size())
				if err != nil {
					b.Fatal(err)
				}
				files[i], rowGroups[i] = f, f.RowGroups()[0]
			}

			for n := 1; n <= numRowGroups; n++ {
				b.Run(fmt.Sprintf("groups=%d,rows=%d", n, n*rowsPerGroup), func(b *testing.B) {
					mergedRowGroup, err := parquet.MergeRowGroups(rowGroups[:n], options...)
					if err != nil {
						b.Fatal(err)
					}

					rows := mergedRowGroup.Rows()
					rbuf := make([]parquet.Row, benchmarkRowsPerStep)
					defer func() { rows.Close() }()

					benchmarkRowsPerSecond(b, func() int {
						n, err := rows.ReadRows(rbuf)
						if err != nil {
							if !errors.Is(err, io.EOF) {
								b.Fatal(err)
							}
							rows.Close()
							rows = mergedRowGroup.Rows()
						}
						return n
					})

					totalSize := int64(0)
					for _, f := range files[:n] {
						totalSize += f.Size()
					}
				})
			}
		})
	}
}
