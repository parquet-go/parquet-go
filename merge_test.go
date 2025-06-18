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

		// Read until EOF
		rbuf := make([]parquet.Row, numRowGroups*rowsPerGroup)
		for {
			_, err := mergedRows.ReadRows(rbuf)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if errors.Is(err, io.EOF) {
				break
			}
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
		rows := make([]any, 0, rowsPerGroup)
		for range rowsPerGroup {
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
						total := 0
						for {
							n, err := rows.ReadRows(rbuf)
							if err != nil {
								if !errors.Is(err, io.EOF) {
									b.Fatal(err)
								}
								rows.Close()
								rows = mergedRowGroup.Rows()
							}
							total += n
							if errors.Is(err, io.EOF) {
								break
							}
						}
						return total
					})
				})
			}
		})
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []parquet.Node
		expected func(result parquet.Node) bool
	}{
		{
			name:  "empty input",
			nodes: []parquet.Node{},
			expected: func(result parquet.Node) bool {
				return result == nil
			},
		},
		{
			name:  "single node",
			nodes: []parquet.Node{parquet.Leaf(parquet.Int32Type)},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Leaf() && result.Type().Kind() == parquet.Int32
			},
		},
		{
			name: "merge two simple leaf nodes",
			nodes: []parquet.Node{
				parquet.Leaf(parquet.Int32Type),
				parquet.Leaf(parquet.Int64Type),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Leaf() && result.Type().Kind() == parquet.Int64
			},
		},
		{
			name: "merge nodes with compression - keep last",
			nodes: []parquet.Node{
				parquet.Compressed(parquet.Leaf(parquet.Int32Type), &parquet.Snappy),
				parquet.Compressed(parquet.Leaf(parquet.Int32Type), &parquet.Gzip),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Compression() == &parquet.Gzip
			},
		},
		{
			name: "merge nodes with encoding - keep last non-plain",
			nodes: []parquet.Node{
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.DeltaBinaryPacked),
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Encoding() == &parquet.RLEDictionary
			},
		},
		{
			name: "merge nodes with field IDs - keep last non-zero",
			nodes: []parquet.Node{
				parquet.FieldID(parquet.Leaf(parquet.Int32Type), 1),
				parquet.FieldID(parquet.Leaf(parquet.Int32Type), 2),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.ID() == 2
			},
		},
		{
			name: "merge repetition types - most permissive (repeated)",
			nodes: []parquet.Node{
				parquet.Required(parquet.Leaf(parquet.Int32Type)),
				parquet.Repeated(parquet.Leaf(parquet.Int32Type)),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Repeated() && !result.Optional() && !result.Required()
			},
		},
		{
			name: "merge repetition types - optional over required",
			nodes: []parquet.Node{
				parquet.Required(parquet.Leaf(parquet.Int32Type)),
				parquet.Optional(parquet.Leaf(parquet.Int32Type)),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Optional() && !result.Repeated() && !result.Required()
			},
		},
		{
			name: "merge complex nodes with all properties",
			nodes: []parquet.Node{
				parquet.FieldID(
					parquet.Compressed(
						parquet.Encoded(
							parquet.Optional(parquet.Leaf(parquet.Int32Type)),
							&parquet.DeltaBinaryPacked,
						),
						&parquet.Snappy,
					),
					1,
				),
				parquet.FieldID(
					parquet.Compressed(
						parquet.Encoded(
							parquet.Repeated(parquet.Leaf(parquet.Int64Type)),
							&parquet.RLEDictionary,
						),
						&parquet.Gzip,
					),
					2,
				),
			},
			expected: func(result parquet.Node) bool {
				return result != nil &&
					result.Type().Kind() == parquet.Int64 &&
					result.Compression() == &parquet.Gzip &&
					result.Encoding() == &parquet.RLEDictionary &&
					result.ID() == 2 &&
					result.Repeated()
			},
		},
		{
			name: "merge group nodes - union of fields",
			nodes: []parquet.Node{
				parquet.Group{
					"field1": parquet.Leaf(parquet.Int32Type),
					"field2": parquet.Leaf(parquet.ByteArrayType),
				},
				parquet.Group{
					"field2": parquet.Leaf(parquet.Int64Type), // Will override
					"field3": parquet.Leaf(parquet.FloatType),
				},
			},
			expected: func(result parquet.Node) bool {
				if result == nil || result.Leaf() {
					return false
				}
				fields := result.Fields()
				if len(fields) != 3 {
					return false
				}

				fieldMap := make(map[string]parquet.Node)
				for _, field := range fields {
					fieldMap[field.Name()] = field
				}

				// Check that all expected fields exist
				field1, has1 := fieldMap["field1"]
				field2, has2 := fieldMap["field2"]
				field3, has3 := fieldMap["field3"]

				return has1 && has2 && has3 &&
					field1.Type().Kind() == parquet.Int32 &&
					field2.Type().Kind() == parquet.Int64 && // Should be overridden
					field3.Type().Kind() == parquet.Float
			},
		},
		{
			name: "merge nested group nodes",
			nodes: []parquet.Node{
				parquet.Group{
					"group1": parquet.Group{
						"nested1": parquet.Leaf(parquet.Int32Type),
					},
				},
				parquet.Group{
					"group1": parquet.Group{
						"nested1": parquet.Leaf(parquet.Int64Type), // Will override
						"nested2": parquet.Leaf(parquet.ByteArrayType),
					},
					"group2": parquet.Group{
						"nested3": parquet.Leaf(parquet.FloatType),
					},
				},
			},
			expected: func(result parquet.Node) bool {
				if result == nil || result.Leaf() {
					return false
				}
				fields := result.Fields()
				if len(fields) != 2 {
					return false
				}

				fieldMap := make(map[string]parquet.Node)
				for _, field := range fields {
					fieldMap[field.Name()] = field
				}

				group1, hasGroup1 := fieldMap["group1"]
				group2, hasGroup2 := fieldMap["group2"]

				if !hasGroup1 || !hasGroup2 || group1.Leaf() || group2.Leaf() {
					return false
				}

				// Check group1 fields
				group1Fields := group1.Fields()
				if len(group1Fields) != 2 {
					return false
				}

				group1FieldMap := make(map[string]parquet.Node)
				for _, field := range group1Fields {
					group1FieldMap[field.Name()] = field
				}

				nested1, hasNested1 := group1FieldMap["nested1"]
				nested2, hasNested2 := group1FieldMap["nested2"]

				return hasNested1 && hasNested2 &&
					nested1.Type().Kind() == parquet.Int64 &&
					nested2.Type().Kind() == parquet.ByteArray
			},
		},
		{
			name: "merge leaf with group - returns last",
			nodes: []parquet.Node{
				parquet.Leaf(parquet.Int32Type),
				parquet.Group{
					"field1": parquet.Leaf(parquet.ByteArrayType),
				},
			},
			expected: func(result parquet.Node) bool {
				return result != nil && !result.Leaf()
			},
		},
		{
			name: "merge with plain encoding - should prefer non-plain",
			nodes: []parquet.Node{
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.Plain),
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.DeltaBinaryPacked),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && result.Encoding() == &parquet.DeltaBinaryPacked
			},
		},
		{
			name: "merge with mixed properties on groups",
			nodes: []parquet.Node{
				parquet.FieldID(
					parquet.Optional(
						parquet.Group{
							"field1": parquet.Leaf(parquet.Int32Type),
						},
					),
					1,
				),
				parquet.FieldID(
					parquet.Repeated(
						parquet.Group{
							"field2": parquet.Leaf(parquet.ByteArrayType),
						},
					),
					2,
				),
			},
			expected: func(result parquet.Node) bool {
				return result != nil && !result.Leaf() &&
					result.ID() == 2 &&
					result.Repeated() &&
					len(result.Fields()) == 2
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parquet.Merge(test.nodes...)
			if !test.expected(result) {
				t.Errorf("Merge result did not match expectations")
				if result != nil {
					t.Logf("Result: %s", result.String())
					if !result.Leaf() {
						t.Logf("Fields: %d", len(result.Fields()))
						for _, field := range result.Fields() {
							t.Logf("  %s: %s", field.Name(), field.String())
						}
					}
				}
			}
		})
	}
}

func BenchmarkMerge(b *testing.B) {
	// Create test nodes for benchmarking
	nodes := []parquet.Node{
		parquet.FieldID(
			parquet.Compressed(
				parquet.Encoded(
					parquet.Optional(parquet.Leaf(parquet.Int32Type)),
					&parquet.DeltaBinaryPacked,
				),
				&parquet.Snappy,
			),
			1,
		),
		parquet.FieldID(
			parquet.Compressed(
				parquet.Encoded(
					parquet.Repeated(parquet.Leaf(parquet.Int64Type)),
					&parquet.RLEDictionary,
				),
				&parquet.Gzip,
			),
			2,
		),
		parquet.Group{
			"field1": parquet.Leaf(parquet.ByteArrayType),
			"field2": parquet.Leaf(parquet.FloatType),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = parquet.Merge(nodes...)
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
