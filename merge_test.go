package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
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
			scenario: "no row groups with schema",
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
			scenario: "two sorted row groups with common sorting",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("FirstName"),
						parquet.Ascending("LastName"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("FirstName"),
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Alice", LastName: "Brown", Age: 25},
					Person{FirstName: "Bob", LastName: "Smith", Age: 30},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("FirstName"),
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Charlie", LastName: "Johnson", Age: 35},
					Person{FirstName: "David", LastName: "Wilson", Age: 40},
				),
			},
			output: sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingRowGroupConfig(
						parquet.SortingColumns(
							parquet.Ascending("FirstName"),
							parquet.Ascending("LastName"),
						),
					),
				},
				Person{FirstName: "Alice", LastName: "Brown", Age: 25},
				Person{FirstName: "Bob", LastName: "Smith", Age: 30},
				Person{FirstName: "Charlie", LastName: "Johnson", Age: 35},
				Person{FirstName: "David", LastName: "Wilson", Age: 40},
			),
		},

		{
			scenario: "two sorted row groups with partial common sorting",
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("FirstName"),
								parquet.Ascending("LastName"),
							),
						),
					},
					Person{FirstName: "Alice", LastName: "Brown", Age: 25},
					Person{FirstName: "Bob", LastName: "Smith", Age: 30},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("FirstName"),
								parquet.Descending("Age"), // Different second column
							),
						),
					},
					Person{FirstName: "Charlie", LastName: "Johnson", Age: 35},
					Person{FirstName: "David", LastName: "Wilson", Age: 40},
				),
			},
			output: sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingRowGroupConfig(
						parquet.SortingColumns(
							parquet.Ascending("FirstName"), // Only FirstName should be preserved
						),
					),
				},
				Person{FirstName: "Alice", LastName: "Brown", Age: 25},
				Person{FirstName: "Bob", LastName: "Smith", Age: 30},
				Person{FirstName: "Charlie", LastName: "Johnson", Age: 35},
				Person{FirstName: "David", LastName: "Wilson", Age: 40},
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

		// Test cases to exercise non-overlapping optimization (currently disabled due to range detection being disabled)
		{
			scenario: "potentially non-overlapping sorted row groups by Age",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("Age"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("Age"),
							),
						),
					},
					Person{FirstName: "Alice", LastName: "Brown", Age: 20},
					Person{FirstName: "Bob", LastName: "Smith", Age: 25},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("Age"),
							),
						),
					},
					Person{FirstName: "Charlie", LastName: "Johnson", Age: 30},
					Person{FirstName: "David", LastName: "Wilson", Age: 35},
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("Age"),
							),
						),
					},
					Person{FirstName: "Eve", LastName: "Davis", Age: 40},
					Person{FirstName: "Frank", LastName: "Miller", Age: 45},
				),
			},
			output: sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingRowGroupConfig(
						parquet.SortingColumns(
							parquet.Ascending("Age"),
						),
					),
				},
				Person{FirstName: "Alice", LastName: "Brown", Age: 20},
				Person{FirstName: "Bob", LastName: "Smith", Age: 25},
				Person{FirstName: "Charlie", LastName: "Johnson", Age: 30},
				Person{FirstName: "David", LastName: "Wilson", Age: 35},
				Person{FirstName: "Eve", LastName: "Davis", Age: 40},
				Person{FirstName: "Frank", LastName: "Miller", Age: 45},
			),
		},

		{
			scenario: "potentially overlapping sorted row groups by Age",
			options: []parquet.RowGroupOption{
				parquet.SortingRowGroupConfig(
					parquet.SortingColumns(
						parquet.Ascending("Age"),
					),
				),
			},
			input: []parquet.RowGroup{
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("Age"),
							),
						),
					},
					Person{FirstName: "Alice", LastName: "Brown", Age: 20},
					Person{FirstName: "Bob", LastName: "Smith", Age: 30}, // Overlaps with next group
				),
				sortedRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingRowGroupConfig(
							parquet.SortingColumns(
								parquet.Ascending("Age"),
							),
						),
					},
					Person{FirstName: "Charlie", LastName: "Johnson", Age: 25}, // Overlaps with previous group
					Person{FirstName: "David", LastName: "Wilson", Age: 35},
				),
			},
			output: sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingRowGroupConfig(
						parquet.SortingColumns(
							parquet.Ascending("Age"),
						),
					),
				},
				Person{FirstName: "Alice", LastName: "Brown", Age: 20},
				Person{FirstName: "Charlie", LastName: "Johnson", Age: 25},
				Person{FirstName: "Bob", LastName: "Smith", Age: 30},
				Person{FirstName: "David", LastName: "Wilson", Age: 35},
			),
		},
	}

	// Additional tests for different field ordering scenarios
	fieldOrderTests := []struct {
		scenario string
		input    []parquet.RowGroup
		output   parquet.RowGroup
	}{
		{
			scenario: "two row groups with same fields in different order",
			input: []parquet.RowGroup{
				createRowGroupWithFieldOrder([]string{"Age", "FirstName", "LastName"},
					Person{Age: 25, FirstName: "John", LastName: "Doe"},
					Person{Age: 30, FirstName: "Jane", LastName: "Smith"},
				),
				createRowGroupWithFieldOrder([]string{"LastName", "Age", "FirstName"},
					Person{Age: 35, FirstName: "Bob", LastName: "Johnson"},
					Person{Age: 40, FirstName: "Alice", LastName: "Brown"},
				),
			},
			output: createExpectedMergedRowGroup(
				Person{Age: 25, FirstName: "John", LastName: "Doe"},
				Person{Age: 30, FirstName: "Jane", LastName: "Smith"},
				Person{Age: 35, FirstName: "Bob", LastName: "Johnson"},
				Person{Age: 40, FirstName: "Alice", LastName: "Brown"},
			),
		},
		{
			scenario: "three row groups with mixed field ordering",
			input: []parquet.RowGroup{
				createRowGroupWithFieldOrder([]string{"FirstName", "LastName", "Age"},
					Person{Age: 20, FirstName: "Charlie", LastName: "Wilson"},
				),
				createRowGroupWithFieldOrder([]string{"Age", "LastName", "FirstName"},
					Person{Age: 25, FirstName: "David", LastName: "Taylor"},
				),
				createRowGroupWithFieldOrder([]string{"LastName", "FirstName", "Age"},
					Person{Age: 30, FirstName: "Eve", LastName: "Anderson"},
				),
			},
			output: createExpectedMergedRowGroup(
				Person{Age: 20, FirstName: "Charlie", LastName: "Wilson"},
				Person{Age: 25, FirstName: "David", LastName: "Taylor"},
				Person{Age: 30, FirstName: "Eve", LastName: "Anderson"},
			),
		},
		{
			scenario: "nested groups with field reordering",
			input: []parquet.RowGroup{
				createNestedRowGroupWithFieldOrder(
					map[string][]string{
						"":         {"user", "metadata"},
						"user":     {"id", "name"},
						"metadata": {"created", "updated"},
					},
					map[string]any{
						"user.id":          int64(1),
						"user.name":        "Alice",
						"metadata.created": "2023-01-01",
						"metadata.updated": "2023-01-02",
					},
				),
				createNestedRowGroupWithFieldOrder(
					map[string][]string{
						"":         {"metadata", "user"},
						"user":     {"name", "id"},
						"metadata": {"updated", "created"},
					},
					map[string]any{
						"user.id":          int64(2),
						"user.name":        "Bob",
						"metadata.created": "2023-02-01",
						"metadata.updated": "2023-02-02",
					},
				),
			},
			output: createNestedRowGroupExpected(),
		},
		{
			scenario: "row groups with different field sets - missing fields become optional",
			input: []parquet.RowGroup{
				createPersonSubsetRowGroup([]string{"Age", "FirstName"}, // Missing LastName
					PersonSubset1{Age: 25, FirstName: "John"},
					PersonSubset1{Age: 30, FirstName: "Jane"},
				),
				createPersonSubsetRowGroup([]string{"FirstName", "LastName"}, // Missing Age
					PersonSubset2{FirstName: "Bob", LastName: "Johnson"},
					PersonSubset2{FirstName: "Alice", LastName: "Brown"},
				),
			},
			output: createExpectedMergedWithOptionalFields(
				Person{Age: 25, FirstName: "John", LastName: ""}, // Missing fields will be zero values
				Person{Age: 30, FirstName: "Jane", LastName: ""},
				Person{Age: 0, FirstName: "Bob", LastName: "Johnson"},
				Person{Age: 0, FirstName: "Alice", LastName: "Brown"},
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
						t.Fatalf("the number of rows mismatch: want=%d got=%d", test.output.NumRows(), merged.NumRows())
					}
					if !parquet.SameNodes(merged.Schema(), test.output.Schema()) {
						t.Fatalf("the row group schemas mismatch:\n%v\n%v", test.output.Schema(), merged.Schema())
					}

					// Validate sorting columns are properly propagated
					expectedSortingColumns := determineMergedSortingColumns(test.input, test.options)
					actualSortingColumns := merged.SortingColumns()
					if !equalSortingColumns(expectedSortingColumns, actualSortingColumns) {
						t.Errorf("sorting columns not properly propagated:\nexpected: %v\nactual: %v",
							expectedSortingColumns, actualSortingColumns)
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
							var (
								expectedRows = test.output.Rows()
								mergedRows   = merge.rowGroup.Rows()
								row1         = make([]parquet.Row, 1)
								row2         = make([]parquet.Row, 1)
								numRows      int64
							)

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
										t.Fatalf("errors mismatched while comparing row %d/%d: want=%v got=%v", totalRows, numRows, err1, err2)
									}
								}

								if n != 0 {
									if !row1[0].Equal(row2[0]) {
										t.Errorf("row at index %d/%d mismatch: want=%+v got=%+v", totalRows, numRows, row1[0], row2[0])
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

	// Test field ordering scenarios
	for _, adapter := range []struct {
		scenario string
		function func(parquet.RowGroup) parquet.RowGroup
	}{
		{scenario: "buffer", function: selfRowGroup},
		{scenario: "file", function: fileRowGroup},
	} {
		t.Run("field-order-"+adapter.scenario, func(t *testing.T) {
			for _, test := range fieldOrderTests {
				t.Run(test.scenario, func(t *testing.T) {
					input := make([]parquet.RowGroup, len(test.input))
					for i := range test.input {
						if test.input[i] != nil {
							input[i] = adapter.function(test.input[i])
						}
					}

					merged, err := parquet.MergeRowGroups(input)
					if err != nil {
						t.Fatal(err)
					}

					if merged.NumRows() != test.output.NumRows() {
						t.Fatalf("the number of rows mismatch: want=%d got=%d", test.output.NumRows(), merged.NumRows())
					}

					// For field ordering tests, we just verify that:
					// 1. The merge succeeded
					// 2. We got the expected number of rows
					// 3. The schema has the same fields (using SameNodes which ignores order)
					// 4. We can read the data correctly

					// For missing fields tests, verify schema structure manually
					if test.scenario == "row groups with different field sets - missing fields become optional" {
						// Verify the merged schema has correct field optionality
						schema := merged.Schema()
						fields := schema.Fields()

						// Check that we have the expected fields
						fieldMap := make(map[string]parquet.Field)
						for _, field := range fields {
							fieldMap[field.Name()] = field
						}

						// Age should be optional (missing from second input)
						if ageField, exists := fieldMap["Age"]; !exists {
							t.Error("Age field missing from merged schema")
						} else if !ageField.Optional() {
							t.Error("Age field should be optional in merged schema")
						}

						// FirstName should be required (present in both inputs)
						if firstNameField, exists := fieldMap["FirstName"]; !exists {
							t.Error("FirstName field missing from merged schema")
						} else if !firstNameField.Required() {
							t.Error("FirstName field should be required in merged schema")
						}

						// LastName should be optional (missing from first input)
						if lastNameField, exists := fieldMap["LastName"]; !exists {
							t.Error("LastName field missing from merged schema")
						} else if !lastNameField.Optional() {
							t.Error("LastName field should be optional in merged schema")
						}
					} else {
						// Verify schema compatibility (field order independent)
						if !parquet.SameNodes(merged.Schema(), test.output.Schema()) {
							t.Fatalf("the row group schemas are not equivalent:\nmerged=%v\nexpected=%v", merged.Schema(), test.output.Schema())
						}
					}

					// Verify we can read all rows without error
					mergedRows := merged.Rows()
					rowBuf := make([]parquet.Row, 1)
					var readRows int64

					defer mergedRows.Close()

					for {
						n, err := mergedRows.ReadRows(rowBuf)

						// Process any data we got first
						if n > 0 {
							readRows++

							// For nested tests, expect more columns
							expectedCols := 3
							if test.scenario == "nested groups with field reordering" {
								expectedCols = 4 // nested structure has more columns
							}

							// Verify the row has the expected number of values
							if len(rowBuf[0]) != expectedCols {
								t.Errorf("row %d has wrong number of values: want=%d got=%d", readRows-1, expectedCols, len(rowBuf[0]))
							}
						}

						// Then check for errors or end conditions
						if err != nil {
							if err == io.EOF {
								break
							}
							t.Fatalf("error reading merged rows at index %d: %v", readRows, err)
						}
						if n == 0 {
							break
						}
					}

					if readRows != test.output.NumRows() {
						t.Errorf("expected to read %d rows but %d were found", test.output.NumRows(), readRows)
					}
				})
			}
		})
	}
}

// determineMergedSortingColumns calculates what the sorting columns should be
// after merging row groups with given options
func determineMergedSortingColumns(input []parquet.RowGroup, options []parquet.RowGroupOption) []parquet.SortingColumn {
	// If explicit sorting is specified in options, use that
	for _, option := range options {
		if config, ok := option.(interface {
			SortingColumns() []parquet.SortingColumn
		}); ok {
			sortingCols := config.SortingColumns()
			if len(sortingCols) > 0 {
				return sortingCols
			}
		}
	}

	// Otherwise, determine the common prefix of sorting columns from input row groups
	if len(input) == 0 {
		return nil
	}

	// Start with the sorting columns of the first row group
	commonSorting := input[0].SortingColumns()

	// Find the common prefix with all other row groups
	for _, rowGroup := range input[1:] {
		rowGroupSorting := rowGroup.SortingColumns()
		commonSorting = commonSortingPrefix(commonSorting, rowGroupSorting)
		if len(commonSorting) == 0 {
			break // No common sorting
		}
	}

	return commonSorting
}

// commonSortingPrefix returns the common prefix of two sorting column slices
func commonSortingPrefix(a, b []parquet.SortingColumn) []parquet.SortingColumn {
	minLen := min(len(b), len(a))

	for i := range minLen {
		if !equalSortingColumn(a[i], b[i]) {
			return a[:i]
		}
	}

	return a[:minLen]
}

// equalSortingColumn compares two sorting columns for equality
func equalSortingColumn(a, b parquet.SortingColumn) bool {
	return slices.Equal(a.Path(), b.Path()) &&
		a.Descending() == b.Descending() &&
		a.NullsFirst() == b.NullsFirst()
}

// equalSortingColumns compares two slices of sorting columns for equality
func equalSortingColumns(a, b []parquet.SortingColumn) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !equalSortingColumn(a[i], b[i]) {
			return false
		}
	}

	return true
}

// TestMergeRowGroupsEmptyWithoutSchema tests that MergeRowGroups returns an error
// when called with an empty slice and no schema option (issue #322)
func TestMergeRowGroupsEmptyWithoutSchema(t *testing.T) {
	_, err := parquet.MergeRowGroups([]parquet.RowGroup{})
	if err == nil {
		t.Fatal("expected error when merging empty row groups without schema, got nil")
	}
	expectedErrMsg := "cannot merge empty row groups without a schema"
	if err.Error() != expectedErrMsg {
		t.Errorf("expected error message %q, got %q", expectedErrMsg, err.Error())
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

func TestMergeNodes(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []parquet.Node
		expected parquet.Node
	}{
		{
			name:     "empty input",
			nodes:    []parquet.Node{},
			expected: nil,
		},
		{
			name:     "single node",
			nodes:    []parquet.Node{parquet.Leaf(parquet.Int32Type)},
			expected: parquet.Required(parquet.Leaf(parquet.Int32Type)),
		},
		{
			name: "merge two simple leaf nodes",
			nodes: []parquet.Node{
				parquet.Leaf(parquet.Int32Type),
				parquet.Leaf(parquet.Int64Type),
			},
			expected: parquet.Required(parquet.Leaf(parquet.Int64Type)),
		},
		{
			name: "merge nodes with compression - keep last",
			nodes: []parquet.Node{
				parquet.Compressed(parquet.Leaf(parquet.Int32Type), &parquet.Snappy),
				parquet.Compressed(parquet.Leaf(parquet.Int32Type), &parquet.Gzip),
			},
			expected: parquet.Required(
				parquet.Compressed(
					parquet.Leaf(parquet.Int32Type),
					&parquet.Gzip,
				),
			),
		},
		{
			name: "merge nodes with encoding - keep last non-plain",
			nodes: []parquet.Node{
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.DeltaBinaryPacked),
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.RLEDictionary),
			},
			expected: parquet.Required(
				parquet.Encoded(
					parquet.Leaf(parquet.Int32Type),
					&parquet.RLEDictionary,
				),
			),
		},
		{
			name: "merge nodes with field IDs - keep last non-zero",
			nodes: []parquet.Node{
				parquet.FieldID(parquet.Leaf(parquet.Int32Type), 1),
				parquet.FieldID(parquet.Leaf(parquet.Int32Type), 2),
			},
			expected: parquet.FieldID(
				parquet.Required(parquet.Leaf(parquet.Int32Type)),
				2,
			),
		},
		{
			name: "merge repetition types - most permissive (repeated)",
			nodes: []parquet.Node{
				parquet.Required(parquet.Leaf(parquet.Int32Type)),
				parquet.Repeated(parquet.Leaf(parquet.Int32Type)),
			},
			expected: parquet.Repeated(parquet.Leaf(parquet.Int32Type)),
		},
		{
			name: "merge repetition types - optional over required",
			nodes: []parquet.Node{
				parquet.Required(parquet.Leaf(parquet.Int32Type)),
				parquet.Optional(parquet.Leaf(parquet.Int32Type)),
			},
			expected: parquet.Optional(parquet.Leaf(parquet.Int32Type)),
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
			expected: parquet.FieldID(
				parquet.Repeated(
					parquet.Compressed(
						parquet.Encoded(
							parquet.Leaf(parquet.Int64Type),
							&parquet.RLEDictionary,
						),
						&parquet.Gzip,
					),
				),
				2,
			),
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
			expected: parquet.Required(parquet.Group{
				"field1": parquet.Optional(parquet.Leaf(parquet.Int32Type)), // Missing from second schema
				"field2": parquet.Required(parquet.Leaf(parquet.Int64Type)), // Present in both, overridden
				"field3": parquet.Optional(parquet.Leaf(parquet.FloatType)), // Missing from first schema
			}),
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
			expected: parquet.Required(parquet.Group{
				"group1": parquet.Required(parquet.Group{
					"nested1": parquet.Required(parquet.Leaf(parquet.Int64Type)),     // Present in both, overridden
					"nested2": parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), // Missing from first schema
				}),
				"group2": parquet.Optional(parquet.Group{ // Missing from first schema
					"nested3": parquet.Required(parquet.Leaf(parquet.FloatType)),
				}),
			}),
		},
		{
			name: "merge leaf with group - returns last",
			nodes: []parquet.Node{
				parquet.Leaf(parquet.Int32Type),
				parquet.Group{
					"field1": parquet.Leaf(parquet.ByteArrayType),
				},
			},
			expected: parquet.Required(parquet.Group{
				"field1": parquet.Required(parquet.Leaf(parquet.ByteArrayType)),
			}),
		},
		{
			name: "merge with plain encoding - should prefer non-plain",
			nodes: []parquet.Node{
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.Plain),
				parquet.Encoded(parquet.Leaf(parquet.Int32Type), &parquet.DeltaBinaryPacked),
			},
			expected: parquet.Required(
				parquet.Encoded(
					parquet.Leaf(parquet.Int32Type),
					&parquet.DeltaBinaryPacked,
				),
			),
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
			expected: parquet.FieldID(
				parquet.Repeated(
					parquet.Group{
						"field1": parquet.Optional(parquet.Leaf(parquet.Int32Type)),     // Missing from second schema
						"field2": parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), // Missing from first schema
					},
				),
				2,
			),
		},
		{
			name: "merge logical map types directly",
			nodes: []parquet.Node{
				parquet.Map(parquet.String(), parquet.Int(32)),
				parquet.Map(parquet.String(), parquet.Int(64)),
			},
			expected: parquet.Map(parquet.String(), parquet.Int(64)),
		},
		{
			name: "merge complex map with field properties",
			nodes: []parquet.Node{
				parquet.FieldID(
					parquet.Map(parquet.String(), parquet.Group{
						"name": parquet.String(),
						"age":  parquet.Int(32),
					}),
					10,
				),
				parquet.FieldID(
					parquet.Optional(
						parquet.Map(parquet.String(), parquet.Group{
							"name":   parquet.String(),
							"age":    parquet.Int(64),                   // Override age type
							"active": parquet.Leaf(parquet.BooleanType), // Add new field
						}),
					),
					20,
				),
			},
			expected: parquet.FieldID(
				parquet.Optional(
					parquet.Map(parquet.String(), parquet.Group{
						"name":   parquet.Required(parquet.String()),                  // Present in both schemas
						"age":    parquet.Required(parquet.Int(64)),                   // Present in both, overridden to Int64
						"active": parquet.Optional(parquet.Leaf(parquet.BooleanType)), // Missing from first schema
					}),
				),
				20,
			),
		},
		{
			name: "preserve JSON logical type from first node when second has none",
			nodes: []parquet.Node{
				parquet.Group{
					"data": parquet.JSON(), // Has JSON logical type
				},
				parquet.Group{
					"data": parquet.Leaf(parquet.ByteArrayType), // Plain binary, no logical type
				},
			},
			expected: parquet.Required(parquet.Group{
				"data": parquet.Required(parquet.JSON()), // JSON logical type preserved
			}),
		},
		{
			name: "preserve JSON logical type from second node",
			nodes: []parquet.Node{
				parquet.Group{
					"data": parquet.Leaf(parquet.ByteArrayType), // Plain binary, no logical type
				},
				parquet.Group{
					"data": parquet.JSON(), // Has JSON logical type
				},
			},
			expected: parquet.Required(parquet.Group{
				"data": parquet.Required(parquet.JSON()), // JSON logical type preserved
			}),
		},
		{
			name: "preserve STRING logical type when merging with plain binary",
			nodes: []parquet.Node{
				parquet.Group{
					"name": parquet.String(), // Has STRING logical type
				},
				parquet.Group{
					"name": parquet.Leaf(parquet.ByteArrayType), // Plain binary
				},
			},
			expected: parquet.Required(parquet.Group{
				"name": parquet.Required(parquet.String()), // STRING preserved
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parquet.MergeNodes(test.nodes...)

			// Handle nil cases
			if test.expected == nil {
				if result != nil {
					t.Errorf("Expected nil result, got: %s", result.String())
				}
				return
			}

			if result == nil {
				t.Errorf("Expected non-nil result, got nil")
				return
			}

			if !parquet.EqualNodes(result, test.expected) {
				t.Errorf("MergeNodes result did not match expected node")
				t.Logf("Expected: %s", test.expected.String())
				t.Logf("Got:      %s", result.String())

				// Additional debugging for group nodes
				if !result.Leaf() {
					t.Logf("Result fields: %d", len(result.Fields()))
					for _, field := range result.Fields() {
						t.Logf("  %s: %s", field.Name(), field.String())
					}
				}
				if !test.expected.Leaf() {
					t.Logf("Expected fields: %d", len(test.expected.Fields()))
					for _, field := range test.expected.Fields() {
						t.Logf("  %s: %s", field.Name(), field.String())
					}
				}
			}
		})
	}
}

// TestMergeRowGroupsWithOverlappingAndMissingFields tests merging row groups where:
// - Each row group has one overlapping field (shared between both)
// - Each row group has one unique field (missing from the other)
func TestMergeRowGroupsWithOverlappingAndMissingFields(t *testing.T) {
	// Define structs for testing overlapping and missing fields
	type UserProfile struct {
		UserID   int        // Overlapping field - present in both schemas
		Username utf8string // Unique to first schema - missing from second
	}

	type UserStats struct {
		UserID     int // Overlapping field - present in both schemas
		LoginCount int // Unique to second schema - missing from first
	}

	type FullUser struct {
		UserID     int
		Username   utf8string
		LoginCount int
	}

	// Create row groups with overlapping and missing fields
	profileBuffer := parquet.NewBuffer()
	profileBuffer.Write(UserProfile{UserID: 1, Username: "alice"})
	profileBuffer.Write(UserProfile{UserID: 2, Username: "bob"})
	profileRowGroup := profileBuffer

	statsBuffer := parquet.NewBuffer()
	statsBuffer.Write(UserStats{UserID: 3, LoginCount: 10})
	statsBuffer.Write(UserStats{UserID: 4, LoginCount: 25})
	statsRowGroup := statsBuffer

	// Merge the row groups
	merged, err := parquet.MergeRowGroups([]parquet.RowGroup{profileRowGroup, statsRowGroup})
	if err != nil {
		t.Fatalf("Failed to merge row groups: %v", err)
	}

	// Validate the merged schema
	schema := merged.Schema()
	fields := schema.Fields()
	fieldMap := make(map[string]parquet.Field)
	for _, field := range fields {
		fieldMap[field.Name()] = field
	}

	// UserID should be required (present in both schemas)
	if userIDField, exists := fieldMap["UserID"]; !exists {
		t.Error("UserID field missing from merged schema")
	} else if !userIDField.Required() {
		t.Error("UserID field should be required in merged schema (present in both)")
	}

	// Username should be optional (missing from second schema)
	if usernameField, exists := fieldMap["Username"]; !exists {
		t.Error("Username field missing from merged schema")
	} else if !usernameField.Optional() {
		t.Error("Username field should be optional in merged schema (missing from UserStats)")
	}

	// LoginCount should be optional (missing from first schema)
	if loginCountField, exists := fieldMap["LoginCount"]; !exists {
		t.Error("LoginCount field missing from merged schema")
	} else if !loginCountField.Optional() {
		t.Error("LoginCount field should be optional in merged schema (missing from UserProfile)")
	}

	// Read and validate the merged data
	rows := merged.Rows()
	defer rows.Close()

	readRows := make([]parquet.Row, 0, 4)
	readBuffer := make([]parquet.Row, 10)
	for {
		n, err := rows.ReadRows(readBuffer)
		for _, row := range readBuffer[:n] {
			readRows = append(readRows, row.Clone())
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read merged rows: %v", err)
		}
	}
	if len(readRows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(readRows))
	}

	// Validate the merged data by examining row structure
	// We expect 4 rows total from merging 2+2 rows

	// Validate that all rows have values for the merged schema's fields
	schemaFields := schema.Fields()
	expectedFieldCount := len(schemaFields)

	for i, row := range readRows {
		// Each row should have values for all fields in the merged schema
		if len(row) != expectedFieldCount {
			t.Errorf("Row %d has %d values, expected %d for merged schema", i, len(row), expectedFieldCount)
		}

		// Log the row structure for debugging
		t.Logf("Row %d: %d columns", i, len(row))
		for j, value := range row {
			if j < len(schemaFields) {
				fieldName := schemaFields[j].Name()
				t.Logf("  %s: %s (column %d)", fieldName, value.String(), j)
			}
		}
	}

	// The key validation is that the merge succeeded and we got the right number of rows
	// The detailed field-level validation is covered by the schema structure tests above

	// Also validate that the merge process correctly handled the overlapping and missing fields:
	// - UserProfile rows (0,1) should have UserID and Username, with LoginCount as zero/default
	// - UserStats rows (2,3) should have UserID and LoginCount, with Username as zero/default
	// The exact value handling depends on how parquet processes missing fields

	t.Logf("Successfully merged %d rows with overlapping and missing fields", len(readRows))
	t.Logf("Schema validation passed: UserID=required, Username=optional, LoginCount=optional")
	t.Logf("Merge behavior: Missing fields are handled through schema conversion during merge")
}

// TestMergeFixedSizeByteArrayMinimalReproduction creates a minimal test case
// to reproduce the FIXED_LEN_BYTE_ARRAY validation issue with different field orders
func TestMergeFixedSizeByteArrayMinimalReproduction(t *testing.T) {
	// Link struct for repeated field testing
	type Link struct {
		SpanID [8]byte `parquet:"span_id"` // Fixed[8] in repeated group
	}

	// First struct with span_id first, parent_span_id second, links third
	type SpanRecord1 struct {
		SpanID       [8]byte  `parquet:"span_id"`        // Required fixed[8]
		ParentSpanID *[8]byte `parquet:"parent_span_id"` // Optional fixed[8]
		Links        []Link   `parquet:"links"`          // Repeated group with fixed[8]
		Name         string   `parquet:"name"`
	}

	// Second struct with different field order: parent_span_id first, span_id second, links third
	type SpanRecord2 struct {
		ParentSpanID *[8]byte `parquet:"parent_span_id"` // Optional fixed[8]
		SpanID       [8]byte  `parquet:"span_id"`        // Required fixed[8]
		Links        []Link   `parquet:"links"`          // Repeated group with fixed[8]
		Name         string   `parquet:"name"`
	}

	// Create test data with repeated links to exercise the third reordering condition
	spanID1 := [8]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	parentSpanID1 := [8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	linkSpanID1 := [8]byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}

	spanID2 := [8]byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}
	linkSpanID2 := [8]byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48}
	// parentSpanID2 is nil (optional field)

	records1 := []SpanRecord1{
		{
			SpanID:       spanID1,
			ParentSpanID: &parentSpanID1,
			Links:        []Link{{SpanID: linkSpanID1}}, // Non-empty links
			Name:         "span1",
		},
	}

	records2 := []SpanRecord2{
		{
			SpanID:       spanID2,
			ParentSpanID: nil,                           // nil parent_span_id
			Links:        []Link{{SpanID: linkSpanID2}}, // Non-empty links
			Name:         "span2",
		},
	}

	// Create first row group
	buffer1 := &bytes.Buffer{}
	writer1 := parquet.NewGenericWriter[SpanRecord1](buffer1)
	_, err := writer1.Write(records1)
	if err != nil {
		t.Fatalf("Failed to write records1: %v", err)
	}
	writer1.Close()

	// Create second row group
	buffer2 := &bytes.Buffer{}
	writer2 := parquet.NewGenericWriter[SpanRecord2](buffer2)
	_, err = writer2.Write(records2)
	if err != nil {
		t.Fatalf("Failed to write records2: %v", err)
	}
	writer2.Close()

	// Read back the row groups
	reader1 := parquet.NewReader(bytes.NewReader(buffer1.Bytes()))
	reader2 := parquet.NewReader(bytes.NewReader(buffer2.Bytes()))

	file1 := reader1.File()
	file2 := reader2.File()

	schema1 := file1.Schema()
	schema2 := file2.Schema()

	fmt.Printf("\nMinimal reproduction test:\n")
	fmt.Printf("Schema 1 columns: %d\n", len(schema1.Columns()))
	fmt.Printf("Schema 2 columns: %d\n", len(schema2.Columns()))

	// Print field order for both schemas
	for i, columnPath := range schema1.Columns() {
		leaf, found := schema1.Lookup(columnPath...)
		if found {
			fmt.Printf("Schema1 col %d: %v, type: %s\n", i, columnPath, leaf.Node.Type())
		}
	}

	for i, columnPath := range schema2.Columns() {
		leaf, found := schema2.Lookup(columnPath...)
		if found {
			fmt.Printf("Schema2 col %d: %v, type: %s\n", i, columnPath, leaf.Node.Type())
		}
	}

	// Create row groups and merge them - this will automatically merge schemas
	rowGroup1 := file1.RowGroups()[0]
	rowGroup2 := file2.RowGroups()[0]

	fmt.Printf("\nAttempting to merge row groups...\n")
	mergedRowGroup, err := parquet.MergeRowGroups([]parquet.RowGroup{rowGroup1, rowGroup2})
	if err != nil {
		t.Fatalf("Failed to merge row groups: %v", err)
	}

	// Get the merged schema from the merged row group
	mergedSchema := mergedRowGroup.Schema()
	fmt.Printf("Merged schema columns: %d\n", len(mergedSchema.Columns()))

	// Print merged field order
	for i, columnPath := range mergedSchema.Columns() {
		leaf, found := mergedSchema.Lookup(columnPath...)
		if found {
			fmt.Printf("Merged col %d: %v, type: %s\n", i, columnPath, leaf.Node.Type())
		}
	}

	// Try to write the merged result
	buffer3 := &bytes.Buffer{}
	writer3 := parquet.NewWriter(buffer3, mergedSchema)

	rows := mergedRowGroup.Rows()
	defer rows.Close()

	// Use CopyRows for efficient copying
	fmt.Printf("\nCopying rows from merged row group to writer...\n")

	// Debug: Let's manually read a few rows to see what's wrong
	rowBuf := make([]parquet.Row, 1)
	for i := range 3 {
		n, err := rows.ReadRows(rowBuf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read debug row %d: %v", i, err)
		}
		if n == 0 {
			break
		}

		row := rowBuf[0]
		fmt.Printf("Debug row %d has %d values:\n", i, len(row))
		for j, val := range row {
			fmt.Printf("  [%d] col=%d, kind=%s, bytes=%d, value=%q\n",
				j, val.Column(), val.Kind(), len(val.ByteArray()), val.String())
			if j >= 10 { // Limit output
				break
			}
		}
	}
	rows.Close()

	// Now try the actual copy
	numCopied, err := parquet.CopyRows(writer3, mergedRowGroup.Rows())
	if err != nil {
		t.Fatalf("Failed to copy rows: %v", err)
	}

	writer3.Close()
	fmt.Printf("Successfully merged %d rows\n", numCopied)
}

// TestMergeFixedSizeByteArrayNullValues tests merging with null fixed-size byte arrays
// This reproduces the specific issue where all parent_span_id and links.span_id are null
func TestMergeFixedSizeByteArrayNullValues(t *testing.T) {
	// Link struct for repeated field testing
	type Link struct {
		SpanID [8]byte `parquet:"span_id"` // Fixed[8] in repeated group - will be null
	}

	// First struct with span_id first, parent_span_id second, links third
	type SpanRecord1 struct {
		SpanID       [8]byte  `parquet:"span_id"`        // Required fixed[8] - NOT null
		ParentSpanID *[8]byte `parquet:"parent_span_id"` // Optional fixed[8] - NULL
		Links        []Link   `parquet:"links"`          // Repeated group with fixed[8] - EMPTY (so span_id is null)
		Name         string   `parquet:"name"`
	}

	// Second struct with different field order
	type SpanRecord2 struct {
		ParentSpanID *[8]byte `parquet:"parent_span_id"` // Optional fixed[8] - NULL
		SpanID       [8]byte  `parquet:"span_id"`        // Required fixed[8] - NOT null
		Links        []Link   `parquet:"links"`          // Repeated group with fixed[8] - EMPTY (so span_id is null)
		Name         string   `parquet:"name"`
	}

	// Create test data with NULL values matching the problematic parquet files
	spanID1 := [8]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	spanID2 := [8]byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}

	records1 := []SpanRecord1{
		{
			SpanID:       spanID1,
			ParentSpanID: nil,      // NULL parent_span_id (like in the problematic files)
			Links:        []Link{}, // EMPTY links (so all link span_ids are null)
			Name:         "span1",
		},
	}

	records2 := []SpanRecord2{
		{
			SpanID:       spanID2,
			ParentSpanID: nil,      // NULL parent_span_id (like in the problematic files)
			Links:        []Link{}, // EMPTY links (so all link span_ids are null)
			Name:         "span2",
		},
	}

	// Create first row group
	buffer1 := &bytes.Buffer{}
	writer1 := parquet.NewGenericWriter[SpanRecord1](buffer1)
	_, err := writer1.Write(records1)
	if err != nil {
		t.Fatalf("Failed to write records1: %v", err)
	}
	writer1.Close()

	// Create second row group
	buffer2 := &bytes.Buffer{}
	writer2 := parquet.NewGenericWriter[SpanRecord2](buffer2)
	_, err = writer2.Write(records2)
	if err != nil {
		t.Fatalf("Failed to write records2: %v", err)
	}
	writer2.Close()

	// Read back the row groups
	reader1 := parquet.NewReader(bytes.NewReader(buffer1.Bytes()))
	reader2 := parquet.NewReader(bytes.NewReader(buffer2.Bytes()))

	file1 := reader1.File()
	file2 := reader2.File()

	schema1 := file1.Schema()
	schema2 := file2.Schema()

	fmt.Printf("\nNull values test:\n")
	fmt.Printf("Schema 1 columns: %d\n", len(schema1.Columns()))
	fmt.Printf("Schema 2 columns: %d\n", len(schema2.Columns()))

	// Print field order for both schemas
	for i, columnPath := range schema1.Columns() {
		leaf, found := schema1.Lookup(columnPath...)
		if found {
			fmt.Printf("Schema1 col %d: %v, type: %s\n", i, columnPath, leaf.Node.Type())
		}
	}

	for i, columnPath := range schema2.Columns() {
		leaf, found := schema2.Lookup(columnPath...)
		if found {
			fmt.Printf("Schema2 col %d: %v, type: %s\n", i, columnPath, leaf.Node.Type())
		}
	}

	// Create row groups and merge them - this should trigger the null value issue
	rowGroup1 := file1.RowGroups()[0]
	rowGroup2 := file2.RowGroups()[0]

	fmt.Printf("\nAttempting to merge row groups with null fixed-size byte arrays...\n")
	mergedRowGroup, err := parquet.MergeRowGroups([]parquet.RowGroup{rowGroup1, rowGroup2})
	if err != nil {
		t.Fatalf("Failed to merge row groups: %v", err)
	}

	// Get the merged schema from the merged row group
	mergedSchema := mergedRowGroup.Schema()
	fmt.Printf("Merged schema columns: %d\n", len(mergedSchema.Columns()))

	// Print merged field order
	for i, columnPath := range mergedSchema.Columns() {
		leaf, found := mergedSchema.Lookup(columnPath...)
		if found {
			fmt.Printf("Merged col %d: %v, type: %s\n", i, columnPath, leaf.Node.Type())
		}
	}

	// Try to copy rows - this should reproduce the index out of range panic
	buffer3 := &bytes.Buffer{}
	writer3 := parquet.NewWriter(buffer3, mergedSchema)

	rows := mergedRowGroup.Rows()
	defer rows.Close()

	// This is where we expect the panic due to null value handling issues
	fmt.Printf("\nCopying rows from merged row group to writer...\n")
	numCopied, err := parquet.CopyRows(writer3, rows)
	if err != nil {
		t.Fatalf("Failed to copy rows: %v", err)
	}

	writer3.Close()
	fmt.Printf("Successfully merged %d rows\n", numCopied)
}

func BenchmarkMergeNodes(b *testing.B) {
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

	for b.Loop() {
		_ = parquet.MergeNodes(nodes...)
	}
}

// Helper functions for field ordering tests

// Define struct types with same fields but different declaration order
type PersonAgeFirst struct {
	Age       int
	FirstName utf8string
	LastName  utf8string
}

type PersonLastNameFirst struct {
	LastName  utf8string
	Age       int
	FirstName utf8string
}

type PersonFirstNameFirst struct {
	FirstName utf8string
	LastName  utf8string
	Age       int
}

// Struct types for testing missing fields
type PersonSubset1 struct {
	Age       int
	FirstName utf8string
	// Missing LastName
}

type PersonSubset2 struct {
	FirstName utf8string
	LastName  utf8string
	// Missing Age
}

// createRowGroupWithFieldOrder creates a row group using different struct orders
func createRowGroupWithFieldOrder(fieldOrder []string, rows ...Person) parquet.RowGroup {
	switch {
	case len(fieldOrder) >= 1 && fieldOrder[0] == "Age":
		// Age first
		buf := parquet.NewBuffer()
		for _, row := range rows {
			buf.Write(PersonAgeFirst{
				Age:       row.Age,
				FirstName: row.FirstName,
				LastName:  row.LastName,
			})
		}
		return buf
	case len(fieldOrder) >= 1 && fieldOrder[0] == "LastName":
		// LastName first
		buf := parquet.NewBuffer()
		for _, row := range rows {
			buf.Write(PersonLastNameFirst{
				LastName:  row.LastName,
				Age:       row.Age,
				FirstName: row.FirstName,
			})
		}
		return buf
	case len(fieldOrder) >= 1 && fieldOrder[0] == "FirstName":
		// FirstName first
		buf := parquet.NewBuffer()
		for _, row := range rows {
			buf.Write(PersonFirstNameFirst{
				FirstName: row.FirstName,
				LastName:  row.LastName,
				Age:       row.Age,
			})
		}
		return buf
	default:
		// Default order
		buf := parquet.NewBuffer()
		for _, row := range rows {
			buf.Write(row)
		}
		return buf
	}
}

// createPersonSubsetRowGroup creates a row group with only a subset of Person fields
func createPersonSubsetRowGroup(fields []string, rows ...any) parquet.RowGroup {
	buf := parquet.NewBuffer()
	for _, row := range rows {
		buf.Write(row)
	}
	return buf
}

// Define nested struct types with different field orders
type UserType1 struct {
	ID   int64
	Name utf8string
}

type UserType2 struct {
	Name utf8string
	ID   int64
}

type MetadataType1 struct {
	Created utf8string
	Updated utf8string
}

type MetadataType2 struct {
	Updated utf8string
	Created utf8string
}

type NestedType1 struct {
	User     UserType1
	Metadata MetadataType1
}

type NestedType2 struct {
	Metadata MetadataType2
	User     UserType2
}

// createNestedRowGroupWithFieldOrder creates a nested row group using different struct orders
func createNestedRowGroupWithFieldOrder(fieldOrders map[string][]string, values map[string]any) parquet.RowGroup {
	userID := values["user.id"].(int64)
	userName := utf8string(values["user.name"].(string))
	created := utf8string(values["metadata.created"].(string))
	updated := utf8string(values["metadata.updated"].(string))

	// Check root field order to determine which struct type to use
	if rootOrder, exists := fieldOrders[""]; exists && len(rootOrder) > 0 && rootOrder[0] == "user" {
		// User first
		buf := parquet.NewBuffer()
		buf.Write(NestedType1{
			User: UserType1{
				ID:   userID,
				Name: userName,
			},
			Metadata: MetadataType1{
				Created: created,
				Updated: updated,
			},
		})
		return buf
	} else {
		// Metadata first
		buf := parquet.NewBuffer()
		buf.Write(NestedType2{
			Metadata: MetadataType2{
				Updated: updated,
				Created: created,
			},
			User: UserType2{
				Name: userName,
				ID:   userID,
			},
		})
		return buf
	}
}

// createNestedRowGroupExpected creates the expected result for nested field ordering test
func createNestedRowGroupExpected() parquet.RowGroup {
	// Use one of the struct types as the expected format - after merge,
	// the schemas should be equivalent regardless of original field order
	buf := parquet.NewBuffer()

	// Add expected rows using consistent struct type
	buf.Write(NestedType1{
		User: UserType1{
			ID:   int64(1),
			Name: utf8string("Alice"),
		},
		Metadata: MetadataType1{
			Created: utf8string("2023-01-01"),
			Updated: utf8string("2023-01-02"),
		},
	})
	buf.Write(NestedType1{
		User: UserType1{
			ID:   int64(2),
			Name: utf8string("Bob"),
		},
		Metadata: MetadataType1{
			Created: utf8string("2023-02-01"),
			Updated: utf8string("2023-02-02"),
		},
	})

	return buf
}

// createExpectedMergedRowGroup creates a simple expected result for comparison
// For field ordering tests, we just need to verify the merge works and produces the right number of rows
// The actual field order will be handled by SameNodes comparison
func createExpectedMergedRowGroup(rows ...Person) parquet.RowGroup {
	// Just create a simple row group with the same data
	// The test will use SameNodes for schema comparison which handles field order differences
	buf := parquet.NewBuffer()
	for _, row := range rows {
		buf.Write(row)
	}
	return buf
}

// createExpectedMergedWithOptionalFields creates expected result with optional fields
// For this test case, we just create a simple reference that will be compared using SameNodes
// The actual validation is that the merge succeeds and uses the nullable() function correctly
func createExpectedMergedWithOptionalFields(rows ...Person) parquet.RowGroup {
	// Just create a simple row group - the test will validate the actual schema using SameNodes
	// and check that the merge operation succeeds with the correct optional field handling
	buf := parquet.NewBuffer()
	for _, row := range rows {
		buf.Write(row)
	}
	return buf
}

// TestMergeRowGroupsRetainsDictionaryEncoding tests that dictionary encoding
// is preserved when merging row groups
func TestMergeRowGroupsRetainsDictionaryEncoding(t *testing.T) {
	// Define a schema with dictionary-encoded columns
	type Record struct {
		ID   int64
		Name utf8string
		City utf8string
	}

	// Create a schema with dictionary encoding on string columns
	nameNode := parquet.String()
	nameNode = parquet.Encoded(nameNode, &parquet.RLEDictionary)

	cityNode := parquet.String()
	cityNode = parquet.Encoded(cityNode, &parquet.RLEDictionary)

	schema := parquet.NewSchema("test", parquet.Group{
		"ID":   parquet.Leaf(parquet.Int64Type),
		"Name": nameNode,
		"City": cityNode,
	})

	// Create first row group with dictionary encoding
	buffer1 := &bytes.Buffer{}
	writer1 := parquet.NewGenericWriter[Record](buffer1, schema)
	records1 := []Record{
		{ID: 1, Name: "Alice", City: "NYC"},
		{ID: 2, Name: "Bob", City: "LA"},
		{ID: 3, Name: "Alice", City: "NYC"}, // Repeated values for dictionary
	}
	if _, err := writer1.Write(records1); err != nil {
		t.Fatalf("Failed to write records to first file: %v", err)
	}
	if err := writer1.Close(); err != nil {
		t.Fatalf("Failed to close first writer: %v", err)
	}

	// Create second row group with dictionary encoding
	buffer2 := &bytes.Buffer{}
	writer2 := parquet.NewGenericWriter[Record](buffer2, schema)
	records2 := []Record{
		{ID: 4, Name: "Charlie", City: "SF"},
		{ID: 5, Name: "Diana", City: "NYC"},
		{ID: 6, Name: "Charlie", City: "SF"}, // Repeated values for dictionary
	}
	if _, err := writer2.Write(records2); err != nil {
		t.Fatalf("Failed to write records to second file: %v", err)
	}
	if err := writer2.Close(); err != nil {
		t.Fatalf("Failed to close second writer: %v", err)
	}

	// Read back the parquet files
	reader1 := parquet.NewReader(bytes.NewReader(buffer1.Bytes()))
	reader2 := parquet.NewReader(bytes.NewReader(buffer2.Bytes()))

	file1 := reader1.File()
	file2 := reader2.File()

	rowGroup1 := file1.RowGroups()[0]
	rowGroup2 := file2.RowGroups()[0]

	// Verify original files have dictionary encoding
	t.Log("Original file 1 schema:")
	for i, col := range file1.Schema().Columns() {
		leaf, _ := file1.Schema().Lookup(col...)
		t.Logf("  Column %d (%v): encoding=%v", i, col, leaf.Node.Encoding())
	}

	t.Log("Original file 2 schema:")
	for i, col := range file2.Schema().Columns() {
		leaf, _ := file2.Schema().Lookup(col...)
		t.Logf("  Column %d (%v): encoding=%v", i, col, leaf.Node.Encoding())
	}

	// Merge the row groups
	mergedRowGroup, err := parquet.MergeRowGroups([]parquet.RowGroup{rowGroup1, rowGroup2})
	if err != nil {
		t.Fatalf("Failed to merge row groups: %v", err)
	}

	// Check merged schema
	mergedSchema := mergedRowGroup.Schema()
	t.Log("Merged schema:")
	for i, col := range mergedSchema.Columns() {
		leaf, found := mergedSchema.Lookup(col...)
		if !found {
			t.Fatalf("Column not found in merged schema: %v", col)
		}
		t.Logf("  Column %d (%v): encoding=%v", i, col, leaf.Node.Encoding())

		// Verify dictionary encoding is preserved for Name and City columns
		colName := col[len(col)-1]
		if colName == "Name" || colName == "City" {
			encoding := leaf.Node.Encoding()
			if encoding == nil || encoding.Encoding() != parquet.RLEDictionary.Encoding() {
				t.Errorf("Column %s should have RLEDictionary encoding, got: %v", colName, encoding)
			}
		}
	}

	// Write the merged result to a new file to ensure encoding is actually used
	buffer3 := &bytes.Buffer{}
	writer3 := parquet.NewWriter(buffer3, mergedSchema)

	rows := mergedRowGroup.Rows()
	defer rows.Close()

	numCopied, err := parquet.CopyRows(writer3, rows)
	if err != nil {
		t.Fatalf("Failed to copy merged rows: %v", err)
	}
	if numCopied != int64(len(records1)+len(records2)) {
		t.Errorf("Expected to copy %d rows, but copied %d", len(records1)+len(records2), numCopied)
	}

	if err := writer3.Close(); err != nil {
		t.Fatalf("Failed to close merged writer: %v", err)
	}

	// Read back the merged file and verify encoding
	reader3 := parquet.NewReader(bytes.NewReader(buffer3.Bytes()))
	file3 := reader3.File()

	t.Log("Final merged file schema:")
	for i, col := range file3.Schema().Columns() {
		leaf, _ := file3.Schema().Lookup(col...)
		t.Logf("  Column %d (%v): encoding=%v", i, col, leaf.Node.Encoding())
	}

	// Verify that the actual column chunks use dictionary encoding
	finalRowGroup := file3.RowGroups()[0]
	columnChunks := finalRowGroup.ColumnChunks()

	for i, col := range file3.Schema().Columns() {
		colName := col[len(col)-1]
		if colName == "Name" || colName == "City" {
			chunk := columnChunks[i]
			// Check if the column chunk has dictionary
			pages := chunk.Pages()
			defer pages.Close()

			hasDictionary := false
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Error reading page: %v", err)
				}
				// Check if page has a dictionary
				dict := page.Dictionary()
				parquet.Release(page)
				if dict != nil {
					hasDictionary = true
					t.Logf("Column %s has dictionary with %d entries", colName, dict.Len())
					break
				}
			}

			if !hasDictionary {
				t.Errorf("Column %s does not use dictionary encoding in actual pages", colName)
			}
		}
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

func TestMergeSortingColumns(t *testing.T) {
	tests := []struct {
		name     string
		input    [][]parquet.SortingColumn
		expected []parquet.SortingColumn
	}{
		{
			name:     "empty input",
			input:    [][]parquet.SortingColumn{},
			expected: nil,
		},
		{
			name:     "single empty slice",
			input:    [][]parquet.SortingColumn{{}},
			expected: nil,
		},
		{
			name:     "one slice with empty slice",
			input:    [][]parquet.SortingColumn{{parquet.Ascending("A")}, {}},
			expected: nil,
		},
		{
			name: "identical single column",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A")},
				{parquet.Ascending("A")},
			},
			expected: []parquet.SortingColumn{parquet.Ascending("A")},
		},
		{
			name: "identical multi-column",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A"), parquet.Descending("B")},
				{parquet.Ascending("A"), parquet.Descending("B")},
			},
			expected: []parquet.SortingColumn{parquet.Ascending("A"), parquet.Descending("B")},
		},
		{
			name: "common prefix - partial match",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A"), parquet.Ascending("B"), parquet.Descending("C")},
				{parquet.Ascending("A"), parquet.Ascending("B"), parquet.Ascending("D")},
			},
			expected: []parquet.SortingColumn{parquet.Ascending("A"), parquet.Ascending("B")},
		},
		{
			name: "no common prefix - different first column",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A"), parquet.Ascending("B")},
				{parquet.Ascending("X"), parquet.Ascending("B")},
			},
			expected: []parquet.SortingColumn{},
		},
		{
			name: "different direction on same column",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A"), parquet.Ascending("B")},
				{parquet.Descending("A"), parquet.Ascending("B")},
			},
			expected: []parquet.SortingColumn{},
		},
		{
			name: "different nulls first setting",
			input: [][]parquet.SortingColumn{
				{parquet.NullsFirst(parquet.Ascending("A"))},
				{parquet.Ascending("A")}, // defaults to nulls last
			},
			expected: []parquet.SortingColumn{},
		},
		{
			name: "three inputs with common prefix",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A"), parquet.Ascending("B"), parquet.Descending("C")},
				{parquet.Ascending("A"), parquet.Ascending("B"), parquet.Ascending("D")},
				{parquet.Ascending("A"), parquet.Ascending("B"), parquet.Descending("E")},
			},
			expected: []parquet.SortingColumn{parquet.Ascending("A"), parquet.Ascending("B")},
		},
		{
			name: "unequal length inputs - shorter wins",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("A"), parquet.Ascending("B")},
				{parquet.Ascending("A")},
				{parquet.Ascending("A"), parquet.Ascending("B"), parquet.Descending("C")},
			},
			expected: []parquet.SortingColumn{parquet.Ascending("A")},
		},
		{
			name: "complex paths",
			input: [][]parquet.SortingColumn{
				{parquet.Ascending("nested.field.a")},
				{parquet.Ascending("nested.field.a")},
			},
			expected: []parquet.SortingColumn{parquet.Ascending("nested.field.a")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parquet.MergeSortingColumns(test.input...)

			if !equalSortingColumnsSlices(result, test.expected) {
				t.Errorf("MergeSortingColumns() = %v, expected %v", result, test.expected)
				t.Logf("Result length: %d, Expected length: %d", len(result), len(test.expected))
				for i, col := range result {
					t.Logf("  Result[%d]: Path=%v, Desc=%v, NullsFirst=%v", i, col.Path(), col.Descending(), col.NullsFirst())
				}
				for i, col := range test.expected {
					t.Logf("  Expected[%d]: Path=%v, Desc=%v, NullsFirst=%v", i, col.Path(), col.Descending(), col.NullsFirst())
				}
			}
		})
	}
}

// Helper function to compare slices of sorting columns
func equalSortingColumnsSlices(a, b []parquet.SortingColumn) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !equalSortingColumnsTest(a[i], b[i]) {
			return false
		}
	}

	return true
}

// Helper function to compare two sorting columns
func equalSortingColumnsTest(a, b parquet.SortingColumn) bool {
	aPath := a.Path()
	bPath := b.Path()

	if len(aPath) != len(bPath) {
		return false
	}

	for i, pathElement := range aPath {
		if pathElement != bPath[i] {
			return false
		}
	}

	return a.Descending() == b.Descending() && a.NullsFirst() == b.NullsFirst()
}

// TestMergeRowGroupsJSONAndByteArrayConversion tests that merging row groups
// where one has JSON logical type and one has plain BYTE_ARRAY works correctly.
// This is a regression test for a bug where jsonType.ConvertValue() used
// *byteArrayType (pointer) in type assertion, but byteArrayType is a value type,
// causing conversions from plain BYTE_ARRAY to JSON to always fail.
func TestMergeRowGroupsJSONAndByteArrayConversion(t *testing.T) {
	// Struct with plain []byte (becomes BYTE_ARRAY without JSON logical type)
	type PlainRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"` // Plain BYTE_ARRAY
	}

	// Struct with []byte tagged as JSON (becomes BYTE_ARRAY with JSON logical type)
	type JSONRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data,json"` // JSON logical type
	}

	// Create first parquet file with plain BYTE_ARRAY schema
	plainBuf := &bytes.Buffer{}
	plainWriter := parquet.NewGenericWriter[PlainRecord](plainBuf)
	_, err := plainWriter.Write([]PlainRecord{
		{ID: 1, Data: []byte(`{"key":"value1"}`)},
		{ID: 2, Data: []byte(`{"key":"value2"}`)},
	})
	if err != nil {
		t.Fatalf("Failed to write plain records: %v", err)
	}
	plainWriter.Close()

	// Create second parquet file with JSON logical type
	jsonBuf := &bytes.Buffer{}
	jsonWriter := parquet.NewGenericWriter[JSONRecord](jsonBuf)
	_, err = jsonWriter.Write([]JSONRecord{
		{ID: 3, Data: []byte(`{"key":"value3"}`)},
		{ID: 4, Data: []byte(`{"key":"value4"}`)},
	})
	if err != nil {
		t.Fatalf("Failed to write JSON records: %v", err)
	}
	jsonWriter.Close()

	// Read back the files to get row groups
	plainReader := parquet.NewReader(bytes.NewReader(plainBuf.Bytes()))
	jsonReader := parquet.NewReader(bytes.NewReader(jsonBuf.Bytes()))

	plainFile := plainReader.File()
	jsonFile := jsonReader.File()

	// Verify schemas are different
	plainSchema := plainFile.Schema()
	jsonSchema := jsonFile.Schema()

	plainDataField, ok := plainSchema.Lookup("data")
	if !ok {
		t.Fatal("'data' field not found in plain schema")
	}
	jsonDataField, ok := jsonSchema.Lookup("data")
	if !ok {
		t.Fatal("'data' field not found in JSON schema")
	}

	// Plain schema should NOT have JSON logical type
	if plainDataField.Node.Type().LogicalType() != nil && plainDataField.Node.Type().LogicalType().Json != nil {
		t.Error("Plain schema should not have JSON logical type")
	}

	// JSON schema SHOULD have JSON logical type
	if jsonDataField.Node.Type().LogicalType() == nil || jsonDataField.Node.Type().LogicalType().Json == nil {
		t.Error("JSON schema should have JSON logical type")
	}

	// Test merging plain BYTE_ARRAY into JSON (the bug case)
	t.Run("plain BYTE_ARRAY to JSON", func(t *testing.T) {
		plainRowGroup := plainFile.RowGroups()[0]
		jsonRowGroup := jsonFile.RowGroups()[0]

		// This was the failing case before the fix - merging plain BYTE_ARRAY values
		// into a schema that expects JSON logical type
		merged, err := parquet.MergeRowGroups([]parquet.RowGroup{plainRowGroup, jsonRowGroup})
		if err != nil {
			t.Fatalf("MergeRowGroups failed: %v", err)
		}

		// Now write the merged row group to a new file and read it back
		// This exercises the conversion path that was broken
		outputBuf := &bytes.Buffer{}
		writer := parquet.NewWriter(outputBuf, merged.Schema())
		rows := merged.Rows()
		_, err = parquet.CopyRows(writer, rows)
		rows.Close()
		if err != nil {
			t.Fatalf("CopyRows failed (this is where the conversion bug manifests): %v", err)
		}
		writer.Close()

		// Re-read from the written file
		reader := parquet.NewReader(bytes.NewReader(outputBuf.Bytes()))
		mergedFile := reader.File()
		merged, err = parquet.MergeRowGroups(mergedFile.RowGroups())
		if err != nil {
			t.Fatalf("MergeRowGroups failed: %v", err)
		}

		// Verify the merged schema has JSON logical type (preferred over plain)
		mergedSchema := merged.Schema()
		mergedDataField, ok := mergedSchema.Lookup("data")
		if !ok {
			t.Fatal("'data' field not found in merged schema")
		}
		if mergedDataField.Node.Type().LogicalType() == nil || mergedDataField.Node.Type().LogicalType().Json == nil {
			t.Error("Expected merged schema to have JSON logical type for 'data' field")
		}

		// Read all rows to verify data conversion worked
		finalRows := merged.Rows()
		defer finalRows.Close()

		readRows := make([]parquet.Row, 0, 4)
		readBuffer := make([]parquet.Row, 10)
		for {
			n, err := finalRows.ReadRows(readBuffer)
			for _, row := range readBuffer[:n] {
				readRows = append(readRows, row.Clone())
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Failed to read merged rows: %v", err)
			}
		}

		if len(readRows) != 4 {
			t.Fatalf("Expected 4 rows, got %d", len(readRows))
		}

		// Verify all data values are present and readable
		// Column order in merged schema: data (0), id (1) - alphabetical
		for i, row := range readRows {
			if len(row) != 2 {
				t.Errorf("Row %d: expected 2 columns, got %d", i, len(row))
				continue
			}
			// The 'data' column is index 0 (alphabetical ordering)
			dataVal := row[0]
			if dataVal.IsNull() {
				t.Errorf("Row %d: data value is null", i)
				continue
			}
			if dataVal.Kind() != parquet.ByteArray {
				t.Errorf("Row %d: expected ByteArray kind, got %v", i, dataVal.Kind())
				continue
			}
			data := string(dataVal.ByteArray())
			if len(data) == 0 {
				t.Errorf("Row %d: data is empty", i)
			}
			// Just verify it's valid JSON-like content
			if data[0] != '{' {
				t.Errorf("Row %d: expected JSON object, got %q", i, data)
			}
		}
	})

	// Test merging JSON into plain BYTE_ARRAY (reverse order)
	t.Run("JSON to plain BYTE_ARRAY", func(t *testing.T) {
		plainRowGroup := plainFile.RowGroups()[0]
		jsonRowGroup := jsonFile.RowGroups()[0]

		// Merge with JSON first - should still prefer JSON logical type
		merged, err := parquet.MergeRowGroups([]parquet.RowGroup{jsonRowGroup, plainRowGroup})
		if err != nil {
			t.Fatalf("MergeRowGroups failed: %v", err)
		}

		// Read all rows
		rows := merged.Rows()
		defer rows.Close()

		readRows := make([]parquet.Row, 0, 4)
		readBuffer := make([]parquet.Row, 10)
		for {
			n, err := rows.ReadRows(readBuffer)
			for _, row := range readBuffer[:n] {
				readRows = append(readRows, row.Clone())
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Failed to read merged rows: %v", err)
			}
		}

		if len(readRows) != 4 {
			t.Fatalf("Expected 4 rows, got %d", len(readRows))
		}

		// Verify all data is readable (order may differ due to merge order)
		// Column order in merged schema: data (0), id (1) - alphabetical
		for i, row := range readRows {
			if len(row) != 2 {
				t.Errorf("Row %d: expected 2 columns, got %d", i, len(row))
				continue
			}
			// The 'data' column is index 0 (alphabetical ordering)
			dataVal := row[0]
			if dataVal.IsNull() {
				t.Errorf("Row %d: data value is null", i)
				continue
			}
			if dataVal.Kind() != parquet.ByteArray {
				t.Errorf("Row %d: expected ByteArray kind, got %v", i, dataVal.Kind())
				continue
			}
			data := string(dataVal.ByteArray())
			if len(data) == 0 {
				t.Errorf("Row %d: data is empty", i)
			}
		}
	})
}

// TestMergeRowGroupsWithMissingColumnsMultiPage tests merging row groups with different
// schemas where some columns are missing, using enough data to span multiple pages.
// This exercises two key fixes:
// 1. Nested multiColumnChunk flattening (multi_row_group.go)
// 2. missingPageValues multi-page reading (convert.go)
func TestMergeRowGroupsWithMissingColumnsMultiPage(t *testing.T) {
	// Schema A: has columns ID, X, Y
	type SchemaA struct {
		ID int64  `parquet:"id"`
		X  string `parquet:"x"`
		Y  int32  `parquet:"y"`
	}

	// Schema B: has columns ID, X, Z (missing Y, has Z instead)
	type SchemaB struct {
		ID int64  `parquet:"id"`
		X  string `parquet:"x"`
		Z  int32  `parquet:"z"`
	}

	// Use enough rows to span multiple pages
	const rowsPerFile = 1000

	// Helper to create a parquet file with SchemaA
	createFileA := func(startID int64) *parquet.File {
		buf := &bytes.Buffer{}
		writer := parquet.NewGenericWriter[SchemaA](buf, parquet.PageBufferSize(256)) // Small pages
		records := make([]SchemaA, rowsPerFile)
		for i := range records {
			records[i] = SchemaA{
				ID: startID + int64(i),
				X:  fmt.Sprintf("a-value-%d", startID+int64(i)),
				Y:  int32(startID) + int32(i)*10 + 1, // +1 to ensure non-zero
			}
		}
		if _, err := writer.Write(records); err != nil {
			t.Fatalf("Failed to write SchemaA records: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close SchemaA writer: %v", err)
		}

		file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("Failed to open SchemaA file: %v", err)
		}
		return file
	}

	// Helper to create a parquet file with SchemaB
	createFileB := func(startID int64) *parquet.File {
		buf := &bytes.Buffer{}
		writer := parquet.NewGenericWriter[SchemaB](buf, parquet.PageBufferSize(256)) // Small pages
		records := make([]SchemaB, rowsPerFile)
		for i := range records {
			records[i] = SchemaB{
				ID: startID + int64(i),
				X:  fmt.Sprintf("b-value-%d", startID+int64(i)),
				Z:  int32(startID) + int32(i)*100 + 1, // +1 to ensure non-zero
			}
		}
		if _, err := writer.Write(records); err != nil {
			t.Fatalf("Failed to write SchemaB records: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close SchemaB writer: %v", err)
		}

		file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("Failed to open SchemaB file: %v", err)
		}
		return file
	}

	// Create 4 files: 2 with SchemaA, 2 with SchemaB
	fileA1 := createFileA(0)
	fileA2 := createFileA(10000)
	fileB1 := createFileB(20000)
	fileB2 := createFileB(30000)

	// First level merge: merge A1+B1 and A2+B2 separately
	// This creates multiColumnChunk structures
	merged1, err := parquet.MergeRowGroups([]parquet.RowGroup{
		fileA1.RowGroups()[0],
		fileB1.RowGroups()[0],
	})
	if err != nil {
		t.Fatalf("First merge (A1+B1) failed: %v", err)
	}

	merged2, err := parquet.MergeRowGroups([]parquet.RowGroup{
		fileA2.RowGroups()[0],
		fileB2.RowGroups()[0],
	})
	if err != nil {
		t.Fatalf("Second merge (A2+B2) failed: %v", err)
	}

	// Second level merge: merge the two merged results
	// This exercises nested multiColumnChunk flattening
	finalMerged, err := parquet.MergeRowGroups([]parquet.RowGroup{merged1, merged2})
	if err != nil {
		t.Fatalf("Final merge failed: %v", err)
	}

	// Verify the merged schema has all columns (id, x, y, z)
	mergedSchema := finalMerged.Schema()
	columns := mergedSchema.Columns()
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns in merged schema, got %d: %v", len(columns), columns)
	}

	// Write the merged data to a buffer - this exercises the conversion code
	// and will panic if there are issues with missing column handling
	outputBuf := &bytes.Buffer{}
	writer := parquet.NewWriter(outputBuf, mergedSchema)

	rows := finalMerged.Rows()
	n, err := parquet.CopyRows(writer, rows)
	rows.Close()
	if err != nil {
		t.Fatalf("CopyRows failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Writer close failed: %v", err)
	}

	expectedRows := int64(rowsPerFile * 4) // 4 files
	if n != expectedRows {
		t.Fatalf("Expected %d rows copied, got %d", expectedRows, n)
	}

	// Read back and verify data integrity
	outputFile, err := parquet.OpenFile(bytes.NewReader(outputBuf.Bytes()), int64(outputBuf.Len()))
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}

	// Verify the output file has the expected number of rows
	outputRowCount := outputFile.RowGroups()[0].NumRows()
	if outputRowCount != expectedRows {
		t.Errorf("Expected %d rows in output, got %d", expectedRows, outputRowCount)
	}

	// Verify we can read back all rows without error (the original panic test)
	reader := outputFile.RowGroups()[0].Rows()
	defer reader.Close()

	rowBuf := make([]parquet.Row, 100)
	totalRows := int64(0)
	for {
		n, err := reader.ReadRows(rowBuf)
		totalRows += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadRows failed: %v", err)
		}
	}

	if totalRows != expectedRows {
		t.Errorf("Expected %d total rows read, got %d", expectedRows, totalRows)
	}

	// Note: When merging schemas with missing columns, the missing columns become optional.
	// The key test is that merge, write, and read operations complete without panic.
	t.Logf("Successfully merged and wrote %d rows from 4 files with different schemas", totalRows)
}

func TestMergeRowGroupsFromTestdataFiles(t *testing.T) {
	mergeDir := "testdata/merge"

	entries, err := os.ReadDir(mergeDir)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("testdata/merge directory does not exist")
		}
		t.Fatalf("failed to read testdata/merge directory: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		testDir := filepath.Join(mergeDir, entry.Name())
		t.Run(entry.Name(), func(t *testing.T) {
			// Find all parquet files in this directory
			parquetFiles, err := filepath.Glob(filepath.Join(testDir, "*.parquet"))
			if err != nil {
				t.Fatalf("failed to glob parquet files: %v", err)
			}

			if len(parquetFiles) == 0 {
				t.Skip("no parquet files found")
			}

			// Open all parquet files and collect row groups
			var rowGroups []parquet.RowGroup
			var openFiles []*os.File
			defer func() {
				for _, f := range openFiles {
					f.Close()
				}
			}()

			for _, path := range parquetFiles {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("failed to open file %s: %v", path, err)
				}
				openFiles = append(openFiles, f)

				stat, err := f.Stat()
				if err != nil {
					t.Fatalf("failed to stat file %s: %v", path, err)
				}

				pf, err := parquet.OpenFile(f, stat.Size(),
					parquet.SkipBloomFilters(true),
					parquet.SkipPageIndex(true),
					parquet.ReadBufferSize(256*1024), // 256 KiB
				)
				if err != nil {
					t.Fatalf("failed to open parquet file %s: %v", path, err)
				}

				rowGroups = append(rowGroups, pf.RowGroups()...)
			}

			t.Logf("merging %d row groups from %d files", len(rowGroups), len(parquetFiles))

			// Merge row groups
			mergedRowGroups, err := parquet.MergeRowGroups(rowGroups)
			if err != nil {
				t.Fatalf("failed to merge row groups: %v", err)
			}

			// Write merged data to a buffer
			var output bytes.Buffer
			writer := parquet.NewWriter(&output, mergedRowGroups.Schema(),
				parquet.DataPageStatistics(true),
				parquet.PageBufferSize(1024*1024),      // 1 MiB
				parquet.WriteBufferSize(256*1024),      // 256 KiB
				parquet.MaxRowsPerRowGroup(1000000),    // 1M rows
				parquet.DictionaryMaxBytes(2*1024*1024), // 2 MiB - limit dictionary size to prevent overflow
			)

			rowReader := mergedRowGroups.Rows()
			defer rowReader.Close()

			n, err := parquet.CopyRows(writer, rowReader)
			if err != nil {
				t.Fatalf("failed to copy rows: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			t.Logf("successfully merged and wrote %d rows to %d bytes", n, output.Len())

			// Verify the merged file can be read back
			mergedFile, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatalf("failed to open merged file: %v", err)
			}

			// Read all rows to ensure data integrity
			rows := mergedFile.RowGroups()
			var totalRows int64
			for _, rg := range rows {
				totalRows += rg.NumRows()
			}

			if totalRows != n {
				t.Errorf("expected %d rows in merged file, got %d", n, totalRows)
			}
		})
	}
}
