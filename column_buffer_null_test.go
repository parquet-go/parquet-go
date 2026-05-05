package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestNullColumnBuffer(t *testing.T) {
	// Create a null type
	nullType := parquet.NullType

	// Create a column buffer
	columnIndex := 0
	buffer := nullType.NewColumnBuffer(columnIndex, 0)

	// Test Type
	if buffer.Type() != nullType {
		t.Errorf("Type() = %v, want %v", buffer.Type(), nullType)
	}

	// Test Column
	if buffer.Column() != columnIndex {
		t.Errorf("Column() = %d, want %d", buffer.Column(), columnIndex)
	}

	// Test Dictionary
	if buffer.Dictionary() != nil {
		t.Errorf("Dictionary() = %v, want nil", buffer.Dictionary())
	}

	// Test initial Len
	if buffer.Len() != 0 {
		t.Errorf("Len() = %d, want 0", buffer.Len())
	}

	// Test initial Cap
	if buffer.Cap() != 0 {
		t.Errorf("Cap() = %d, want 0", buffer.Cap())
	}

	// Test Size
	if buffer.Size() != 0 {
		t.Errorf("Size() = %d, want 0", buffer.Size())
	}

	// Test WriteValues
	values := make([]parquet.Value, 5)
	for i := range values {
		values[i] = parquet.NullValue()
	}
	n, err := buffer.WriteValues(values)
	if err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}
	if n != 5 {
		t.Errorf("WriteValues() = %d, want 5", n)
	}

	// Test Len after WriteValues
	if buffer.Len() != 5 {
		t.Errorf("Len() after WriteValues = %d, want 5", buffer.Len())
	}

	// Test Clone
	cloned := buffer.Clone()
	if cloned.Type() != nullType {
		t.Errorf("Clone().Type() = %v, want %v", cloned.Type(), nullType)
	}
	if cloned.Len() != buffer.Len() {
		t.Errorf("Clone().Len() = %d, want %d", cloned.Len(), buffer.Len())
	}

	// Test Reset
	buffer.Reset()
	if buffer.Len() != 0 {
		t.Errorf("Len() after Reset = %d, want 0", buffer.Len())
	}

	// Write some values to test Page and Pages
	values2 := make([]parquet.Value, 3)
	for i := range values2 {
		values2[i] = parquet.NullValue()
	}
	buffer.WriteValues(values2)

	// Test Page
	page := buffer.Page()
	if page.Type() != nullType {
		t.Errorf("Page().Type() = %v, want %v", page.Type(), nullType)
	}
	if page.NumValues() != 3 {
		t.Errorf("Page().NumValues() = %d, want 3", page.NumValues())
	}

	// Test Pages
	pages := buffer.Pages()
	page2, err := pages.ReadPage()
	if err != nil {
		t.Fatalf("Pages().ReadPage() error = %v", err)
	}
	if page2.Type() != nullType {
		t.Errorf("Pages().ReadPage().Type() = %v, want %v", page2.Type(), nullType)
	}
}

func TestNullColumnBufferReadValuesAt(t *testing.T) {
	nullType := parquet.NullType
	buffer := nullType.NewColumnBuffer(0, 0)

	// Write some values
	values := make([]parquet.Value, 3)
	for i := range values {
		values[i] = parquet.NullValue()
	}
	buffer.WriteValues(values)

	// Read them back
	readValues := make([]parquet.Value, 3)
	n, err := buffer.ReadValuesAt(readValues, 0)
	if err != nil {
		t.Fatalf("ReadValuesAt() error = %v", err)
	}
	if n != 3 {
		t.Errorf("ReadValuesAt() = %d, want 3", n)
	}

	// Check all values are null
	for i := range readValues {
		if !readValues[i].IsNull() {
			t.Errorf("readValues[%d].IsNull() = false, want true", i)
		}
	}

	// Test reading beyond bounds
	readValues2 := make([]parquet.Value, 1)
	_, err = buffer.ReadValuesAt(readValues2, 3)
	if err == nil {
		t.Error("ReadValuesAt() at offset 3 should return error, got nil")
	}

	// Test reading with offset
	readValues3 := make([]parquet.Value, 2)
	n, err = buffer.ReadValuesAt(readValues3, 1)
	if err != nil {
		t.Fatalf("ReadValuesAt() with offset 1 error = %v", err)
	}
	if n != 2 {
		t.Errorf("ReadValuesAt() with offset 1 = %d, want 2", n)
	}
}

func TestNullColumnBufferLessAndSwap(t *testing.T) {
	nullType := parquet.NullType
	buffer := nullType.NewColumnBuffer(0, 0)

	// Write some values
	values := make([]parquet.Value, 3)
	for i := range values {
		values[i] = parquet.NullValue()
	}
	buffer.WriteValues(values)

	// Test Less - all nulls are equal
	if buffer.Less(0, 1) {
		t.Error("Less(0, 1) = true, want false")
	}
	if buffer.Less(1, 0) {
		t.Error("Less(1, 0) = true, want false")
	}

	// Test Swap - should not panic
	buffer.Swap(0, 2)
	// No way to verify swap worked since all values are null, but it shouldn't panic
}
