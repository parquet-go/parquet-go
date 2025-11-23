package parquet

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
)

func TestJSONParseNull(t *testing.T) {
	val, err := jsonParse([]byte("null"))
	if err != nil {
		t.Fatal(err)
	}
	if val.kind() != jsonNull {
		t.Errorf("expected jsonNull, got %v", val.kind())
	}
}

func TestJSONParseBool(t *testing.T) {
	tests := []struct {
		input    string
		expected jsonKind
		value    bool
	}{
		{"true", jsonTrue, true},
		{"false", jsonFalse, false},
	}

	for _, tt := range tests {
		val, err := jsonParse([]byte(tt.input))
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		if val.kind() != tt.expected {
			t.Errorf("%q: expected %v, got %v", tt.input, tt.expected, val.kind())
		}
	}
}

func TestJSONParseNumber(t *testing.T) {
	tests := []string{
		"42",
		"-42",
		"0",
		"9223372036854775807",  // large int
		"18446744073709551615", // very large int
		"3.14",
		"-3.14",
		"1e10",
		"1.5e-10",
	}

	for _, input := range tests {
		val, err := jsonParse([]byte(input))
		if err != nil {
			t.Fatalf("parse %q: %v (type: %T)", input, err, err)
		}
		if val.kind() != jsonNumber {
			t.Errorf("%q: expected jsonNumber, got %v", input, val.kind())
		}
	}
}

func TestJSONParseString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"hello"`, "hello"},
		{`""`, ""},
		{`"hello world"`, "hello world"},
		{`"with\nnewline"`, "with\nnewline"},
		{`"with\ttab"`, "with\ttab"},
		{`"with\"quote"`, `with"quote`},
		{`"unicode: \u0048\u0065\u006c\u006c\u006f"`, "unicode: Hello"},
	}

	for _, tt := range tests {
		val, err := jsonParse([]byte(tt.input))
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		if val.kind() != jsonString {
			t.Errorf("%q: expected jsonString, got %v", tt.input, val.kind())
		}
		if val.string() != tt.expected {
			t.Errorf("%q: expected %q, got %q", tt.input, tt.expected, val.string())
		}
	}
}

func TestJSONParseArray(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"[]", 0},
		{"[1]", 1},
		{"[1,2,3]", 3},
		{`["a","b","c"]`, 3},
		{"[1,2,3,4,5,6,7,8,9,10]", 10},
	}

	for _, tt := range tests {
		val, err := jsonParse([]byte(tt.input))
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		if val.kind() != jsonArray {
			t.Errorf("%q: expected jsonArray, got %v", tt.input, val.kind())
		}
		if val.len() != tt.expected {
			t.Errorf("%q: expected length %d, got %d", tt.input, tt.expected, val.len())
		}
	}
}

func TestJSONParseObject(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"{}", 0},
		{`{"a":1}`, 1},
		{`{"a":1,"b":2}`, 2},
		{`{"a":1,"b":2,"c":3}`, 3},
	}

	for _, tt := range tests {
		val, err := jsonParse([]byte(tt.input))
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		if val.kind() != jsonObject {
			t.Errorf("%q: expected jsonObject, got %v", tt.input, val.kind())
		}
		if val.len() != tt.expected {
			t.Errorf("%q: expected length %d, got %d", tt.input, tt.expected, val.len())
		}
	}
}

func TestJSONParseNested(t *testing.T) {
	input := `{
		"name": "test",
		"age": 42,
		"active": true,
		"tags": ["a", "b", "c"],
		"metadata": {
			"created": "2024-01-01",
			"updated": null
		}
	}`

	val, err := jsonParse([]byte(input))
	if err != nil {
		t.Fatal(err)
	}

	if val.kind() != jsonObject {
		t.Fatalf("expected jsonObject, got %v", val.kind())
	}

	fields := val.object()
	if len(fields) != 5 {
		t.Fatalf("expected 5 fields, got %d", len(fields))
	}

	// Check "tags" array
	var tagsField *jsonField
	for i := range fields {
		if fields[i].key == "tags" {
			tagsField = &fields[i]
			break
		}
	}
	if tagsField == nil {
		t.Fatal("tags field not found")
	}
	if tagsField.val.kind() != jsonArray {
		t.Fatalf("tags: expected jsonArray, got %v", tagsField.val.kind())
	}
	if tagsField.val.len() != 3 {
		t.Fatalf("tags: expected length 3, got %d", tagsField.val.len())
	}

	// Check "metadata" object
	var metadataField *jsonField
	for i := range fields {
		if fields[i].key == "metadata" {
			metadataField = &fields[i]
			break
		}
	}
	if metadataField == nil {
		t.Fatal("metadata field not found")
	}
	if metadataField.val.kind() != jsonObject {
		t.Fatalf("metadata: expected jsonObject, got %v", metadataField.val.kind())
	}
}

func TestJSONSerializeRoundTrip(t *testing.T) {
	tests := []string{
		"null",
		"true",
		"false",
		"42",
		"3.14",
		`"hello"`,
		"[]",
		"[1,2,3]",
		"{}",
		`{"a":1}`,
		`{"a":1,"b":"hello","c":true,"d":null}`,
		`[1,"two",true,null,{"nested":"object"}]`,
		`{"array":[1,2,3],"object":{"nested":true}}`,
	}

	for _, input := range tests {
		val, err := jsonParse([]byte(input))
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}

		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		jsonFormat(stream, val)
		serialized := stream.Buffer()
		jsoniter.ConfigFastest.ReturnStream(stream)

		val2, err := jsonParse(serialized)
		if err != nil {
			t.Fatalf("parse serialized %q: %v", serialized, err)
		}

		if !jsonValuesEqual(*val, *val2) {
			t.Errorf("round-trip failed for %q\ngot: %s", input, serialized)
		}
	}
}

func jsonValuesEqual(a, b jsonValue) bool {
	if a.kind() != b.kind() {
		return false
	}

	switch a.kind() {
	case jsonNull, jsonTrue, jsonFalse:
		return true
	case jsonNumber:
		return a.float() == b.float()
	case jsonString:
		return a.string() == b.string()
	case jsonArray:
		aArr := a.array()
		bArr := b.array()
		if len(aArr) != len(bArr) {
			return false
		}
		for i := range aArr {
			if !jsonValuesEqual(aArr[i], bArr[i]) {
				return false
			}
		}
		return true
	case jsonObject:
		aObj := a.object()
		bObj := b.object()
		if len(aObj) != len(bObj) {
			return false
		}
		// Note: field order might differ, so we need to match by key
		for _, af := range aObj {
			found := false
			for _, bf := range bObj {
				if af.key == bf.key {
					if !jsonValuesEqual(af.val, bf.val) {
						return false
					}
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	return false
}

func TestJSONAccessors(t *testing.T) {
	input := `{"num1":42,"num2":18446744073709551615,"num3":3.14,"string":"hello","bool":true,"null":null,"array":[1,2],"object":{"a":1}}`
	val, err := jsonParse([]byte(input))
	if err != nil {
		t.Fatal(err)
	}

	fields := val.object()
	fieldMap := make(map[string]jsonValue)
	for _, f := range fields {
		fieldMap[f.key] = f.val
	}

	// Test number accessor - all numbers are float64
	if num1Val, ok := fieldMap["num1"]; ok {
		if num1Val.kind() != jsonNumber {
			t.Errorf("num1: expected jsonNumber, got %v", num1Val.kind())
		}
		if num1Val.float() != 42 {
			t.Errorf("num1: expected 42, got %f", num1Val.float())
		}
	} else {
		t.Error("num1 field not found")
	}

	// Test large number
	if num2Val, ok := fieldMap["num2"]; ok {
		if num2Val.kind() != jsonNumber {
			t.Errorf("num2: expected jsonNumber, got %v", num2Val.kind())
		}
	} else {
		t.Error("num2 field not found")
	}

	// Test float
	if num3Val, ok := fieldMap["num3"]; ok {
		if num3Val.kind() != jsonNumber {
			t.Errorf("num3: expected jsonNumber, got %v", num3Val.kind())
		}
		if num3Val.float() != 3.14 {
			t.Errorf("num3: expected 3.14, got %f", num3Val.float())
		}
	} else {
		t.Error("num3 field not found")
	}

	// Test string accessor
	if strVal, ok := fieldMap["string"]; ok {
		if strVal.kind() != jsonString {
			t.Errorf("string: expected jsonString, got %v", strVal.kind())
		}
		if strVal.string() != "hello" {
			t.Errorf("string: expected 'hello', got %q", strVal.string())
		}
	} else {
		t.Error("string field not found")
	}

	// Test bool accessor
	if boolVal, ok := fieldMap["bool"]; ok {
		if boolVal.kind() != jsonTrue {
			t.Errorf("bool: expected jsonTrue, got %v", boolVal.kind())
		}
	} else {
		t.Error("bool field not found")
	}

	// Test null
	if nullVal, ok := fieldMap["null"]; ok {
		if nullVal.kind() != jsonNull {
			t.Errorf("null: expected jsonNull, got %v", nullVal.kind())
		}
	} else {
		t.Error("null field not found")
	}

	// Test array accessor
	if arrayVal, ok := fieldMap["array"]; ok {
		if arrayVal.kind() != jsonArray {
			t.Errorf("array: expected jsonArray, got %v", arrayVal.kind())
		}
		arr := arrayVal.array()
		if len(arr) != 2 {
			t.Errorf("array: expected length 2, got %d", len(arr))
		}
	} else {
		t.Error("array field not found")
	}

	// Test object accessor
	if objVal, ok := fieldMap["object"]; ok {
		if objVal.kind() != jsonObject {
			t.Errorf("object: expected jsonObject, got %v", objVal.kind())
		}
		obj := objVal.object()
		if len(obj) != 1 {
			t.Errorf("object: expected length 1, got %d", len(obj))
		}
	} else {
		t.Error("object field not found")
	}
}

func TestJSONParseError(t *testing.T) {
	tests := []string{
		"{",
		`{"unclosed": "string}`,
	}

	for _, input := range tests {
		_, err := jsonParse([]byte(input))
		if err == nil {
			t.Errorf("expected error for %q, got nil", input)
		}
	}
}
