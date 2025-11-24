package parquet

import (
	"iter"
	"slices"
	"testing"
)

// jsonTokens is a test helper that wraps jsonTokenizer to provide an iter.Seq
// interface for testing. This ensures that the tokenizer implementation is
// properly tested through all the token tests.
func jsonTokens(json string) iter.Seq[string] {
	return func(yield func(string) bool) {
		t := &jsonTokenizer{json: json}
		for {
			token, ok := t.next()
			if !ok {
				return
			}
			if !yield(token) {
				return
			}
		}
	}
}

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

		serialized := jsonFormat(nil, val)

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

func TestJSONTokensNull(t *testing.T) {
	tokens := slices.Collect(jsonTokens("null"))
	expected := []string{"null"}
	if !slices.Equal(tokens, expected) {
		t.Errorf("tokens mismatch:\nexpected: %v\ngot: %v", expected, tokens)
	}
}

func TestJSONTokensBool(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"true", []string{"true"}},
		{"false", []string{"false"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensNumber(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"0", []string{"0"}},
		{"42", []string{"42"}},
		{"-42", []string{"-42"}},
		{"3.14", []string{"3.14"}},
		{"-3.14", []string{"-3.14"}},
		{"1e10", []string{"1e10"}},
		{"1.5e-10", []string{"1.5e-10"}},
		{"9223372036854775807", []string{"9223372036854775807"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensString(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{`"hello"`, []string{`"hello"`}},
		{`""`, []string{`""`}},
		{`"hello world"`, []string{`"hello world"`}},
		{`"with\nnewline"`, []string{`"with\nnewline"`}},
		{`"with\ttab"`, []string{`"with\ttab"`}},
		{`"with\"quote"`, []string{`"with\"quote"`}},
		{`"with\\backslash"`, []string{`"with\\backslash"`}},
		{`"unicode: \u0048\u0065\u006c\u006c\u006f"`, []string{`"unicode: \u0048\u0065\u006c\u006c\u006f"`}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensArray(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"[]", []string{"[", "]"}},
		{"[1]", []string{"[", "1", "]"}},
		{"[1,2,3]", []string{"[", "1", ",", "2", ",", "3", "]"}},
		{`["a","b","c"]`, []string{"[", `"a"`, ",", `"b"`, ",", `"c"`, "]"}},
		{"[1, 2, 3]", []string{"[", "1", ",", "2", ",", "3", "]"}},
		{"[ 1 , 2 , 3 ]", []string{"[", "1", ",", "2", ",", "3", "]"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensObject(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"{}", []string{"{", "}"}},
		{`{"a":1}`, []string{"{", `"a"`, ":", "1", "}"}},
		{`{"a":1,"b":2}`, []string{"{", `"a"`, ":", "1", ",", `"b"`, ":", "2", "}"}},
		{`{ "a" : 1 , "b" : 2 }`, []string{"{", `"a"`, ":", "1", ",", `"b"`, ":", "2", "}"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensNested(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			`{"name":"test","age":42}`,
			[]string{"{", `"name"`, ":", `"test"`, ",", `"age"`, ":", "42", "}"},
		},
		{
			`{"tags":["a","b","c"]}`,
			[]string{"{", `"tags"`, ":", "[", `"a"`, ",", `"b"`, ",", `"c"`, "]", "}"},
		},
		{
			`{"user":{"name":"Alice","age":30}}`,
			[]string{"{", `"user"`, ":", "{", `"name"`, ":", `"Alice"`, ",", `"age"`, ":", "30", "}", "}"},
		},
		{
			`[{"id":1},{"id":2}]`,
			[]string{"[", "{", `"id"`, ":", "1", "}", ",", "{", `"id"`, ":", "2", "}", "]"},
		},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensWhitespace(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"  true  ", []string{"true"}},
		{"\ttrue\t", []string{"true"}},
		{"\ntrue\n", []string{"true"}},
		{"\r\ntrue\r\n", []string{"true"}},
		{"  [  1  ,  2  ,  3  ]  ", []string{"[", "1", ",", "2", ",", "3", "]"}},
		{"\n\t{\n\t\"a\"\n\t:\n\t1\n\t}\n\t", []string{"{", `"a"`, ":", "1", "}"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensInvalid(t *testing.T) {
	// The tokenizer is flexible and handles invalid JSON
	tests := []struct {
		input    string
		expected []string
	}{
		// Unclosed string - tokenizer continues to end
		{`"unclosed`, []string{`"unclosed`}},
		// Trailing comma
		{`[1,2,3,]`, []string{"[", "1", ",", "2", ",", "3", ",", "]"}},
		// Missing quotes on keys (treats as token)
		{`{name:"test"}`, []string{"{", "name", ":", `"test"`, "}"}},
		// Multiple values without array
		{`1 2 3`, []string{"1", "2", "3"}},
		// Missing comma
		{`[1 2 3]`, []string{"[", "1", "2", "3", "]"}},
		// Extra colons
		{`{"a"::1}`, []string{"{", `"a"`, ":", ":", "1", "}"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensEmpty(t *testing.T) {
	tokens := slices.Collect(jsonTokens(""))
	var expected []string
	if !slices.Equal(tokens, expected) {
		t.Errorf("tokens mismatch for empty string:\nexpected: %v\ngot: %v", expected, tokens)
	}
}

func TestJSONTokensComplex(t *testing.T) {
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

	expected := []string{
		"{",
		`"name"`, ":", `"test"`, ",",
		`"age"`, ":", "42", ",",
		`"active"`, ":", "true", ",",
		`"tags"`, ":", "[", `"a"`, ",", `"b"`, ",", `"c"`, "]", ",",
		`"metadata"`, ":", "{",
		`"created"`, ":", `"2024-01-01"`, ",",
		`"updated"`, ":", "null",
		"}",
		"}",
	}

	tokens := slices.Collect(jsonTokens(input))
	if !slices.Equal(tokens, expected) {
		t.Errorf("tokens mismatch:\nexpected: %v\ngot: %v", expected, tokens)
	}
}

func TestJSONTokensEscapedQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{`"a\"b"`, []string{`"a\"b"`}},
		{`"a\\\"b"`, []string{`"a\\\"b"`}},
		{`"a\\\\\"b"`, []string{`"a\\\\\"b"`}},
		{`{"key":"value\"with\"quotes"}`, []string{"{", `"key"`, ":", `"value\"with\"quotes"`, "}"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensSpecialNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"0.0", []string{"0.0"}},
		{"-0", []string{"-0"}},
		{"1e+10", []string{"1e+10"}},
		{"1E10", []string{"1E10"}},
		{"1.23e-45", []string{"1.23e-45"}},
	}

	for _, tt := range tests {
		tokens := slices.Collect(jsonTokens(tt.input))
		if !slices.Equal(tokens, tt.expected) {
			t.Errorf("tokens mismatch for %q:\nexpected: %v\ngot: %v", tt.input, tt.expected, tokens)
		}
	}
}

func TestJSONTokensMixedTypes(t *testing.T) {
	input := `[null,true,false,42,"string",{"key":"value"},[1,2,3]]`

	expected := []string{
		"[",
		"null", ",",
		"true", ",",
		"false", ",",
		"42", ",",
		`"string"`, ",",
		"{", `"key"`, ":", `"value"`, "}", ",",
		"[", "1", ",", "2", ",", "3", "]",
		"]",
	}

	tokens := slices.Collect(jsonTokens(input))
	if !slices.Equal(tokens, expected) {
		t.Errorf("tokens mismatch:\nexpected: %v\ngot: %v", expected, tokens)
	}
}

func BenchmarkJSONTokens(b *testing.B) {
	benchmarks := []struct {
		name  string
		input string
	}{
		{
			name:  "Small",
			input: `{"name":"test","age":42,"active":true}`,
		},
		{
			name: "Medium",
			input: `{
				"name": "test",
				"age": 42,
				"active": true,
				"tags": ["a", "b", "c"],
				"metadata": {
					"created": "2024-01-01",
					"updated": null
				}
			}`,
		},
		{
			name: "Large",
			input: `{
				"users": [
					{"id":1,"name":"Alice","email":"alice@example.com","active":true},
					{"id":2,"name":"Bob","email":"bob@example.com","active":false},
					{"id":3,"name":"Charlie","email":"charlie@example.com","active":true}
				],
				"metadata": {
					"total": 3,
					"page": 1,
					"perPage": 10,
					"filters": {
						"status": "active",
						"role": ["admin", "user"],
						"createdAfter": "2024-01-01T00:00:00Z"
					}
				},
				"stats": {
					"activeUsers": 2,
					"inactiveUsers": 1,
					"averageAge": 28.5,
					"tags": ["production", "verified", "premium"]
				}
			}`,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(bm.input)))
			for b.Loop() {
				for range jsonTokens(bm.input) {
				}
			}
		})
	}
}

func BenchmarkJSONTokensCollect(b *testing.B) {
	benchmarks := []struct {
		name  string
		input string
	}{
		{
			name:  "Small",
			input: `{"name":"test","age":42,"active":true}`,
		},
		{
			name: "Medium",
			input: `{
				"name": "test",
				"age": 42,
				"active": true,
				"tags": ["a", "b", "c"],
				"metadata": {
					"created": "2024-01-01",
					"updated": null
				}
			}`,
		},
		{
			name: "Large",
			input: `{
				"users": [
					{"id":1,"name":"Alice","email":"alice@example.com","active":true},
					{"id":2,"name":"Bob","email":"bob@example.com","active":false},
					{"id":3,"name":"Charlie","email":"charlie@example.com","active":true}
				],
				"metadata": {
					"total": 3,
					"page": 1,
					"perPage": 10,
					"filters": {
						"status": "active",
						"role": ["admin", "user"],
						"createdAfter": "2024-01-01T00:00:00Z"
					}
				},
				"stats": {
					"activeUsers": 2,
					"inactiveUsers": 1,
					"averageAge": 28.5,
					"tags": ["production", "verified", "premium"]
				}
			}`,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(bm.input)))
			for b.Loop() {
				_ = slices.Collect(jsonTokens(bm.input))
			}
		})
	}
}

func BenchmarkJSONParse(b *testing.B) {
	benchmarks := []struct {
		name  string
		input string
	}{
		{
			name:  "Small",
			input: `{"name":"test","age":42,"active":true}`,
		},
		{
			name: "Medium",
			input: `{
				"name": "test",
				"age": 42,
				"active": true,
				"tags": ["a", "b", "c"],
				"metadata": {
					"created": "2024-01-01",
					"updated": null
				}
			}`,
		},
		{
			name: "Large",
			input: `{
				"users": [
					{"id":1,"name":"Alice","email":"alice@example.com","active":true},
					{"id":2,"name":"Bob","email":"bob@example.com","active":false},
					{"id":3,"name":"Charlie","email":"charlie@example.com","active":true}
				],
				"metadata": {
					"total": 3,
					"page": 1,
					"perPage": 10,
					"filters": {
						"status": "active",
						"role": ["admin", "user"],
						"createdAfter": "2024-01-01T00:00:00Z"
					}
				},
				"stats": {
					"activeUsers": 2,
					"inactiveUsers": 1,
					"averageAge": 28.5,
					"tags": ["production", "verified", "premium"]
				}
			}`,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			data := []byte(bm.input)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				_, err := jsonParse(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestUnquoteValid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "EmptyString",
			input:    `""`,
			expected: "",
		},
		{
			name:     "SimpleString",
			input:    `"hello"`,
			expected: "hello",
		},
		{
			name:     "StringWithSpaces",
			input:    `"hello world"`,
			expected: "hello world",
		},
		{
			name:     "EscapedQuote",
			input:    `"say \"hello\""`,
			expected: `say "hello"`,
		},
		{
			name:     "EscapedBackslash",
			input:    `"path\\to\\file"`,
			expected: `path\to\file`,
		},
		{
			name:     "EscapedSlash",
			input:    `"a\/b"`,
			expected: "a/b",
		},
		{
			name:     "EscapedBackspace",
			input:    `"a\bb"`,
			expected: "a\bb",
		},
		{
			name:     "EscapedFormfeed",
			input:    `"a\fb"`,
			expected: "a\fb",
		},
		{
			name:     "EscapedNewline",
			input:    `"line1\nline2"`,
			expected: "line1\nline2",
		},
		{
			name:     "EscapedCarriageReturn",
			input:    `"line1\rline2"`,
			expected: "line1\rline2",
		},
		{
			name:     "EscapedTab",
			input:    `"col1\tcol2"`,
			expected: "col1\tcol2",
		},
		{
			name:     "UnicodeNull",
			input:    `"\u0000"`,
			expected: "\u0000",
		},
		{
			name:     "UnicodeASCII",
			input:    `"\u0041"`,
			expected: "A",
		},
		{
			name:     "UnicodeMultiByte",
			input:    `"\u4e2d\u6587"`,
			expected: "中文",
		},
		{
			name:     "UnicodeMax",
			input:    `"\uffff"`,
			expected: "\uffff",
		},
		{
			name:     "MixedEscapes",
			input:    `"line1\nline2\ttab\u0041end"`,
			expected: "line1\nline2\ttabAend",
		},
		{
			name:     "MultipleQuotes",
			input:    `"\"quote1\" and \"quote2\""`,
			expected: `"quote1" and "quote2"`,
		},
		{
			name:     "AllSingleCharEscapes",
			input:    `"\"\\\//\b\f\n\r\t"`,
			expected: "\"\\//\b\f\n\r\t",
		},
		{
			name:     "OnlyEscapedChars",
			input:    `"\n\t"`,
			expected: "\n\t",
		},
		{
			name:     "LongString",
			input:    `"The quick brown fox jumps over the lazy dog"`,
			expected: "The quick brown fox jumps over the lazy dog",
		},
		{
			name:     "StringWithNumbers",
			input:    `"test123"`,
			expected: "test123",
		},
		{
			name:     "JSONValue",
			input:    `"{\"key\":\"value\"}"`,
			expected: `{"key":"value"}`,
		},
		{
			name:     "UnicodeLowercaseHex",
			input:    `"\u00e9"`,
			expected: "é",
		},
		{
			name:     "UnicodeEmoji",
			input:    `"\ud83d\ude00"`,
			expected: "😀",
		},
		{
			name:     "UnicodeSurrogatePairHeart",
			input:    `"\ud83d\udc96"`,
			expected: "💖",
		},
		{
			name:     "UnicodeSurrogatePairRocket",
			input:    `"\ud83d\ude80"`,
			expected: "🚀",
		},
		{
			name:     "MultipleEmojis",
			input:    `"\ud83d\ude00\ud83d\udc96"`,
			expected: "😀💖",
		},
		{
			name:     "EmojiWithText",
			input:    `"Hello \ud83d\udc4b World"`,
			expected: "Hello 👋 World",
		},
		{
			name:     "ConsecutiveEscapes",
			input:    `"\\\\n"`,
			expected: `\\n`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unquote(tt.input)
			if err != nil {
				t.Errorf("unquote(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.expected {
				t.Errorf("unquote(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestUnquoteInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "NoQuotes",
			input: "hello",
		},
		{
			name:  "OnlyOpeningQuote",
			input: `"hello`,
		},
		{
			name:  "OnlyClosingQuote",
			input: `hello"`,
		},
		{
			name:  "SingleQuote",
			input: `"`,
		},
		{
			name:  "EmptyInput",
			input: "",
		},
		{
			name:  "TrailingBackslash",
			input: `"hello\`,
		},
		{
			name:  "TrailingBackslashBeforeQuote",
			input: `"hello\"`,
		},
		{
			name:  "InvalidEscapeX",
			input: `"hello\x"`,
		},
		{
			name:  "InvalidEscapeV",
			input: `"hello\v"`,
		},
		{
			name:  "InvalidEscapeA",
			input: `"hello\a"`,
		},
		{
			name:  "InvalidEscapeDigit",
			input: `"hello\0"`,
		},
		{
			name:  "IncompleteUnicode3Chars",
			input: `"\u041"`,
		},
		{
			name:  "IncompleteUnicode2Chars",
			input: `"\u04"`,
		},
		{
			name:  "IncompleteUnicode1Char",
			input: `"\u0"`,
		},
		{
			name:  "IncompleteUnicodeNoChars",
			input: `"\u"`,
		},
		{
			name:  "InvalidUnicodeHexG",
			input: `"\u00GG"`,
		},
		{
			name:  "InvalidUnicodeHexSpace",
			input: `"\u00 0"`,
		},
		{
			name:  "InvalidUnicodeHexMinus",
			input: `"\u-001"`,
		},
		{
			name:  "UnicodeAtEnd",
			input: `"hello\u123"`,
		},
		{
			name:  "BackslashAtVeryEnd",
			input: `"test\`,
		},
		{
			name:  "OnlyBackslash",
			input: `"\"`,
		},
		{
			name:  "UnterminatedString",
			input: `"hello world`,
		},
		{
			name:  "WrongQuoteType",
			input: "'hello'",
		},
		{
			name:  "HighSurrogateWithoutLow",
			input: `"\ud83d"`,
		},
		{
			name:  "HighSurrogateWithText",
			input: `"\ud83dtext"`,
		},
		{
			name:  "HighSurrogateWithNormalUnicode",
			input: `"\ud83d\u0041"`,
		},
		{
			name:  "LowSurrogateWithoutHigh",
			input: `"\ude00"`,
		},
		{
			name:  "LowSurrogateAlone",
			input: `"\udc96"`,
		},
		{
			name:  "HighSurrogateWithInvalidLow",
			input: `"\ud83d\uffff"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unquote(tt.input)
			if err == nil {
				t.Errorf("unquote(%q) expected error, got %q", tt.input, got)
			}
		})
	}
}
