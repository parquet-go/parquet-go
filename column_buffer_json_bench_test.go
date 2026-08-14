package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// BenchmarkWriteJSONShredded measures writing rows whose json.RawMessage
// column is shredded into a typed group schema: each document is parsed with
// jsonlite on the write path and its fields distributed into the group's
// columns.
func BenchmarkWriteJSONShredded(b *testing.B) {
	type httpRequest struct {
		Method    string  `parquet:"method"`
		URL       string  `parquet:"url"`
		Status    int32   `parquet:"status"`
		LatencyMS float64 `parquet:"latency_ms"`
		UserAgent string  `parquet:"user_agent"`
		RemoteIP  string  `parquet:"remote_ip"`
		CacheHit  bool    `parquet:"cache_hit"`
	}
	type logData struct {
		Severity string      `parquet:"severity"`
		Message  string      `parquet:"message"`
		Trace    string      `parquet:"trace"`
		HTTP     httpRequest `parquet:"http"`
	}
	type typedRecord struct {
		ID   int64   `parquet:"id"`
		Data logData `parquet:"data"`
	}
	type rawRecord struct {
		ID   int64           `parquet:"id"`
		Data json.RawMessage `parquet:"data"`
	}

	httpJSON := `"http":{"method":"GET","url":"https://example.com/api/v1/items",` +
		`"status":200,"latency_ms":12.5,"user_agent":"Mozilla/5.0 (compatible)",` +
		`"remote_ip":"192.168.1.100","cache_hit":false}`
	small := `{"severity":"INFO","message":"request served","trace":"","` +
		`http":{"method":"GET","url":"/","status":200,"latency_ms":1,` +
		`"user_agent":"","remote_ip":"","cache_hit":true}}`
	medium := `{"severity":"INFO","message":"` + strings.Repeat("request served ", 8) + `",` +
		`"trace":"projects/test-project/traces/1234567890abcdef",` + httpJSON + `}`
	large := `{"severity":"WARNING","message":"` + strings.Repeat("upstream latency above threshold ", 24) + `",` +
		`"trace":"projects/test-project/traces/1234567890abcdef",` + httpJSON + `}`

	schema := SchemaOf(typedRecord{})
	const numRows = 512
	for _, payload := range []struct {
		name string
		data string
	}{
		{"small", small},
		{"medium", medium},
		{"large", large},
	} {
		rows := make([]rawRecord, numRows)
		for i := range rows {
			rows[i] = rawRecord{ID: int64(i), Data: json.RawMessage(payload.data)}
		}
		w := NewGenericWriter[rawRecord](io.Discard, schema)
		b.Run(fmt.Sprintf("%s/%dB", payload.name, len(payload.data)), func(b *testing.B) {
			b.SetBytes(int64(numRows * len(payload.data)))
			for b.Loop() {
				w.Reset(io.Discard)
				if _, err := w.Write(rows); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWriteJSONLeaves measures writing rows whose individual leaf
// columns are fed json.RawMessage fragments, each parsed with jsonlite and
// coerced to the leaf type.
func BenchmarkWriteJSONLeaves(b *testing.B) {
	type typedRecord struct {
		ID     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Age    int32   `parquet:"age"`
		Active bool    `parquet:"active"`
		Score  float64 `parquet:"score"`
	}
	type rawRecord struct {
		ID     int64           `parquet:"id"`
		Name   json.RawMessage `parquet:"name"`
		Age    json.RawMessage `parquet:"age"`
		Active json.RawMessage `parquet:"active"`
		Score  json.RawMessage `parquet:"score"`
	}

	schema := SchemaOf(typedRecord{})
	const numRows = 512
	rows := make([]rawRecord, numRows)
	totalBytes := 0
	for i := range rows {
		rows[i] = rawRecord{
			ID:     int64(i),
			Name:   json.RawMessage(`"user-name-with-some-length"`),
			Age:    json.RawMessage(`42`),
			Active: json.RawMessage(`true`),
			Score:  json.RawMessage(`95.5`),
		}
	}
	for _, r := range rows {
		totalBytes += len(r.Name) + len(r.Age) + len(r.Active) + len(r.Score)
	}
	w := NewGenericWriter[rawRecord](io.Discard, schema)
	b.SetBytes(int64(totalBytes))
	for b.Loop() {
		w.Reset(io.Discard)
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
