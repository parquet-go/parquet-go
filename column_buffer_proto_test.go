package parquet

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	testproto "github.com/parquet-go/parquet-go/testdata/gen/go"
)

func TestProtoTimestamps(t *testing.T) {
	type Event struct {
		ID        int64                  `parquet:"id"`
		Name      string                 `parquet:"name"`
		Timestamp *timestamppb.Timestamp `parquet:"timestamp"`
		CreatedAt *timestamppb.Timestamp `parquet:"created_at,optional"`
	}

	now := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)
	created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	events := []Event{
		{
			ID:        1,
			Name:      "event1",
			Timestamp: timestamppb.New(now),
			CreatedAt: timestamppb.New(created),
		},
		{
			ID:        2,
			Name:      "event2",
			Timestamp: timestamppb.New(now.Add(time.Hour)),
			CreatedAt: nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Event](buf)
	if _, err := writer.Write(events); err != nil {
		t.Fatalf("failed to write events: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Event](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readEvents := make([]Event, len(events))
	if _, err := reader.Read(readEvents); err != nil && err != io.EOF {
		t.Fatalf("failed to read events: %v", err)
	}

	if !reflect.DeepEqual(events, readEvents) {
		t.Errorf("events mismatch:\nexpected: %+v\ngot: %+v", events, readEvents)
	}
}

func TestProtoDurations(t *testing.T) {
	type Task struct {
		ID       int64                `parquet:"id"`
		Name     string               `parquet:"name"`
		Duration *durationpb.Duration `parquet:"duration"`
		Timeout  *durationpb.Duration `parquet:"timeout,optional"`
	}

	tasks := []Task{
		{
			ID:       1,
			Name:     "task1",
			Duration: durationpb.New(5*time.Second + 500*time.Millisecond),
			Timeout:  durationpb.New(30 * time.Second),
		},
		{
			ID:       2,
			Name:     "task2",
			Duration: durationpb.New(100 * time.Millisecond),
			Timeout:  nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Task](buf)
	if _, err := writer.Write(tasks); err != nil {
		t.Fatalf("failed to write tasks: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Task](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readTasks := make([]Task, len(tasks))
	if _, err := reader.Read(readTasks); err != nil && err != io.EOF {
		t.Fatalf("failed to read tasks: %v", err)
	}

	if !reflect.DeepEqual(tasks, readTasks) {
		t.Errorf("tasks mismatch:\nexpected: %+v\ngot: %+v", tasks, readTasks)
	}
}

func TestProtoWrappers(t *testing.T) {
	type Record struct {
		ID      int64                   `parquet:"id"`
		Active  *wrapperspb.BoolValue   `parquet:"active,optional"`
		Count   *wrapperspb.Int32Value  `parquet:"count,optional"`
		Size    *wrapperspb.Int64Value  `parquet:"size,optional"`
		Score   *wrapperspb.FloatValue  `parquet:"score,optional"`
		Rating  *wrapperspb.DoubleValue `parquet:"rating,optional"`
		Name    *wrapperspb.StringValue `parquet:"name,optional"`
		Payload *wrapperspb.BytesValue  `parquet:"payload,optional"`
	}

	records := []Record{
		{
			ID:      1,
			Active:  wrapperspb.Bool(true),
			Count:   wrapperspb.Int32(42),
			Size:    wrapperspb.Int64(123456789),
			Score:   wrapperspb.Float(3.14),
			Rating:  wrapperspb.Double(4.5),
			Name:    wrapperspb.String("test"),
			Payload: wrapperspb.Bytes([]byte{0x01, 0x02, 0x03}),
		},
		{
			ID:      2,
			Active:  nil,
			Count:   nil,
			Size:    nil,
			Score:   nil,
			Rating:  nil,
			Name:    nil,
			Payload: nil,
		},
		{
			ID:      3,
			Active:  wrapperspb.Bool(false),
			Count:   wrapperspb.Int32(0),
			Size:    wrapperspb.Int64(0),
			Score:   wrapperspb.Float(0.0),
			Rating:  wrapperspb.Double(0.0),
			Name:    wrapperspb.String(""),
			Payload: wrapperspb.Bytes([]byte{}),
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoMixed(t *testing.T) {
	type LogEntry struct {
		ID        int64                   `parquet:"id"`
		Level     string                  `parquet:"level"`
		Message   string                  `parquet:"message"`
		Timestamp *timestamppb.Timestamp  `parquet:"timestamp"`
		Duration  *durationpb.Duration    `parquet:"duration,optional"`
		UserID    *wrapperspb.Int64Value  `parquet:"user_id,optional"`
		SessionID *wrapperspb.StringValue `parquet:"session_id,optional"`
		Success   *wrapperspb.BoolValue   `parquet:"success,optional"`
		Tags      []string                `parquet:"tags,list"`
	}

	now := time.Now().Truncate(time.Microsecond)

	entries := []LogEntry{
		{
			ID:        1,
			Level:     "INFO",
			Message:   "User logged in",
			Timestamp: timestamppb.New(now),
			Duration:  durationpb.New(100 * time.Millisecond),
			UserID:    wrapperspb.Int64(12345),
			SessionID: wrapperspb.String("session-abc-123"),
			Success:   wrapperspb.Bool(true),
			Tags:      []string{"auth", "login"},
		},
		{
			ID:        2,
			Level:     "ERROR",
			Message:   "Database connection failed",
			Timestamp: timestamppb.New(now.Add(time.Second)),
			Duration:  nil,
			UserID:    nil,
			SessionID: nil,
			Success:   wrapperspb.Bool(false),
			Tags:      []string{"db", "error"},
		},
		{
			ID:        3,
			Level:     "DEBUG",
			Message:   "Cache hit",
			Timestamp: timestamppb.New(now.Add(2 * time.Second)),
			Duration:  durationpb.New(5 * time.Millisecond),
			UserID:    wrapperspb.Int64(67890),
			SessionID: wrapperspb.String("session-xyz-789"),
			Success:   wrapperspb.Bool(true),
			Tags:      []string{},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[LogEntry](buf)
	if _, err := writer.Write(entries); err != nil {
		t.Fatalf("failed to write entries: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[LogEntry](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readEntries := make([]LogEntry, len(entries))
	if _, err := reader.Read(readEntries); err != nil && err != io.EOF {
		t.Fatalf("failed to read entries: %v", err)
	}

	if !reflect.DeepEqual(entries, readEntries) {
		t.Errorf("entries mismatch:\nexpected: %+v\ngot: %+v", entries, readEntries)
	}
}

func TestProtoInNestedStruct(t *testing.T) {
	type Metadata struct {
		CreatedAt *timestamppb.Timestamp `parquet:"created_at"`
		UpdatedAt *timestamppb.Timestamp `parquet:"updated_at,optional"`
	}

	type Record struct {
		ID       int64     `parquet:"id"`
		Name     string    `parquet:"name"`
		Metadata *Metadata `parquet:"metadata,optional"`
	}

	now := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)
	later := now.Add(time.Hour)

	records := []Record{
		{
			ID:   1,
			Name: "record1",
			Metadata: &Metadata{
				CreatedAt: timestamppb.New(now),
				UpdatedAt: timestamppb.New(later),
			},
		},
		{
			ID:       2,
			Name:     "record2",
			Metadata: nil,
		},
		{
			ID:   3,
			Name: "record3",
			Metadata: &Metadata{
				CreatedAt: timestamppb.New(now),
				UpdatedAt: nil,
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoInMap(t *testing.T) {
	type Config struct {
		ID         int64                             `parquet:"id"`
		Name       string                            `parquet:"name"`
		Timestamps map[string]*timestamppb.Timestamp `parquet:"timestamps"`
	}

	now := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)

	configs := []Config{
		{
			ID:   1,
			Name: "config1",
			Timestamps: map[string]*timestamppb.Timestamp{
				"created": timestamppb.New(now),
				"updated": timestamppb.New(now.Add(time.Hour)),
			},
		},
		{
			ID:         2,
			Name:       "config2",
			Timestamps: map[string]*timestamppb.Timestamp{},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Config](buf)
	if _, err := writer.Write(configs); err != nil {
		t.Fatalf("failed to write configs: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Config](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readConfigs := make([]Config, len(configs))
	if _, err := reader.Read(readConfigs); err != nil && err != io.EOF {
		t.Fatalf("failed to read configs: %v", err)
	}

	if !reflect.DeepEqual(configs, readConfigs) {
		t.Errorf("configs mismatch:\nexpected: %+v\ngot: %+v", configs, readConfigs)
	}
}

func TestProtoMessageInterface(t *testing.T) {
	// Define Go struct schema with time.Time
	type Metadata struct {
		CreatedAt time.Time `parquet:"created_at"`
		UpdatedAt time.Time `parquet:"updated_at"`
	}

	type Record struct {
		ID       int64    `parquet:"id"`
		Name     string   `parquet:"name"`
		Metadata Metadata `parquet:"metadata"`
	}

	// Create schema from Go struct
	schema := SchemaOf(new(Record))

	// Define protobuf-based struct with proto.Message interface
	type ProtoRecord struct {
		ID       int64         `parquet:"id"`
		Name     string        `parquet:"name"`
		Metadata proto.Message `parquet:"metadata"`
	}

	now := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)
	later := now.Add(time.Hour)

	// Create protobuf data
	protoRecords := []ProtoRecord{
		{
			ID:   1,
			Name: "record1",
			Metadata: &testproto.ProtoMetadata{
				CreatedAt: timestamppb.New(now),
				UpdatedAt: timestamppb.New(later),
			},
		},
		{
			ID:   2,
			Name: "record2",
			Metadata: &testproto.ProtoMetadata{
				CreatedAt: timestamppb.New(now.Add(2 * time.Hour)),
				UpdatedAt: timestamppb.New(later.Add(2 * time.Hour)),
			},
		},
	}

	// Write protobuf data using Go-derived schema
	buf := new(bytes.Buffer)
	writer := NewGenericWriter[ProtoRecord](buf, schema)
	if _, err := writer.Write(protoRecords); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back as Go structs
	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(protoRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify data - build expected records from proto data
	expected := make([]Record, len(protoRecords))
	for i := range protoRecords {
		protoMeta := protoRecords[i].Metadata.(*testproto.ProtoMetadata)
		expected[i] = Record{
			ID:   protoRecords[i].ID,
			Name: protoRecords[i].Name,
			Metadata: Metadata{
				CreatedAt: protoMeta.CreatedAt.AsTime().Truncate(time.Microsecond),
				UpdatedAt: protoMeta.UpdatedAt.AsTime().Truncate(time.Microsecond),
			},
		}
		// Truncate read times for comparison
		readRecords[i].Metadata.CreatedAt = readRecords[i].Metadata.CreatedAt.Truncate(time.Microsecond)
		readRecords[i].Metadata.UpdatedAt = readRecords[i].Metadata.UpdatedAt.Truncate(time.Microsecond)
	}

	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoMessageInterfaceNested(t *testing.T) {
	// Define Go struct schema
	type InnerMetadata struct {
		CreatedAt time.Time `parquet:"created_at"`
		UpdatedAt time.Time `parquet:"updated_at"`
	}

	type OuterRecord struct {
		ID       int64          `parquet:"id"`
		Name     string         `parquet:"name"`
		Metadata *InnerMetadata `parquet:"metadata,optional"`
	}

	schema := SchemaOf(new(OuterRecord))

	// Define protobuf struct with proto.Message in optional nested field
	type ProtoOuterRecord struct {
		ID       int64         `parquet:"id"`
		Name     string        `parquet:"name"`
		Metadata proto.Message `parquet:"metadata,optional"`
	}

	now := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)

	protoRecords := []ProtoOuterRecord{
		{
			ID:   1,
			Name: "record1",
			Metadata: &testproto.ProtoMetadata{
				CreatedAt: timestamppb.New(now),
				UpdatedAt: timestamppb.New(now.Add(time.Hour)),
			},
		},
		{
			ID:       2,
			Name:     "record2",
			Metadata: nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[ProtoOuterRecord](buf, schema)
	if _, err := writer.Write(protoRecords); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[OuterRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]OuterRecord, len(protoRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Build expected records from proto data
	expected := make([]OuterRecord, len(protoRecords))
	for i := range protoRecords {
		expected[i].ID = protoRecords[i].ID
		expected[i].Name = protoRecords[i].Name
		if protoRecords[i].Metadata != nil {
			protoMeta := protoRecords[i].Metadata.(*testproto.ProtoMetadata)
			expected[i].Metadata = &InnerMetadata{
				CreatedAt: protoMeta.CreatedAt.AsTime().Truncate(time.Microsecond),
				UpdatedAt: protoMeta.UpdatedAt.AsTime().Truncate(time.Microsecond),
			}
			// Truncate read times for comparison
			if readRecords[i].Metadata != nil {
				readRecords[i].Metadata.CreatedAt = readRecords[i].Metadata.CreatedAt.Truncate(time.Microsecond)
				readRecords[i].Metadata.UpdatedAt = readRecords[i].Metadata.UpdatedAt.Truncate(time.Microsecond)
			}
		}
	}

	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoMessageInterfaceWithWrappers(t *testing.T) {
	// Define Go struct schema
	type Record struct {
		ID     int64  `parquet:"id"`
		Active bool   `parquet:"active,optional"`
		Count  int64  `parquet:"count,optional"`
		Label  string `parquet:"label,optional"`
	}

	schema := SchemaOf(new(Record))

	// Define protobuf struct with proto.Message
	type ProtoRecord struct {
		ID     int64         `parquet:"id"`
		Active proto.Message `parquet:"active,optional"`
		Count  proto.Message `parquet:"count,optional"`
		Label  proto.Message `parquet:"label,optional"`
	}

	protoRecords := []ProtoRecord{
		{
			ID:     1,
			Active: wrapperspb.Bool(true),
			Count:  wrapperspb.Int64(42),
			Label:  wrapperspb.String("test"),
		},
		{
			ID:     2,
			Active: nil,
			Count:  nil,
			Label:  nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[ProtoRecord](buf, schema)
	if _, err := writer.Write(protoRecords); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(protoRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Build expected records (nil wrapper values become zero values)
	expected := []Record{
		{ID: 1, Active: true, Count: 42, Label: "test"},
		{ID: 2, Active: false, Count: 0, Label: ""},
	}

	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoMessageInterfaceSimple(t *testing.T) {
	// Test that proto.Message interface with timestamp works with Go struct schema
	type SimpleRecord struct {
		ID        int64     `parquet:"id"`
		Timestamp time.Time `parquet:"timestamp"`
	}

	type ProtoSimpleRecord struct {
		ID        int64         `parquet:"id"`
		Timestamp proto.Message `parquet:"timestamp"`
	}

	schema := SchemaOf(new(SimpleRecord))
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// Write using proto.Message interface
	protoRecords := []ProtoSimpleRecord{
		{ID: 1, Timestamp: timestamppb.New(ts)},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[ProtoSimpleRecord](buf, schema)
	if _, err := writer.Write(protoRecords); err != nil {
		t.Fatalf("failed to write proto records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back as Go struct
	reader := NewGenericReader[SimpleRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]SimpleRecord, 1)
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []SimpleRecord{{ID: 1, Timestamp: ts}}
	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoDurationInterface(t *testing.T) {
	// Test that proto.Message interface with duration works
	type DurationRecord struct {
		ID       int64         `parquet:"id"`
		Duration time.Duration `parquet:"duration"`
	}

	type ProtoDurationRecord struct {
		ID       int64         `parquet:"id"`
		Duration proto.Message `parquet:"duration"`
	}

	schema := SchemaOf(new(DurationRecord))
	dur := 5 * time.Hour

	// Write using proto.Message interface
	protoRecords := []ProtoDurationRecord{
		{ID: 1, Duration: durationpb.New(dur)},
		{ID: 2, Duration: nil}, // Test nil handling
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[ProtoDurationRecord](buf, schema)
	if _, err := writer.Write(protoRecords); err != nil {
		t.Fatalf("failed to write proto records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back as Go struct
	reader := NewGenericReader[DurationRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]DurationRecord, len(protoRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []DurationRecord{
		{ID: 1, Duration: dur},
		{ID: 2, Duration: 0}, // Nil becomes zero value
	}
	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoStructInterface(t *testing.T) {
	// Test that proto.Message interface with structpb.Struct works
	type StructRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"` // structpb.Struct serializes to JSON bytes
	}

	type ProtoStructRecord struct {
		ID   int64         `parquet:"id"`
		Data proto.Message `parquet:"data"`
	}

	schema := SchemaOf(new(StructRecord))

	// Create a structpb.Struct
	s, err := structpb.NewStruct(map[string]any{
		"name":   "test",
		"count":  float64(42),
		"active": true,
	})
	if err != nil {
		t.Fatalf("failed to create struct: %v", err)
	}

	// Write using proto.Message interface
	protoRecords := []ProtoStructRecord{
		{ID: 1, Data: s},
		{ID: 2, Data: nil}, // Test nil handling
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[ProtoStructRecord](buf, schema)
	if _, err := writer.Write(protoRecords); err != nil {
		t.Fatalf("failed to write proto records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back as Go struct
	reader := NewGenericReader[StructRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]StructRecord, len(protoRecords))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Build expected records
	expectedData, err := json.Marshal(map[string]any{
		"name":   "test",
		"count":  float64(42),
		"active": true,
	})
	if err != nil {
		t.Fatalf("failed to marshal expected JSON: %v", err)
	}

	expected := []StructRecord{
		{ID: 1, Data: expectedData},
		{ID: 2, Data: []byte{}},
	}

	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot:  %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoTimestampPhysicalTypes(t *testing.T) {
	// Test different physical type representations for timestamps
	ts := time.Date(2024, 1, 1, 12, 0, 0, 123456789, time.UTC)
	tsProto := timestamppb.New(ts)

	t.Run("Millis", func(t *testing.T) {
		// Create schema with INT64 and TIMESTAMP(MILLIS) logical type
		schema := NewSchema("test", Group{
			"timestamp": Timestamp(Millisecond),
		})

		type Record struct {
			Timestamp proto.Message `parquet:"timestamp"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Timestamp: tsProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		// Read back as int64 (milliseconds since epoch)
		type ReadRecord struct {
			Timestamp int64 `parquet:"timestamp"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Timestamp: ts.UnixMilli()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("Micros", func(t *testing.T) {
		// Create schema with INT64 and TIMESTAMP(MICROS) logical type
		schema := NewSchema("test", Group{
			"timestamp": Timestamp(Microsecond),
		})

		type Record struct {
			Timestamp proto.Message `parquet:"timestamp"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Timestamp: tsProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		// Read back as int64 (microseconds since epoch)
		type ReadRecord struct {
			Timestamp int64 `parquet:"timestamp"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Timestamp: ts.UnixMicro()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("Nanos", func(t *testing.T) {
		// Create schema with INT64 and TIMESTAMP(NANOS) logical type
		schema := NewSchema("test", Group{
			"timestamp": Timestamp(Nanosecond),
		})

		type Record struct {
			Timestamp proto.Message `parquet:"timestamp"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Timestamp: tsProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		// Read back as int64 (nanoseconds since epoch)
		type ReadRecord struct {
			Timestamp int64 `parquet:"timestamp"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Timestamp: ts.UnixNano()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("Double", func(t *testing.T) {
		// Create schema with DOUBLE physical type
		schema := NewSchema("test", Group{
			"timestamp": Leaf(DoubleType),
		})

		type Record struct {
			Timestamp proto.Message `parquet:"timestamp"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Timestamp: tsProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		// Read back as float64 (seconds since epoch)
		type ReadRecord struct {
			Timestamp float64 `parquet:"timestamp"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Timestamp: ts.Sub(time.Unix(0, 0)).Seconds()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("ByteArray", func(t *testing.T) {
		// Create schema with BYTE_ARRAY physical type
		schema := NewSchema("test", Group{
			"timestamp": Leaf(ByteArrayType),
		})

		type Record struct {
			Timestamp proto.Message `parquet:"timestamp"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Timestamp: tsProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		// Read back as string (RFC3339Nano format)
		type ReadRecord struct {
			Timestamp string `parquet:"timestamp"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Timestamp: ts.Format(time.RFC3339Nano)}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("Nil", func(t *testing.T) {
		type Record struct {
			Timestamp *timestamppb.Timestamp `parquet:"timestamp,optional"`
		}

		type ReadRecord struct {
			Timestamp *int64 `parquet:"timestamp,optional"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf)
		if _, err := writer.Write([]Record{{Timestamp: nil}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer failed: %v", err)
		}

		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}
		reader.Close()

		expected := []ReadRecord{{Timestamp: nil}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})
}

func TestProtoDurationPhysicalTypes(t *testing.T) {
	// Test different physical type representations for durations
	dur := 5*time.Hour + 30*time.Minute
	durProto := durationpb.New(dur)

	t.Run("Int64", func(t *testing.T) {
		// Default: INT64 stores nanoseconds
		schema := NewSchema("test", Group{
			"duration": Leaf(Int64Type),
		})

		type Record struct {
			Duration proto.Message `parquet:"duration"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Duration: durProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		type ReadRecord struct {
			Duration int64 `parquet:"duration"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Duration: dur.Nanoseconds()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("Double", func(t *testing.T) {
		// DOUBLE stores seconds
		schema := NewSchema("test", Group{
			"duration": Leaf(DoubleType),
		})

		type Record struct {
			Duration proto.Message `parquet:"duration"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Duration: durProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		type ReadRecord struct {
			Duration float64 `parquet:"duration"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Duration: dur.Seconds()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("ByteArray", func(t *testing.T) {
		// BYTE_ARRAY stores string representation
		schema := NewSchema("test", Group{
			"duration": Leaf(ByteArrayType),
		})

		type Record struct {
			Duration proto.Message `parquet:"duration"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf, schema)
		if _, err := writer.Write([]Record{{Duration: durProto}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		writer.Close()

		type ReadRecord struct {
			Duration string `parquet:"duration"`
		}
		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		expected := []ReadRecord{{Duration: dur.String()}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})

	t.Run("Nil", func(t *testing.T) {
		type Record struct {
			Duration *durationpb.Duration `parquet:"duration,optional"`
		}

		type ReadRecord struct {
			Duration *int64 `parquet:"duration,optional"`
		}

		buf := new(bytes.Buffer)
		writer := NewGenericWriter[Record](buf)
		if _, err := writer.Write([]Record{{Duration: nil}}); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer failed: %v", err)
		}

		reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
		records := make([]ReadRecord, 1)
		if _, err := reader.Read(records); err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}
		reader.Close()

		expected := []ReadRecord{{Duration: nil}}
		if !reflect.DeepEqual(records, expected) {
			t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", records, expected)
		}
	})
}

func TestProtoWithRepeated(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	later := now.Add(1 * time.Hour)
	dur1 := 5 * time.Second
	dur2 := 10 * time.Minute

	records := []*testproto.ProtoWithRepeated{
		{
			Id: 1,
			Timestamps: []*timestamppb.Timestamp{
				timestamppb.New(now),
				timestamppb.New(later),
			},
			Durations: []*durationpb.Duration{
				durationpb.New(dur1),
				durationpb.New(dur2),
			},
		},
		{
			Id:         2,
			Timestamps: []*timestamppb.Timestamp{},
			Durations:  []*durationpb.Duration{},
		},
		{
			Id: 3,
			Timestamps: []*timestamppb.Timestamp{
				timestamppb.New(now),
			},
			Durations: []*durationpb.Duration{
				durationpb.New(dur1),
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[*testproto.ProtoWithRepeated](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[*testproto.ProtoWithRepeated](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]*testproto.ProtoWithRepeated, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoWithMap(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	later := now.Add(1 * time.Hour)

	records := []*testproto.ProtoWithMap{
		{
			Id: 1,
			TimestampMap: map[string]*timestamppb.Timestamp{
				"created": timestamppb.New(now),
				"updated": timestamppb.New(later),
			},
			StringMap: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			Id:           2,
			TimestampMap: map[string]*timestamppb.Timestamp{},
			StringMap:    map[string]string{},
		},
		{
			Id: 3,
			TimestampMap: map[string]*timestamppb.Timestamp{
				"single": timestamppb.New(now),
			},
			StringMap: map[string]string{
				"only": "one",
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[*testproto.ProtoWithMap](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[*testproto.ProtoWithMap](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]*testproto.ProtoWithMap, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoEvent(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	dur := 5 * time.Minute

	records := []*testproto.ProtoEvent{
		{
			Id:        1,
			Name:      "event1",
			Timestamp: timestamppb.New(now),
			Duration:  durationpb.New(dur),
		},
		{
			Id:        2,
			Name:      "event2",
			Timestamp: timestamppb.New(now.Add(1 * time.Hour)),
			Duration:  durationpb.New(10 * time.Second),
		},
		{
			Id:        3,
			Name:      "event3",
			Timestamp: nil,
			Duration:  nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[*testproto.ProtoEvent](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[*testproto.ProtoEvent](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]*testproto.ProtoEvent, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoNested(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	later := now.Add(1 * time.Hour)

	records := []*testproto.ProtoNested{
		{
			Id:   1,
			Name: "nested1",
			Metadata: &testproto.ProtoMetadata{
				CreatedAt: timestamppb.New(now),
				UpdatedAt: timestamppb.New(later),
			},
		},
		{
			Id:   2,
			Name: "nested2",
			Metadata: &testproto.ProtoMetadata{
				CreatedAt: timestamppb.New(later),
				UpdatedAt: timestamppb.New(now),
			},
		},
		{
			Id:       3,
			Name:     "nested3",
			Metadata: nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[*testproto.ProtoNested](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[*testproto.ProtoNested](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]*testproto.ProtoNested, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoUnsignedWrappers(t *testing.T) {
	type Record struct {
		ID      int64                   `parquet:"id"`
		Count32 *wrapperspb.UInt32Value `parquet:"count32,optional"`
		Count64 *wrapperspb.UInt64Value `parquet:"count64,optional"`
	}

	records := []Record{
		{
			ID:      1,
			Count32: wrapperspb.UInt32(42),
			Count64: wrapperspb.UInt64(9999999999),
		},
		{
			ID:      2,
			Count32: wrapperspb.UInt32(0),
			Count64: wrapperspb.UInt64(0),
		},
		{
			ID:      3,
			Count32: nil,
			Count64: nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoRepeatedNested(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	later := now.Add(1 * time.Hour)

	type Record struct {
		ID       int64                      `parquet:"id"`
		Name     string                     `parquet:"name"`
		Metadata []*testproto.ProtoMetadata `parquet:"metadata"`
	}

	records := []Record{
		{
			ID:   1,
			Name: "record1",
			Metadata: []*testproto.ProtoMetadata{
				{
					CreatedAt: timestamppb.New(now),
					UpdatedAt: timestamppb.New(later),
				},
				{
					CreatedAt: timestamppb.New(later),
					UpdatedAt: timestamppb.New(now),
				},
			},
		},
		{
			ID:       2,
			Name:     "record2",
			Metadata: []*testproto.ProtoMetadata{},
		},
		{
			ID:   3,
			Name: "record3",
			Metadata: []*testproto.ProtoMetadata{
				{
					CreatedAt: timestamppb.New(now),
					UpdatedAt: timestamppb.New(now),
				},
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoMapNested(t *testing.T) {
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	later := now.Add(1 * time.Hour)

	type Record struct {
		ID       int64                               `parquet:"id"`
		Name     string                              `parquet:"name"`
		Metadata map[string]*testproto.ProtoMetadata `parquet:"metadata"`
	}

	records := []Record{
		{
			ID:   1,
			Name: "record1",
			Metadata: map[string]*testproto.ProtoMetadata{
				"first": {
					CreatedAt: timestamppb.New(now),
					UpdatedAt: timestamppb.New(later),
				},
				"second": {
					CreatedAt: timestamppb.New(later),
					UpdatedAt: timestamppb.New(now),
				},
			},
		},
		{
			ID:       2,
			Name:     "record2",
			Metadata: map[string]*testproto.ProtoMetadata{},
		},
		{
			ID:   3,
			Name: "record3",
			Metadata: map[string]*testproto.ProtoMetadata{
				"only": {
					CreatedAt: timestamppb.New(now),
					UpdatedAt: timestamppb.New(now),
				},
			},
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	if !reflect.DeepEqual(records, readRecords) {
		t.Errorf("records mismatch:\nexpected: %+v\ngot: %+v", records, readRecords)
	}
}

func TestProtoAnyNestedGroup(t *testing.T) {
	if purego {
		t.Skip("anypb.Any requires protoreflect, not available in purego mode")
	}
	// Test Any field that maps to a nested group structure
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)

	// Create a ProtoPayload to pack into Any
	payload := &testproto.ProtoPayload{
		Id:        42,
		Message:   "test message",
		CreatedAt: timestamppb.New(now),
	}

	// Pack into Any
	anyPayload, err := anypb.New(payload)
	if err != nil {
		t.Fatalf("failed to pack Any: %v", err)
	}

	// Create schema with nested group matching the type_url path
	// type.googleapis.com/testproto.ProtoPayload -> testproto/ProtoPayload
	schema := NewSchema("test", Group{
		"id":   Leaf(Int64Type),
		"name": String(),
		"payload": Group{
			"testproto": Group{
				"ProtoPayload": Group{
					"id":         Leaf(Int64Type),
					"message":    String(),
					"created_at": Timestamp(Microsecond),
				},
			},
		},
	})

	type Record struct {
		ID      int64         `parquet:"id"`
		Name    string        `parquet:"name"`
		Payload proto.Message `parquet:"payload"`
	}

	records := []Record{
		{
			ID:      1,
			Name:    "record1",
			Payload: anyPayload,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Verify we can read the data back
	// The read schema must match the nested structure
	type ReadProtoPayload struct {
		ID        int64     `parquet:"id"`
		Message   string    `parquet:"message"`
		CreatedAt time.Time `parquet:"created_at"`
	}

	type ReadProtoPayloadWrapper struct {
		ProtoPayload ReadProtoPayload `parquet:"ProtoPayload"`
	}

	type ReadPayload struct {
		Testproto ReadProtoPayloadWrapper `parquet:"testproto"`
	}

	type ReadRecord struct {
		ID      int64        `parquet:"id"`
		Name    string       `parquet:"name"`
		Payload *ReadPayload `parquet:"payload"`
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify the data
	expected := []ReadRecord{
		{
			ID:   1,
			Name: "record1",
			Payload: &ReadPayload{
				Testproto: ReadProtoPayloadWrapper{
					ProtoPayload: ReadProtoPayload{
						ID:        42,
						Message:   "test message",
						CreatedAt: now.Truncate(time.Microsecond),
					},
				},
			},
		},
	}

	// Truncate read time for comparison
	if readRecords[0].Payload != nil {
		readRecords[0].Payload.Testproto.ProtoPayload.CreatedAt =
			readRecords[0].Payload.Testproto.ProtoPayload.CreatedAt.Truncate(time.Microsecond)
	}

	if !reflect.DeepEqual(readRecords, expected) {
		t.Errorf("records mismatch:\ngot: %+v\nwant: %+v", readRecords, expected)
	}
}

func TestProtoAnyLeafColumn(t *testing.T) {
	if purego {
		t.Skip("anypb.Any requires protoreflect, not available in purego mode")
	}
	// Test Any field on a leaf column (should write as marshaled proto)
	payload := &testproto.ProtoPayload{
		Id:      42,
		Message: "test message",
	}

	anyPayload, err := anypb.New(payload)
	if err != nil {
		t.Fatalf("failed to pack Any: %v", err)
	}

	// Schema with Any as a byte array leaf
	schema := NewSchema("test", Group{
		"id":   Leaf(Int64Type),
		"data": Leaf(ByteArrayType),
	})

	type Record struct {
		ID   int64         `parquet:"id"`
		Data proto.Message `parquet:"data"`
	}

	records := []Record{
		{
			ID:   1,
			Data: anyPayload,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back as byte array
	type ReadRecord struct {
		ID   int64  `parquet:"id"`
		Data []byte `parquet:"data"`
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify we can unmarshal it back to an Any
	var readAny anypb.Any
	if err := proto.Unmarshal(readRecords[0].Data, &readAny); err != nil {
		t.Fatalf("failed to unmarshal Any: %v", err)
	}
	if !proto.Equal(&readAny, anyPayload) {
		t.Errorf("Any mismatch:\ngot: %v\nwant: %v", &readAny, anyPayload)
	}
}

func TestProtoAnyNil(t *testing.T) {
	if purego {
		t.Skip("anypb.Any requires protoreflect, not available in purego mode")
	}
	// Test nil Any field writes null
	schema := NewSchema("test", Group{
		"id": Leaf(Int64Type),
		"payload": Group{
			"testproto": Group{
				"ProtoPayload": Group{
					"id":      Leaf(Int64Type),
					"message": String(),
				},
			},
		},
	})

	type Record struct {
		ID      int64         `parquet:"id"`
		Payload proto.Message `parquet:"payload"`
	}

	records := []Record{
		{
			ID:      1,
			Payload: nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf, schema)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back
	type ReadPayload struct {
		ID      int64  `parquet:"id"`
		Message string `parquet:"message"`
	}

	type ReadRecord struct {
		ID      int64        `parquet:"id"`
		Payload *ReadPayload `parquet:"payload"`
	}

	reader := NewGenericReader[ReadRecord](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]ReadRecord, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify payload is nil
	if readRecords[0].Payload != nil {
		t.Errorf("expected nil payload, got: %+v", readRecords[0].Payload)
	}
}

func TestProtoAnyInvalidTypeURL(t *testing.T) {
	if purego {
		t.Skip("anypb.Any requires protoreflect, not available in purego mode")
	}
	// Test that invalid type_url prefix causes panic when Any is in a group
	// (not a leaf column)
	payload := &testproto.ProtoPayload{
		Id:      42,
		Message: "test",
	}

	anyPayload, err := anypb.New(payload)
	if err != nil {
		t.Fatalf("failed to pack Any: %v", err)
	}

	// Modify the type_url to be invalid
	anyPayload.TypeUrl = "invalid.prefix/testproto.ProtoPayload"

	schema := NewSchema("test", Group{
		"id": Leaf(Int64Type),
		"payload": Group{
			"testproto": Group{
				"ProtoPayload": Group{
					"id": Leaf(Int64Type),
				},
			},
		},
	})

	type Record struct {
		ID      int64         `parquet:"id"`
		Payload proto.Message `parquet:"payload"`
	}

	records := []Record{
		{
			ID:      1,
			Payload: anyPayload,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf, schema)

	// Should panic with invalid type_url
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for invalid type_url, but didn't panic")
		}
	}()

	writer.Write(records)
}

func TestProtoAnyPathMismatch(t *testing.T) {
	if purego {
		t.Skip("anypb.Any requires protoreflect, not available in purego mode")
	}
	// Test that path mismatch causes panic
	payload := &testproto.ProtoPayload{
		Id:      42,
		Message: "test",
	}

	anyPayload, err := anypb.New(payload)
	if err != nil {
		t.Fatalf("failed to pack Any: %v", err)
	}

	// Schema with wrong path structure (missing intermediate groups)
	schema := NewSchema("test", Group{
		"id": Leaf(Int64Type),
		"payload": Group{
			"wrong": Group{
				"path": Leaf(Int64Type),
			},
		},
	})

	type Record struct {
		ID      int64         `parquet:"id"`
		Payload proto.Message `parquet:"payload"`
	}

	records := []Record{
		{
			ID:      1,
			Payload: anyPayload,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[Record](buf, schema)

	// Should panic with path mismatch
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for path mismatch, but didn't panic")
		}
	}()

	writer.Write(records)
}

func TestProtoAnyOptional(t *testing.T) {
	if purego {
		t.Skip("anypb.Any requires protoreflect, not available in purego mode")
	}
	// Test optional Any fields
	now := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)

	payload1 := &testproto.ProtoPayload{
		Id:        1,
		Message:   "first",
		CreatedAt: timestamppb.New(now),
	}

	anyPayload1, err := anypb.New(payload1)
	if err != nil {
		t.Fatalf("failed to pack Any: %v", err)
	}

	records := []*testproto.ProtoWithOptionalAny{
		{
			Id:   1,
			Data: anyPayload1,
		},
		{
			Id:   2,
			Data: nil,
		},
	}

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[*testproto.ProtoWithOptionalAny](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	reader := NewGenericReader[*testproto.ProtoWithOptionalAny](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]*testproto.ProtoWithOptionalAny, len(records))
	if _, err := reader.Read(readRecords); err != nil && err != io.EOF {
		t.Fatalf("failed to read records: %v", err)
	}

	// Check first record has data, second is nil
	if readRecords[0].Data == nil {
		t.Error("expected first record to have data")
	}
	if readRecords[1].Data != nil {
		t.Errorf("expected second record to have nil data, got: %+v", readRecords[1].Data)
	}
}
