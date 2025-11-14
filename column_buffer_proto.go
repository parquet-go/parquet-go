package parquet

import (
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go/format"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func writeProtoTimestamp(col ColumnBuffer, levels columnLevels, ts *timestamppb.Timestamp, node Node) {
	if ts == nil {
		col.writeNull(levels)
		return
	}
	var typ = node.Type()
	var unit format.TimeUnit
	if lt := typ.LogicalType(); lt != nil && lt.Timestamp != nil {
		unit = lt.Timestamp.Unit
	} else {
		unit = Nanosecond.TimeUnit()
	}
	var t = ts.AsTime()
	var value int64
	switch {
	case unit.Millis != nil:
		value = t.UnixMilli()
	case unit.Micros != nil:
		value = t.UnixMicro()
	default:
		value = t.UnixNano()
	}
	switch kind := typ.Kind(); kind {
	case Int32, Int64:
		col.writeInt64(levels, value)
	case Float, Double:
		col.writeDouble(levels, t.Sub(time.Unix(0, 0)).Seconds())
	case ByteArray:
		col.writeByteArray(levels, t.AppendFormat(nil, time.RFC3339Nano))
	default:
		panic(fmt.Sprintf("unsupported physical type for timestamp: %v", kind))
	}
}

func writeProtoDuration(col ColumnBuffer, levels columnLevels, dur *durationpb.Duration, node Node) {
	if dur == nil {
		col.writeNull(levels)
		return
	}
	d := dur.AsDuration()
	switch kind := node.Type().Kind(); kind {
	case Int32, Int64:
		col.writeInt64(levels, d.Nanoseconds())
	case Float, Double:
		col.writeDouble(levels, d.Seconds())
	case ByteArray:
		col.writeByteArray(levels, unsafeByteArrayFromString(d.String()))
	default:
		panic(fmt.Sprintf("unsupported physical type for duration: %v", kind))
	}
}

func writeProtoStruct(col ColumnBuffer, levels columnLevels, s *structpb.Struct, node Node) {
	data, err := s.MarshalJSON()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal structpb.Struct: %v", err))
	}
	col.writeByteArray(levels, data)
}
