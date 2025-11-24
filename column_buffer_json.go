package parquet

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

func writeJSONToLeaf(col ColumnBuffer, levels columnLevels, val *jsonValue, node Node) {
	switch typ := node.Type(); val.kind() {
	case jsonNull:
		col.writeNull(levels)

	case jsonTrue, jsonFalse:
		if kind := typ.Kind(); kind != Boolean {
			panic(fmt.Errorf("cannot write JSON boolean to column with type %v", kind))
		}
		col.writeBoolean(levels, val.kind() == jsonTrue)

	case jsonNumber:
		if logicalType := typ.LogicalType(); logicalType != nil {
			switch {
			case logicalType.Timestamp != nil:
				// Interpret number as seconds since Unix epoch (with sub-second precision)
				num := val.float()
				sec, frac := math.Modf(num)
				t := time.Unix(int64(sec), int64(frac*1e9)).UTC()
				writeTime(col, levels, t, node)
				return

			case logicalType.Date != nil:
				// Interpret number as seconds since Unix epoch
				t := time.Unix(val.int(), 0).UTC()
				writeTime(col, levels, t, node)
				return

			case logicalType.Time != nil:
				// Interpret number as seconds since midnight
				num := val.float()
				d := time.Duration(num * float64(time.Second))
				writeDuration(col, levels, d, node)
				return
			}
		}

		switch kind := typ.Kind(); kind {
		case Boolean:
			col.writeBoolean(levels, val.float() != 0)
		case Int32, Int64:
			switch val.number() {
			case jsonNumberTypeInt:
				col.writeInt64(levels, val.int())
			case jsonNumberTypeUint:
				col.writeInt64(levels, int64(val.uint()))
			case jsonNumberTypeFloat:
				col.writeDouble(levels, val.float())
			}
		case Float, Double:
			col.writeDouble(levels, val.float())
		default:
			col.writeByteArray(levels, unsafeByteArrayFromString(val.string()))
		}

	case jsonString:
		str := val.string()

		if logicalType := typ.LogicalType(); logicalType != nil {
			switch {
			case logicalType.Timestamp != nil:
				t, err := time.Parse(time.RFC3339, str)
				if err != nil {
					panic(fmt.Errorf("cannot parse JSON string %q as timestamp: %w", str, err))
				}
				writeTime(col, levels, t, node)
				return

			case logicalType.Date != nil:
				t, err := time.Parse("2006-01-02", str)
				if err != nil {
					panic(fmt.Errorf("cannot parse JSON string %q as date: %w", str, err))
				}
				writeTime(col, levels, t, node)
				return

			case logicalType.Time != nil:
				t, err := time.Parse("15:04:05.000000000", str)
				if err != nil {
					panic(fmt.Errorf("cannot parse JSON string %q as time: %w", str, err))
				}
				d := time.Duration(t.Hour())*time.Hour +
					time.Duration(t.Minute())*time.Minute +
					time.Duration(t.Second())*time.Second +
					time.Duration(t.Nanosecond())*time.Nanosecond
				writeDuration(col, levels, d, node)
				return
			}
		}

		col.writeByteArray(levels, unsafeByteArrayFromString(str))

	default:
		// Nested objects/arrays shouldn't appear in leaf nodes, but if they do,
		// serialize back to JSON bytes for BYTE_ARRAY columns
		buf := buffers.get(256)
		buf.data = jsonFormat(buf.data[:0], val)
		col.writeByteArray(levels, buf.data)
		buf.unref()
	}
}

func writeJSONToGroup(columns []ColumnBuffer, levels columnLevels, val *jsonValue, node Node, writers []fieldWriter) {

	if val.kind() != jsonObject {
		for i := range writers {
			w := &writers[i]
			w.writeValue(columns, levels, reflect.Value{})
		}
		return
	}

	for i := range writers {
		w := &writers[i]
		f := val.lookup(w.fieldName)
		if f == nil {
			w.writeValue(columns, levels, reflect.Value{})
		} else {
			w.writeValue(columns, levels, reflect.ValueOf(f))
		}
	}
}

func writeJSONToRepeated(columns []ColumnBuffer, levels columnLevels, val *jsonValue, elementWriter writeValueFunc) {
	if val.kind() == jsonArray {
		array := val.array()
		if len(array) == 0 {
			elementWriter(columns, levels, reflect.Value{})
			return
		}

		levels.repetitionDepth++
		levels.definitionLevel++

		for i := range array {
			elementWriter(columns, levels, reflect.ValueOf(&array[i]))
			levels.repetitionLevel = levels.repetitionDepth
		}
		return
	}

	// Auto-wrap scalar to single-element array
	levels.repetitionDepth++
	levels.definitionLevel++
	elementWriter(columns, levels, reflect.ValueOf(val))
}

func writeJSONNumber(col ColumnBuffer, levels columnLevels, num json.Number, node Node) {
	typ := node.Type()
	str := num.String()

	switch kind := typ.Kind(); kind {
	case Boolean:
		switch jsonNumberTypeOf(str) {
		case jsonNumberTypeInt:
			i, err := num.Int64()
			if err != nil {
				panic(fmt.Errorf("cannot convert json.Number %q to boolean: %w", num, err))
			}
			col.writeBoolean(levels, i != 0)
		case jsonNumberTypeUint:
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				panic(fmt.Errorf("cannot convert json.Number %q to boolean: %w", num, err))
			}
			col.writeBoolean(levels, u != 0)
		case jsonNumberTypeFloat:
			f, err := num.Float64()
			if err != nil {
				panic(fmt.Errorf("cannot convert json.Number %q to boolean: %w", num, err))
			}
			col.writeBoolean(levels, f != 0)
		}

	case Int32, Int64:
		switch jsonNumberTypeOf(str) {
		case jsonNumberTypeInt:
			i, err := num.Int64()
			if err != nil {
				panic(fmt.Errorf("cannot convert json.Number %q to int: %w", num, err))
			}
			col.writeInt64(levels, i)
		case jsonNumberTypeUint:
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				panic(fmt.Errorf("cannot convert json.Number %q to int: %w", num, err))
			}
			col.writeInt64(levels, int64(u))
		case jsonNumberTypeFloat:
			f, err := num.Float64()
			if err != nil {
				panic(fmt.Errorf("cannot convert json.Number %q to float: %w", num, err))
			}
			col.writeInt64(levels, int64(f))
		}

	case Float, Double:
		f, err := num.Float64()
		if err != nil {
			panic(fmt.Errorf("cannot convert json.Number %q to float64: %w", num, err))
		}
		col.writeDouble(levels, f)

	default:
		col.writeByteArray(levels, unsafeByteArrayFromString(str))
	}
}
