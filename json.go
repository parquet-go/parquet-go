package parquet

import (
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

var jsonNumberSentinel struct{}

const (
	jsonKindShift = 60
	jsonKindMask  = (1 << jsonKindShift) - 1
)

type jsonKind int

const (
	jsonNull jsonKind = iota
	jsonTrue
	jsonFalse
	jsonNumber
	jsonString
	jsonObject
	jsonArray
)

type jsonField struct {
	key string
	val jsonValue
}

type jsonValue struct {
	ptr unsafe.Pointer
	u64 uint64
}

func (v *jsonValue) kind() jsonKind {
	if v.ptr == unsafe.Pointer(&jsonNumberSentinel) {
		return jsonNumber
	}
	return jsonKind(v.u64 >> jsonKindShift)
}

func (v *jsonValue) len() int {
	return int(v.u64 & jsonKindMask)
}

func (v *jsonValue) float() float64 {
	return math.Float64frombits(v.u64)
}

func (v *jsonValue) string() string {
	return unsafe.String((*byte)(v.ptr), v.len())
}

func (v *jsonValue) bytes() []byte {
	return unsafe.Slice((*byte)(v.ptr), v.len())
}

func (v *jsonValue) array() []jsonValue {
	return unsafe.Slice((*jsonValue)(v.ptr), v.len())
}

func (v *jsonValue) object() []jsonField {
	return unsafe.Slice((*jsonField)(v.ptr), v.len())
}

func (v *jsonValue) lookup(k string) *jsonValue {
	fields := v.object()
	i, ok := slices.BinarySearchFunc(fields, k, func(a jsonField, b string) int {
		return strings.Compare(a.key, b)
	})
	if ok {
		return &fields[i].val
	}
	return nil
}

func jsonNullValue() jsonValue {
	return jsonValue{u64: uint64(jsonNull)}
}

func jsonTrueValue() jsonValue {
	return jsonValue{u64: uint64(jsonTrue)<<jsonKindShift | 1}
}

func jsonFalseValue() jsonValue {
	return jsonValue{u64: uint64(jsonFalse)<<jsonKindShift | 0}
}

func jsonBoolValue(b bool) jsonValue {
	if b {
		return jsonTrueValue()
	} else {
		return jsonFalseValue()
	}
}

func jsonNumberValue(f float64) jsonValue {
	return jsonValue{
		ptr: unsafe.Pointer(&jsonNumberSentinel),
		u64: math.Float64bits(f),
	}
}

func jsonStringValue(s string) jsonValue {
	return jsonValue{
		u64: (uint64(jsonString) << jsonKindShift) | uint64(len(s)),
		ptr: unsafe.Pointer(unsafe.StringData(s)),
	}
}

func jsonArrayValue(elements []jsonValue) jsonValue {
	return jsonValue{
		u64: (uint64(jsonArray) << jsonKindShift) | uint64(len(elements)),
		ptr: unsafe.Pointer(unsafe.SliceData(elements)),
	}
}

func jsonObjectValue(fields []jsonField) jsonValue {
	return jsonValue{
		u64: (uint64(jsonObject) << jsonKindShift) | uint64(len(fields)),
		ptr: unsafe.Pointer(unsafe.SliceData(fields)),
	}
}

func jsonParse(data []byte) (*jsonValue, error) {
	if len(data) == 0 {
		v := jsonNullValue()
		return &v, nil
	}
	i := jsoniter.ParseBytes(jsoniter.ConfigFastest, data)
	v := jsonParseIterator(i)
	if i.Error != nil && i.Error != io.EOF {
		return nil, i.Error
	}
	return &v, nil
}

func jsonParseIterator(iter *jsoniter.Iterator) jsonValue {
	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.ReadNil()
		return jsonNullValue()

	case jsoniter.BoolValue:
		return jsonBoolValue(iter.ReadBool())

	case jsoniter.NumberValue:
		return jsonNumberValue(iter.ReadFloat64())

	case jsoniter.StringValue:
		return jsonStringValue(iter.ReadString())

	case jsoniter.ArrayValue:
		elements := make([]jsonValue, 0, 8)
		for iter.ReadArray() {
			elements = append(elements, jsonParseIterator(iter))
		}
		return jsonArrayValue(elements)

	case jsoniter.ObjectValue:
		fields := make([]jsonField, 0, 8)
		for key := iter.ReadObject(); key != ""; key = iter.ReadObject() {
			fields = append(fields, jsonField{key: key, val: jsonParseIterator(iter)})
		}
		slices.SortFunc(fields, func(a, b jsonField) int {
			return strings.Compare(a.key, b.key)
		})
		return jsonObjectValue(fields)

	default:
		return jsonNullValue()
	}
}

func jsonFormat(buf []byte, val *jsonValue) []byte {
	switch val.kind() {
	case jsonNull:
		return append(buf, "null"...)

	case jsonTrue:
		return append(buf, "true"...)

	case jsonFalse:
		return append(buf, "false"...)

	case jsonNumber:
		return strconv.AppendFloat(buf, val.float(), 'g', -1, 64)

	case jsonString:
		return strconv.AppendQuote(buf, val.string())

	case jsonArray:
		buf = append(buf, '[')
		array := val.array()
		for i := range array {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = jsonFormat(buf, &array[i])
		}
		return append(buf, ']')

	case jsonObject:
		buf = append(buf, '{')
		fields := val.object()
		for i := range fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendQuote(buf, fields[i].key)
			buf = append(buf, ':')
			buf = jsonFormat(buf, &fields[i].val)
		}
		return append(buf, '}')

	default:
		return buf
	}
}
