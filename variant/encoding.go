package variant

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/google/uuid"
)

// Encode encodes a variant Value into its binary representation.
// Field names for objects are registered with the provided MetadataBuilder.
// Returns the encoded value bytes.
func Encode(b *MetadataBuilder, v Value) []byte {
	switch v.basic {
	case BasicObject:
		enc := encoder{
			b:       b,
			scratch: make([]byte, 0, 512),
		}
		start, end, err := enc.encodeValueObject(v.object)
		if err != nil {
			return nil
		}
		res := make([]byte, end-start)
		copy(res, enc.scratch[start:end])
		return res
	case BasicArray:
		enc := encoder{
			b:       b,
			scratch: make([]byte, 0, 512),
		}
		start, end, err := enc.encodeValueArray(v.array)
		if err != nil {
			return nil
		}
		res := make([]byte, end-start)
		copy(res, enc.scratch[start:end])
		return res
	default:
		return encodePrimitive(v)
	}
}

func encodePrimitive(v Value) []byte {
	switch v.primitive {
	case PrimitiveNull:
		return []byte{makeHeader(BasicPrimitive, byte(PrimitiveNull))}
	case PrimitiveTrue:
		return []byte{makeHeader(BasicPrimitive, byte(PrimitiveTrue))}
	case PrimitiveFalse:
		return []byte{makeHeader(BasicPrimitive, byte(PrimitiveFalse))}
	case PrimitiveInt8:
		return []byte{makeHeader(BasicPrimitive, byte(PrimitiveInt8)), byte(v.i64)}
	case PrimitiveInt16:
		buf := make([]byte, 3)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveInt16))
		binary.LittleEndian.PutUint16(buf[1:], uint16(v.i64))
		return buf
	case PrimitiveInt32:
		buf := make([]byte, 5)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveInt32))
		binary.LittleEndian.PutUint32(buf[1:], uint32(v.i64))
		return buf
	case PrimitiveInt64:
		buf := make([]byte, 9)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveInt64))
		binary.LittleEndian.PutUint64(buf[1:], uint64(v.i64))
		return buf
	case PrimitiveFloat:
		buf := make([]byte, 5)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveFloat))
		binary.LittleEndian.PutUint32(buf[1:], math.Float32bits(float32(v.f64)))
		return buf
	case PrimitiveDouble:
		buf := make([]byte, 9)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveDouble))
		binary.LittleEndian.PutUint64(buf[1:], math.Float64bits(v.f64))
		return buf
	case PrimitiveString:
		s := v.str
		if len(s) <= 63 {
			buf := make([]byte, 1+len(s))
			buf[0] = makeHeader(BasicShortString, byte(len(s)))
			copy(buf[1:], s)
			return buf
		}
		buf := make([]byte, 5+len(s))
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveString))
		binary.LittleEndian.PutUint32(buf[1:], uint32(len(s)))
		copy(buf[5:], s)
		return buf
	case PrimitiveBinary:
		buf := make([]byte, 5+len(v.bytes))
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveBinary))
		binary.LittleEndian.PutUint32(buf[1:], uint32(len(v.bytes)))
		copy(buf[5:], v.bytes)
		return buf
	case PrimitiveDate:
		buf := make([]byte, 5)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveDate))
		binary.LittleEndian.PutUint32(buf[1:], uint32(v.i64))
		return buf
	case PrimitiveTimestamp:
		buf := make([]byte, 9)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveTimestamp))
		binary.LittleEndian.PutUint64(buf[1:], uint64(v.i64))
		return buf
	case PrimitiveTimestampNTZ:
		buf := make([]byte, 9)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveTimestampNTZ))
		binary.LittleEndian.PutUint64(buf[1:], uint64(v.i64))
		return buf
	case PrimitiveTime:
		buf := make([]byte, 9)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveTime))
		binary.LittleEndian.PutUint64(buf[1:], uint64(v.i64))
		return buf
	case PrimitiveUUID:
		buf := make([]byte, 17)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveUUID))
		copy(buf[1:], v.uuid[:])
		return buf
	case PrimitiveDecimal4:
		buf := make([]byte, 6)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveDecimal4))
		buf[1] = v.scale
		binary.LittleEndian.PutUint32(buf[2:], uint32(v.i64))
		return buf
	case PrimitiveDecimal8:
		buf := make([]byte, 10)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveDecimal8))
		buf[1] = v.scale
		binary.LittleEndian.PutUint64(buf[2:], uint64(v.i64))
		return buf
	case PrimitiveDecimal16:
		buf := make([]byte, 18)
		buf[0] = makeHeader(BasicPrimitive, byte(PrimitiveDecimal16))
		buf[1] = v.scale
		copy(buf[2:], v.decimal16[:])
		return buf
	default:
		return []byte{makeHeader(BasicPrimitive, byte(PrimitiveNull))}
	}
}

// makeHeader constructs a value_metadata byte.
func makeHeader(basic BasicType, valueHeader byte) byte {
	return byte(basic) | (valueHeader << 2)
}

type encoder struct {
	b       *MetadataBuilder
	scratch []byte
}

type encodedField struct {
	id    int
	name  string
	start int
	end   int
}

type elementPos struct {
	start int
	end   int
}

var testHookVerifyArrayLayout func(e *encoder, positions []elementPos)
var testHookVerifyObjectLayout func(e *encoder, entries []encodedField)

func (e *encoder) encodeValuePrimitive(v Value) (start, end int) {
	start = len(e.scratch)
	switch v.primitive {
	case PrimitiveNull:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveNull)))
	case PrimitiveTrue:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveTrue)))
	case PrimitiveFalse:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveFalse)))
	case PrimitiveInt8:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveInt8)), byte(v.i64))
	case PrimitiveInt16:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveInt16)), 0, 0)
		binary.LittleEndian.PutUint16(e.scratch[start+1:], uint16(v.i64))
	case PrimitiveInt32:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveInt32)), 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(e.scratch[start+1:], uint32(v.i64))
	case PrimitiveInt64:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveInt64)), 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(e.scratch[start+1:], uint64(v.i64))
	case PrimitiveFloat:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveFloat)), 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(e.scratch[start+1:], math.Float32bits(float32(v.f64)))
	case PrimitiveDouble:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveDouble)), 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(e.scratch[start+1:], math.Float64bits(v.f64))
	case PrimitiveString:
		s := v.str
		if len(s) <= 63 {
			e.scratch = append(e.scratch, makeHeader(BasicShortString, byte(len(s))))
			e.scratch = append(e.scratch, s...)
		} else {
			e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveString)), 0, 0, 0, 0)
			binary.LittleEndian.PutUint32(e.scratch[start+1:], uint32(len(s)))
			e.scratch = append(e.scratch, s...)
		}
	case PrimitiveBinary:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveBinary)), 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(e.scratch[start+1:], uint32(len(v.bytes)))
		e.scratch = append(e.scratch, v.bytes...)
	case PrimitiveDate:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveDate)), 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(e.scratch[start+1:], uint32(v.i64))
	case PrimitiveTimestamp:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveTimestamp)), 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(e.scratch[start+1:], uint64(v.i64))
	case PrimitiveTimestampNTZ:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveTimestampNTZ)), 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(e.scratch[start+1:], uint64(v.i64))
	case PrimitiveTime:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveTime)), 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(e.scratch[start+1:], uint64(v.i64))
	case PrimitiveUUID:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveUUID)))
		e.scratch = append(e.scratch, v.uuid[:]...)
	case PrimitiveDecimal4:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveDecimal4)), v.scale, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(e.scratch[start+2:], uint32(v.i64))
	case PrimitiveDecimal8:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveDecimal8)), v.scale, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(e.scratch[start+2:], uint64(v.i64))
	case PrimitiveDecimal16:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveDecimal16)), v.scale)
		e.scratch = append(e.scratch, v.decimal16[:]...)
	default:
		e.scratch = append(e.scratch, makeHeader(BasicPrimitive, byte(PrimitiveNull)))
	}
	end = len(e.scratch)
	return start, end
}

func (e *encoder) encodeValue(v Value) (start, end int, err error) {
	switch v.basic {
	case BasicObject:
		return e.encodeValueObject(v.object)
	case BasicArray:
		return e.encodeValueArray(v.array)
	default:
		start, end = e.encodeValuePrimitive(v)
		return start, end, nil
	}
}

func (e *encoder) encodeValueArray(arr Array) (start, end int, err error) {
	n := len(arr.Elements)
	if n == 0 {
		return e.buildArrayBytes(nil)
	}

	var posBuf [32]elementPos
	var positions []elementPos
	if n <= len(posBuf) {
		positions = posBuf[:n]
	} else {
		positions = make([]elementPos, n)
	}

	for i := range n {
		pStart, pEnd, err := e.encodeValue(arr.Elements[i])
		if err != nil {
			return 0, 0, err
		}
		positions[i] = elementPos{start: pStart, end: pEnd}
	}

	return e.buildArrayBytes(positions)
}

func (e *encoder) encodeValueObject(obj Object) (start, end int, err error) {
	n := len(obj.Fields)
	if n == 0 {
		return e.buildObjectBytes(nil)
	}

	var entriesBuf [16]encodedField
	var entries []encodedField
	if n <= len(entriesBuf) {
		entries = entriesBuf[:n]
	} else {
		entries = make([]encodedField, n)
	}

	for i, f := range obj.Fields {
		id := e.b.Add(f.Name)
		pStart, pEnd, err := e.encodeValue(f.Value)
		if err != nil {
			return 0, 0, err
		}
		entries[i] = encodedField{
			id:    id,
			name:  f.Name,
			start: pStart,
			end:   pEnd,
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	return e.buildObjectBytes(entries)
}

func (e *encoder) encodeReflect(rv reflect.Value) (start, end int, err error) {
	if !rv.IsValid() {
		start, end = e.encodeValuePrimitive(Null())
		return start, end, nil
	}

	// Dereference pointers/interfaces
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			start, end = e.encodeValuePrimitive(Null())
			return start, end, nil
		}
		rv = rv.Elem()
	}

	// Check concrete types first
	if rv.Type() == reflect.TypeFor[uuid.UUID]() {
		start, end = e.encodeValuePrimitive(UUID(rv.Interface().(uuid.UUID)))
		return start, end, nil
	}

	switch rv.Kind() {
	case reflect.Bool:
		start, end = e.encodeValuePrimitive(Bool(rv.Bool()))
		return start, end, nil
	case reflect.Int8:
		start, end = e.encodeValuePrimitive(Int8(int8(rv.Int())))
		return start, end, nil
	case reflect.Int16:
		start, end = e.encodeValuePrimitive(Int16(int16(rv.Int())))
		return start, end, nil
	case reflect.Int32:
		start, end = e.encodeValuePrimitive(Int32(int32(rv.Int())))
		return start, end, nil
	case reflect.Int64, reflect.Int:
		start, end = e.encodeValuePrimitive(Int64(rv.Int()))
		return start, end, nil
	case reflect.Uint8:
		start, end = e.encodeValuePrimitive(Int16(int16(rv.Uint())))
		return start, end, nil
	case reflect.Uint16:
		start, end = e.encodeValuePrimitive(Int32(int32(rv.Uint())))
		return start, end, nil
	case reflect.Uint32:
		start, end = e.encodeValuePrimitive(Int64(int64(rv.Uint())))
		return start, end, nil
	case reflect.Uint64, reflect.Uint:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return 0, 0, fmt.Errorf("variant marshal: uint64 value %d overflows int64", u)
		}
		start, end = e.encodeValuePrimitive(Int64(int64(u)))
		return start, end, nil
	case reflect.Float32:
		start, end = e.encodeValuePrimitive(Float(float32(rv.Float())))
		return start, end, nil
	case reflect.Float64:
		start, end = e.encodeValuePrimitive(Double(rv.Float()))
		return start, end, nil
	case reflect.String:
		start, end = e.encodeValuePrimitive(String(rv.String()))
		return start, end, nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			start, end = e.encodeValuePrimitive(Binary(rv.Bytes()))
			return start, end, nil
		}
		return e.encodeSliceArray(rv)
	case reflect.Array:
		if rv.Type() == reflect.TypeFor[uuid.UUID]() {
			var u uuid.UUID
			reflect.ValueOf(&u).Elem().Set(rv)
			start, end = e.encodeValuePrimitive(UUID(u))
			return start, end, nil
		}
		return e.encodeSliceArray(rv)
	case reflect.Map:
		return e.encodeMapObject(rv)
	case reflect.Struct:
		return e.encodeStructObject(rv)
	default:
		return 0, 0, fmt.Errorf("variant marshal: unsupported type %s", rv.Type())
	}
}

func (e *encoder) encodeSliceArray(rv reflect.Value) (start, end int, err error) {
	n := rv.Len()
	if n == 0 {
		return e.buildArrayBytes(nil)
	}

	var posBuf [32]elementPos
	var positions []elementPos
	if n <= len(posBuf) {
		positions = posBuf[:n]
	} else {
		positions = make([]elementPos, n)
	}

	for i := range n {
		pStart, pEnd, err := e.encodeReflect(rv.Index(i))
		if err != nil {
			return 0, 0, err
		}
		positions[i] = elementPos{start: pStart, end: pEnd}
	}

	return e.buildArrayBytes(positions)
}

func (e *encoder) buildArrayBytes(positions []elementPos) (start, end int, err error) {
	n := len(positions)

	if testHookVerifyArrayLayout != nil {
		testHookVerifyArrayLayout(e, positions)
	}

	totalSize := 0
	for _, pos := range positions {
		totalSize += pos.end - pos.start
	}

	offsets := make([]int, n+1)
	off := 0
	for i, pos := range positions {
		offsets[i] = off
		off += pos.end - pos.start
	}
	offsets[n] = off

	offsetSzCode := offsetSizeCode(totalSize)
	offsetSz := offsetSize(offsetSzCode)

	isLarge := byte(0)
	numElemSize := 1
	if n > 255 {
		isLarge = 1
		numElemSize = 4
	}

	header := byte(BasicArray) | (offsetSzCode << 2) | (isLarge << 4)

	bufSize := 1 + numElemSize + (n+1)*offsetSz + totalSize
	buf := make([]byte, bufSize)
	buf[0] = header

	posIdx := 1
	if isLarge == 1 {
		binary.LittleEndian.PutUint32(buf[posIdx:], uint32(n))
		posIdx += 4
	} else {
		buf[posIdx] = byte(n)
		posIdx += 1
	}

	for _, o := range offsets {
		writeUint(buf[posIdx:], o, offsetSz)
		posIdx += offsetSz
	}

	for _, p := range positions {
		copy(buf[posIdx:], e.scratch[p.start:p.end])
		posIdx += p.end - p.start
	}

	firstStart := len(e.scratch)
	if n > 0 {
		firstStart = positions[0].start
	}
	e.scratch = e.scratch[:firstStart]

	start = len(e.scratch)
	e.scratch = append(e.scratch, buf...)
	end = len(e.scratch)

	return start, end, nil
}

func (e *encoder) encodeMapObject(rv reflect.Value) (start, end int, err error) {
	if rv.IsNil() {
		start, end = e.encodeValuePrimitive(Null())
		return start, end, nil
	}

	n := rv.Len()
	if n == 0 {
		return e.buildObjectBytes(nil)
	}

	var entriesBuf [16]encodedField
	var entries []encodedField
	if n <= len(entriesBuf) {
		entries = entriesBuf[:n]
	} else {
		entries = make([]encodedField, n)
	}

	iter := rv.MapRange()
	idx := 0
	for iter.Next() {
		key := iter.Key()
		if key.Kind() != reflect.String {
			return 0, 0, fmt.Errorf("variant marshal: map key must be string, got %s", key.Type())
		}
		name := key.String()
		id := e.b.Add(name)

		pStart, pEnd, err := e.encodeReflect(iter.Value())
		if err != nil {
			return 0, 0, err
		}
		entries[idx] = encodedField{
			id:    id,
			name:  name,
			start: pStart,
			end:   pEnd,
		}
		idx++
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	return e.buildObjectBytes(entries)
}

func (e *encoder) encodeStructObject(rv reflect.Value) (start, end int, err error) {
	rt := rv.Type()
	numFields := rt.NumField()

	visibleCount := 0
	for i := range numFields {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		name := fieldName(sf)
		if name == "-" {
			continue
		}
		visibleCount++
	}

	if visibleCount == 0 {
		return e.buildObjectBytes(nil)
	}

	var entriesBuf [16]encodedField
	var entries []encodedField
	if visibleCount <= len(entriesBuf) {
		entries = entriesBuf[:visibleCount]
	} else {
		entries = make([]encodedField, visibleCount)
	}

	idx := 0
	for i := range numFields {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		name := fieldName(sf)
		if name == "-" {
			continue
		}

		id := e.b.Add(name)
		pStart, pEnd, err := e.encodeReflect(rv.Field(i))
		if err != nil {
			return 0, 0, err
		}
		entries[idx] = encodedField{
			id:    id,
			name:  name,
			start: pStart,
			end:   pEnd,
		}
		idx++
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	return e.buildObjectBytes(entries)
}

func (e *encoder) buildObjectBytes(entries []encodedField) (start, end int, err error) {
	n := len(entries)

	if testHookVerifyObjectLayout != nil {
		testHookVerifyObjectLayout(e, entries)
	}

	maxID := 0
	totalValueSize := 0
	for _, entry := range entries {
		if entry.id > maxID {
			maxID = entry.id
		}
		totalValueSize += entry.end - entry.start
	}

	valueOffsets := make([]int, n+1)
	off := 0
	for i, entry := range entries {
		valueOffsets[i] = off
		off += entry.end - entry.start
	}
	valueOffsets[n] = off

	fieldIDSizeCode := offsetSizeCode(maxID)
	fieldIDSize := offsetSize(fieldIDSizeCode)
	offsetSzCode := offsetSizeCode(totalValueSize)
	offsetSz := offsetSize(offsetSzCode)

	isLarge := byte(0)
	numElemSize := 1
	if n > 255 {
		isLarge = 1
		numElemSize = 4
	}

	header := byte(BasicObject) | (fieldIDSizeCode << 2) | (offsetSzCode << 4) | (isLarge << 6)

	totalSize := 1 + numElemSize + n*fieldIDSize + (n+1)*offsetSz + totalValueSize
	buf := make([]byte, totalSize)
	buf[0] = header

	pos := 1
	if isLarge == 1 {
		binary.LittleEndian.PutUint32(buf[pos:], uint32(n))
		pos += 4
	} else {
		buf[pos] = byte(n)
		pos += 1
	}

	for _, entry := range entries {
		writeUint(buf[pos:], entry.id, fieldIDSize)
		pos += fieldIDSize
	}

	for _, offsetVal := range valueOffsets {
		writeUint(buf[pos:], offsetVal, offsetSz)
		pos += offsetSz
	}

	for _, entry := range entries {
		copy(buf[pos:], e.scratch[entry.start:entry.end])
		pos += entry.end - entry.start
	}

	firstStart := len(e.scratch)
	if n > 0 {
		firstStart = entries[0].start
		for _, entry := range entries {
			if entry.start < firstStart {
				firstStart = entry.start
			}
		}
	}
	e.scratch = e.scratch[:firstStart]

	start = len(e.scratch)
	e.scratch = append(e.scratch, buf...)
	end = len(e.scratch)

	return start, end, nil
}
