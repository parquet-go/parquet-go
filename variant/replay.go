package variant

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
	"unsafe"

	"github.com/google/uuid"
)

// Replay walks the variant binary in data and replays it as events on w,
// translating object field IDs through m. It is equivalent to Decode
// followed by Value.Write, and applies the same validation, but does not
// materialize a Value tree: containers are walked in place and scalar
// payloads are passed to w as views of data. The views are only valid for
// the duration of each event call, so w must consume them before returning
// (every ValueBuilder in this module does).
func Replay(w ValueBuilder, m Metadata, data []byte) error {
	if len(data) == 0 {
		return errors.New("variant value: empty data")
	}
	header := data[0]
	switch BasicType(header & 0x03) {
	case BasicObject:
		return replayObject(w, m, header, data[1:])
	case BasicArray:
		return replayArray(w, m, header, data[1:])
	case BasicShortString:
		length := int(header >> 2)
		if len(data) < 1+length {
			return fmt.Errorf("variant value: short string length %d exceeds data", length)
		}
		s := data[1 : 1+length]
		if !utf8.Valid(s) {
			return errors.New("variant value: short string is not valid UTF-8")
		}
		w.String(viewString(s))
		return nil
	default:
		return replayPrimitive(w, PrimitiveType(header>>2), data[1:])
	}
}

// ReplayObjectFields replays the fields of the object value encoded in data
// as Field/value event pairs on w, skipping fields whose name skip reports
// true for (a nil skip replays every field). It emits no BeginObject or
// EndObject events, so callers can splice the fields into an object of
// their own framing. The boolean result reports whether data encoded an
// object; other values replay nothing. Field names are validated for
// duplicates whether or not they are skipped, matching Decode; the values
// of skipped fields are not walked and therefore not validated.
func ReplayObjectFields(w ValueBuilder, m Metadata, data []byte, skip func(name string) bool) (bool, error) {
	if len(data) == 0 {
		return false, errors.New("variant value: empty data")
	}
	header := data[0]
	if BasicType(header&0x03) != BasicObject {
		return false, nil
	}
	return true, replayObjectFields(w, m, header, data[1:], skip)
}

// viewString views a byte slice as a string without copying. Callers must
// not let the string outlive the backing data.
func viewString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func replayObject(w ValueBuilder, m Metadata, header byte, data []byte) error {
	w.BeginObject()
	if err := replayObjectFields(w, m, header, data, nil); err != nil {
		return err
	}
	w.EndObject()
	return nil
}

// replayObjectFields walks the object encoding of decodeObject (see
// decoding.go for the layout and the spec references) field by field,
// with the same validation, but without materializing field ID, offset, or
// Field slices: the fixed-width field_ids and field_offsets arrays are
// indexed in place.
func replayObjectFields(w ValueBuilder, m Metadata, header byte, data []byte, skip func(name string) bool) error {
	idSize := offsetSize((header >> 4) & 0x03)
	offSize := offsetSize((header >> 2) & 0x03)

	num, pos, err := replayNumElements((header>>6)&0x01 == 1, data, "object")
	if err != nil {
		return err
	}
	// The two divisions also guarantee that the index arithmetic below
	// cannot overflow int on 32-bit platforms: every product is bounded by
	// len(data).
	if num < 0 || (len(data)-pos)/idSize < num {
		return fmt.Errorf("variant value: object element count %d exceeds data", num)
	}
	ids := pos
	offs := ids + num*idSize
	if (len(data)-offs)/offSize <= num {
		return fmt.Errorf("variant value: object element count %d exceeds data", num)
	}
	values := offs + (num+1)*offSize
	valuesEnd, err := replayOffset(data, offs, num, offSize, values)
	if err != nil {
		return err
	}

	// Duplicate field names are rejected like decodeObject: in sort-order
	// input a duplicate is adjacent, and the map is only allocated for
	// inputs that violate the spec's sort order.
	prev := ""
	var seen map[string]struct{}
	for i := range num {
		id, _, err := readUint(data[ids+i*idSize:], idSize)
		if err != nil {
			return fmt.Errorf("variant value: reading object field id %d: %w", i, err)
		}
		name, err := m.Lookup(id)
		if err != nil {
			return fmt.Errorf("variant value: object field %d: %w", i, err)
		}
		if seen == nil && i > 0 && name <= prev {
			if name == prev {
				return fmt.Errorf("variant value: duplicate object field %q", name)
			}
			seen = make(map[string]struct{}, num)
			for j := range i {
				id, _, err := readUint(data[ids+j*idSize:], idSize)
				if err != nil {
					return fmt.Errorf("variant value: reading object field id %d: %w", j, err)
				}
				prevName, err := m.Lookup(id)
				if err != nil {
					return fmt.Errorf("variant value: object field %d: %w", j, err)
				}
				seen[prevName] = struct{}{}
			}
		}
		if seen != nil {
			if _, dup := seen[name]; dup {
				return fmt.Errorf("variant value: duplicate object field %q", name)
			}
			seen[name] = struct{}{}
		}
		prev = name

		start, err := replayOffset(data, offs, i, offSize, values)
		if err != nil {
			return fmt.Errorf("variant value: object field %d: invalid value offset", i)
		}
		if start > valuesEnd {
			return fmt.Errorf("variant value: object field %d: invalid value offset", i)
		}
		if skip != nil && skip(name) {
			continue
		}
		w.Field(name)
		if err := Replay(w, m, data[start:valuesEnd]); err != nil {
			return fmt.Errorf("variant value: object field %q: %w", name, err)
		}
	}
	return nil
}

// replayArray walks the array encoding of decodeArray (see decoding.go)
// element by element with the same validation: elements are sliced from
// consecutive offset pairs, and the trailing offset is only bounds-checked
// where decodeArray checks it (as the end of the last element).
func replayArray(w ValueBuilder, m Metadata, header byte, data []byte) error {
	offSize := offsetSize((header >> 2) & 0x03)

	num, pos, err := replayNumElements((header>>4)&0x01 == 1, data, "array")
	if err != nil {
		return err
	}
	if num < 0 || (len(data)-pos)/offSize <= num {
		return fmt.Errorf("variant value: array element count %d exceeds data", num)
	}
	offs := pos
	values := offs + (num+1)*offSize

	w.BeginArray()
	for i := range num {
		start, _, err := readUint(data[offs+i*offSize:], offSize)
		if err != nil {
			return fmt.Errorf("variant value: reading array offset %d: %w", i, err)
		}
		end, _, err := readUint(data[offs+(i+1)*offSize:], offSize)
		if err != nil {
			return fmt.Errorf("variant value: reading array offset %d: %w", i+1, err)
		}
		// start < 0 happens only on 32-bit platforms, where a 4-byte
		// offset above math.MaxInt32 overflows int (see decodeArray).
		elemStart, elemEnd := values+start, values+end
		if start < 0 || elemEnd > len(data) || elemStart > elemEnd {
			return fmt.Errorf("variant value: array element %d: invalid offset", i)
		}
		if err := Replay(w, m, data[elemStart:elemEnd]); err != nil {
			return fmt.Errorf("variant value: array element %d: %w", i, err)
		}
	}
	w.EndArray()
	return nil
}

// replayNumElements reads the num_elements header of an object or array
// and returns it with the position of the first byte after it.
func replayNumElements(isLarge bool, data []byte, what string) (int, int, error) {
	if isLarge {
		if len(data) < 4 {
			return 0, 0, fmt.Errorf("variant value: not enough data for %s num_elements", what)
		}
		return int(binary.LittleEndian.Uint32(data[:4])), 4, nil
	}
	if len(data) < 1 {
		return 0, 0, fmt.Errorf("variant value: not enough data for %s num_elements", what)
	}
	return int(data[0]), 1, nil
}

// replayOffset reads the i-th entry of the field_offsets array at offs and
// returns it as an absolute position from values. A result below values
// (offset overflow on 32-bit platforms) is an error; callers bound it
// against the end of their value data.
func replayOffset(data []byte, offs, i, offSize, values int) (int, error) {
	off, _, err := readUint(data[offs+i*offSize:], offSize)
	if err != nil {
		return 0, err
	}
	pos := values + off
	if pos < values || pos > len(data) {
		return 0, errors.New("variant value: offset exceeds data")
	}
	return pos, nil
}

// replayPrimitive replays one primitive scalar, mirroring the validation of
// decodePrimitive (see decoding.go) but emitting the value as an event
// instead of materializing it; string and binary payloads are views of
// data.
func replayPrimitive(w ValueBuilder, pt PrimitiveType, data []byte) error {
	need := func(n int, what string) error {
		if len(data) < n {
			return fmt.Errorf("variant value: not enough data for %s", what)
		}
		return nil
	}
	switch pt {
	case PrimitiveNull:
		w.Null()
	case PrimitiveTrue:
		w.Bool(true)
	case PrimitiveFalse:
		w.Bool(false)
	case PrimitiveInt8:
		if err := need(1, "int8"); err != nil {
			return err
		}
		w.Int8(int8(data[0]))
	case PrimitiveInt16:
		if err := need(2, "int16"); err != nil {
			return err
		}
		w.Int16(int16(binary.LittleEndian.Uint16(data[:2])))
	case PrimitiveInt32:
		if err := need(4, "int32"); err != nil {
			return err
		}
		w.Int32(int32(binary.LittleEndian.Uint32(data[:4])))
	case PrimitiveInt64:
		if err := need(8, "int64"); err != nil {
			return err
		}
		w.Int64(int64(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveFloat:
		if err := need(4, "float"); err != nil {
			return err
		}
		w.Float(math.Float32frombits(binary.LittleEndian.Uint32(data[:4])))
	case PrimitiveDouble:
		if err := need(8, "double"); err != nil {
			return err
		}
		w.Double(math.Float64frombits(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveString:
		if err := need(4, "string length"); err != nil {
			return err
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if length < 0 || length > len(data)-4 {
			return fmt.Errorf("variant value: string length %d exceeds data", length)
		}
		s := data[4 : 4+length]
		if !utf8.Valid(s) {
			return errors.New("variant value: string is not valid UTF-8")
		}
		w.String(viewString(s))
	case PrimitiveBinary:
		if err := need(4, "binary length"); err != nil {
			return err
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if length < 0 || length > len(data)-4 {
			return fmt.Errorf("variant value: binary length %d exceeds data", length)
		}
		w.Binary(data[4 : 4+length])
	case PrimitiveDate:
		if err := need(4, "date"); err != nil {
			return err
		}
		w.Date(int32(binary.LittleEndian.Uint32(data[:4])))
	case PrimitiveTimestamp:
		if err := need(8, "timestamp"); err != nil {
			return err
		}
		w.Timestamp(int64(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveTimestampNTZ:
		if err := need(8, "timestamp_ntz"); err != nil {
			return err
		}
		w.TimestampNTZ(int64(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveTime:
		if err := need(8, "time"); err != nil {
			return err
		}
		w.Time(int64(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveTimestampNanos:
		if err := need(8, "timestamp nanos"); err != nil {
			return err
		}
		w.TimestampNanos(int64(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveTimestampNTZNanos:
		if err := need(8, "timestamp_ntz nanos"); err != nil {
			return err
		}
		w.TimestampNTZNanos(int64(binary.LittleEndian.Uint64(data[:8])))
	case PrimitiveUUID:
		if err := need(16, "uuid"); err != nil {
			return err
		}
		w.UUID(uuid.UUID(data[:16]))
	case PrimitiveDecimal4:
		if err := need(5, "decimal4"); err != nil {
			return err
		}
		w.Decimal4(int32(binary.LittleEndian.Uint32(data[1:5])), data[0])
	case PrimitiveDecimal8:
		if err := need(9, "decimal8"); err != nil {
			return err
		}
		w.Decimal8(int64(binary.LittleEndian.Uint64(data[1:9])), data[0])
	case PrimitiveDecimal16:
		if err := need(17, "decimal16"); err != nil {
			return err
		}
		var v [16]byte
		copy(v[:], data[1:17])
		w.Decimal16(v, data[0])
	default:
		return fmt.Errorf("variant value: unknown primitive type %d", pt)
	}
	return nil
}
