// Package thriftdecode provides optimized decoders for parquet format types.
//
// The decoders use zero-copy techniques where possible, so the input data
// must remain valid for the lifetime of the decoded structures.
package thriftdecode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unsafe"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

const (
	typeStop   = 0
	typeTrue   = 1
	typeFalse  = 2
	typeI8     = 3
	typeI16    = 4
	typeI32    = 5
	typeI64    = 6
	typeDouble = 7
	typeBinary = 8
	typeList   = 9
	typeSet    = 10
	typeMap    = 11
	typeStruct = 12
)

type buffer struct {
	data []byte
	pos  int
}

func (b *buffer) ReadByte() (byte, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	v := b.data[b.pos]
	b.pos++
	return v, nil
}

func (b *buffer) readSlice(n int) ([]byte, error) {
	if b.pos+n > len(b.data) {
		return nil, io.ErrUnexpectedEOF
	}
	slice := b.data[b.pos : b.pos+n]
	b.pos += n
	return slice, nil
}

func (b *buffer) skip(n int) error {
	if b.pos+n > len(b.data) {
		return io.ErrUnexpectedEOF
	}
	b.pos += n
	return nil
}

func (b *buffer) readUvarint() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; ; i++ {
		if b.pos >= len(b.data) {
			return 0, io.ErrUnexpectedEOF
		}
		v := b.data[b.pos]
		b.pos++
		if v < 0x80 {
			if i >= binary.MaxVarintLen64 || i == binary.MaxVarintLen64-1 && v > 1 {
				return 0, errors.New("thriftdecode: varint overflows uint64")
			}
			return x | uint64(v)<<s, nil
		}
		x |= uint64(v&0x7f) << s
		s += 7
	}
}

func (b *buffer) readVarint() (int64, error) {
	ux, err := b.readUvarint()
	if err != nil {
		return 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}

func (b *buffer) readLength() (int, error) {
	n, err := b.readUvarint()
	return int(n), err
}

func (b *buffer) readBytesRef() ([]byte, error) {
	n, err := b.readLength()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	return b.readSlice(n)
}

func (b *buffer) readI32() (int32, error) {
	v, err := b.readVarint()
	return int32(v), err
}

func (b *buffer) readI64() (int64, error) {
	return b.readVarint()
}

func (b *buffer) readI8() (int8, error) {
	v, err := b.ReadByte()
	return int8(v), err
}

func (b *buffer) readI16() (int16, error) {
	v, err := b.readVarint()
	return int16(v), err
}

func (b *buffer) readDouble() (float64, error) {
	if b.pos+8 > len(b.data) {
		return 0, io.ErrUnexpectedEOF
	}
	bits := binary.LittleEndian.Uint64(b.data[b.pos:])
	b.pos += 8
	return math.Float64frombits(bits), nil
}

func (b *buffer) readBool(typ byte) (bool, error) {
	switch typ {
	case typeTrue:
		return true, nil
	case typeFalse:
		return false, nil
	default:
		return false, fmt.Errorf("thriftdecode: expected BOOL type, got %d", typ)
	}
}

func (b *buffer) readStringRef() (string, error) {
	data, err := b.readBytesRef()
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", nil
	}
	return unsafe.String(&data[0], len(data)), nil
}

func (b *buffer) readField(lastID int16) (id int16, typ byte, err error) {
	v, err := b.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	typ = v & 0x0F
	if typ == typeStop {
		return 0, typeStop, nil
	}

	delta := v >> 4
	if delta != 0 {
		id = lastID + int16(delta)
	} else {
		v, err := b.readVarint()
		if err != nil {
			return 0, 0, err
		}
		id = int16(v)
	}

	return id, typ, nil
}

func (b *buffer) readList() (size int, typ byte, err error) {
	v, err := b.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	typ = v & 0x0F
	size = int(v >> 4)

	if size == 0x0F {
		n, err := b.readUvarint()
		if err != nil {
			return 0, 0, err
		}
		size = int(n)
	}

	return size, typ, nil
}

func (b *buffer) skipValue(typ byte) error {
	switch typ {
	case typeTrue, typeFalse:
		return nil
	case typeI8:
		return b.skip(1)
	case typeI16, typeI32, typeI64:
		_, err := b.readVarint()
		return err
	case typeDouble:
		return b.skip(8)
	case typeBinary:
		n, err := b.readLength()
		if err != nil {
			return err
		}
		return b.skip(n)
	case typeList, typeSet:
		size, elemType, err := b.readList()
		if err != nil {
			return err
		}
		for range size {
			if err := b.skipValue(elemType); err != nil {
				return err
			}
		}
		return nil
	case typeMap:
		n, err := b.readUvarint()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		kv, err := b.ReadByte()
		if err != nil {
			return err
		}
		keyType := kv >> 4
		valType := kv & 0x0F
		for range n {
			if err := b.skipValue(keyType); err != nil {
				return err
			}
			if err := b.skipValue(valType); err != nil {
				return err
			}
		}
		return nil
	case typeStruct:
		return b.skipStruct()
	default:
		return fmt.Errorf("thriftdecode: unknown type %d", typ)
	}
}

func (b *buffer) skipStruct() error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		if err := b.skipValue(typ); err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeStatistics(s *format.Statistics) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.Max: expected BINARY, got %d", typ)
			}
			s.Max, err = b.readBytesRef()
		case 2:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.Min: expected BINARY, got %d", typ)
			}
			s.Min, err = b.readBytesRef()
		case 3:
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: Statistics.NullCount: expected I64, got %d", typ)
			}
			s.NullCount, err = b.readI64()
		case 4:
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: Statistics.DistinctCount: expected I64, got %d", typ)
			}
			s.DistinctCount, err = b.readI64()
		case 5:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.MaxValue: expected BINARY, got %d", typ)
			}
			s.MaxValue, err = b.readBytesRef()
		case 6:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.MinValue: expected BINARY, got %d", typ)
			}
			s.MinValue, err = b.readBytesRef()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeKeyValue(kv *format.KeyValue) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: KeyValue.Key: expected BINARY, got %d", typ)
			}
			kv.Key, err = b.readStringRef()
		case 2:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: KeyValue.Value: expected BINARY, got %d", typ)
			}
			kv.Value, err = b.readStringRef()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeSortingColumn(sc *format.SortingColumn) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SortingColumn.ColumnIdx: expected I32, got %d", typ)
			}
			sc.ColumnIdx, err = b.readI32()
		case 2:
			sc.Descending, err = b.readBool(typ)
		case 3:
			sc.NullsFirst, err = b.readBool(typ)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodePageEncodingStats(pes *format.PageEncodingStats) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageEncodingStats.PageType: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			pes.PageType = format.PageType(v)
		case 2:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageEncodingStats.Encoding: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			pes.Encoding = format.Encoding(v)
		case 3:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageEncodingStats.Count: expected I32, got %d", typ)
			}
			pes.Count, err = b.readI32()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeDecimalType(dt *format.DecimalType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DecimalType.Scale: expected I32, got %d", typ)
			}
			dt.Scale, err = b.readI32()
		case 2:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DecimalType.Precision: expected I32, got %d", typ)
			}
			dt.Precision, err = b.readI32()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeIntType(it *format.IntType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeI8 {
				return fmt.Errorf("thriftdecode: IntType.BitWidth: expected I8, got %d", typ)
			}
			it.BitWidth, err = b.readI8()
		case 2:
			it.IsSigned, err = b.readBool(typ)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeGeometryType(gt *format.GeometryType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: GeometryType.CRS: expected BINARY, got %d", typ)
			}
			gt.CRS, err = b.readStringRef()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeGeographyType(gt *format.GeographyType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: GeographyType.CRS: expected BINARY, got %d", typ)
			}
			gt.CRS, err = b.readStringRef()
		case 2:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: GeographyType.Algorithm: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			gt.Algorithm = format.EdgeInterpolationAlgorithm(v)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeSizeStatistics(ss *format.SizeStatistics) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: SizeStatistics.UnencodedByteArrayDataBytes: expected I64, got %d", typ)
			}
			ss.UnencodedByteArrayDataBytes, err = b.readI64()
		case 2:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: SizeStatistics.RepetitionLevelHistogram: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI64 {
				return fmt.Errorf("thriftdecode: SizeStatistics.RepetitionLevelHistogram: expected I64 elements, got %d", elemType)
			}
			if cap(ss.RepetitionLevelHistogram) >= size {
				ss.RepetitionLevelHistogram = ss.RepetitionLevelHistogram[:size]
			} else {
				ss.RepetitionLevelHistogram = make([]int64, size)
			}
			for i := range size {
				ss.RepetitionLevelHistogram[i], err = b.readI64()
				if err != nil {
					return err
				}
			}
		case 3:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: SizeStatistics.DefinitionLevelHistogram: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI64 {
				return fmt.Errorf("thriftdecode: SizeStatistics.DefinitionLevelHistogram: expected I64 elements, got %d", elemType)
			}
			if cap(ss.DefinitionLevelHistogram) >= size {
				ss.DefinitionLevelHistogram = ss.DefinitionLevelHistogram[:size]
			} else {
				ss.DefinitionLevelHistogram = make([]int64, size)
			}
			for i := range size {
				ss.DefinitionLevelHistogram[i], err = b.readI64()
				if err != nil {
					return err
				}
			}
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeBoundingBox(bb *format.BoundingBox) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.XMin: expected DOUBLE, got %d", typ)
			}
			bb.XMin, err = b.readDouble()
		case 2:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.XMax: expected DOUBLE, got %d", typ)
			}
			bb.XMax, err = b.readDouble()
		case 3:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.YMin: expected DOUBLE, got %d", typ)
			}
			bb.YMin, err = b.readDouble()
		case 4:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.YMax: expected DOUBLE, got %d", typ)
			}
			bb.YMax, err = b.readDouble()
		case 5:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.ZMin: expected DOUBLE, got %d", typ)
			}
			v, err := b.readDouble()
			if err != nil {
				return err
			}
			bb.ZMin = &v
		case 6:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.ZMax: expected DOUBLE, got %d", typ)
			}
			v, err := b.readDouble()
			if err != nil {
				return err
			}
			bb.ZMax = &v
		case 7:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.MMin: expected DOUBLE, got %d", typ)
			}
			v, err := b.readDouble()
			if err != nil {
				return err
			}
			bb.MMin = &v
		case 8:
			if typ != typeDouble {
				return fmt.Errorf("thriftdecode: BoundingBox.MMax: expected DOUBLE, got %d", typ)
			}
			v, err := b.readDouble()
			if err != nil {
				return err
			}
			bb.MMax = &v
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeAesGcmV1(a *format.AesGcmV1) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: AesGcmV1.AadPrefix: expected BINARY, got %d", typ)
			}
			a.AadPrefix, err = b.readBytesRef()
		case 2:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: AesGcmV1.AadFileUnique: expected BINARY, got %d", typ)
			}
			a.AadFileUnique, err = b.readBytesRef()
		case 3:
			a.SupplyAadPrefix, err = b.readBool(typ)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeAesGcmCtrV1(a *format.AesGcmCtrV1) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: AesGcmCtrV1.AadPrefix: expected BINARY, got %d", typ)
			}
			a.AadPrefix, err = b.readBytesRef()
		case 2:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: AesGcmCtrV1.AadFileUnique: expected BINARY, got %d", typ)
			}
			a.AadFileUnique, err = b.readBytesRef()
		case 3:
			a.SupplyAadPrefix, err = b.readBool(typ)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeEncryptionWithColumnKey(e *format.EncryptionWithColumnKey) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: EncryptionWithColumnKey.PathInSchema: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeBinary {
				return fmt.Errorf("thriftdecode: EncryptionWithColumnKey.PathInSchema: expected BINARY elements, got %d", elemType)
			}
			if cap(e.PathInSchema) >= size {
				e.PathInSchema = e.PathInSchema[:size]
			} else {
				e.PathInSchema = make([]string, size)
			}
			for i := range size {
				e.PathInSchema[i], err = b.readStringRef()
				if err != nil {
					return err
				}
			}
		case 2:
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: EncryptionWithColumnKey.KeyMetadata: expected BINARY, got %d", typ)
			}
			e.KeyMetadata, err = b.readBytesRef()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodePageLocation(p *format.PageLocation) error {
	var lastID int16

	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}

		switch id {
		case 1:
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: PageLocation.Offset: expected I64, got %d", typ)
			}
			p.Offset, err = b.readI64()
		case 2:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageLocation.CompressedPageSize: expected I32, got %d", typ)
			}
			p.CompressedPageSize, err = b.readI32()
		case 3:
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: PageLocation.FirstRowIndex: expected I64, got %d", typ)
			}
			p.FirstRowIndex, err = b.readI64()
		default:
			err = b.skipValue(typ)
		}

		if err != nil {
			return err
		}
		lastID = id
	}
}

// DecodePageLocation decodes a PageLocation from the compact thrift data in src.
func DecodePageLocation(data []byte, p *format.PageLocation) error {
	return (&buffer{data: data}).decodePageLocation(p)
}

// DecodeOffsetIndex decodes an OffsetIndex from the compact thrift data in src.
func DecodeOffsetIndex(data []byte, o *format.OffsetIndex) error {
	b := &buffer{data: data}
	var lastID int16

	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}

		switch id {
		case 1:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: OffsetIndex.PageLocations: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: OffsetIndex.PageLocations: expected STRUCT elements, got %d", elemType)
			}
			if cap(o.PageLocations) >= size {
				o.PageLocations = o.PageLocations[:size]
			} else {
				o.PageLocations = make([]format.PageLocation, size)
			}
			for i := range size {
				if err := b.decodePageLocation(&o.PageLocations[i]); err != nil {
					return err
				}
			}

		case 2:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: OffsetIndex.UnencodedByteArrayDataBytes: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI64 {
				return fmt.Errorf("thriftdecode: OffsetIndex.UnencodedByteArrayDataBytes: expected I64 elements, got %d", elemType)
			}
			if cap(o.UnencodedByteArrayDataBytes) >= size {
				o.UnencodedByteArrayDataBytes = o.UnencodedByteArrayDataBytes[:size]
			} else {
				o.UnencodedByteArrayDataBytes = make([]int64, size)
			}
			for i := range size {
				o.UnencodedByteArrayDataBytes[i], err = b.readI64()
				if err != nil {
					return err
				}
			}

		default:
			err = b.skipValue(typ)
		}

		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeTimeUnit(tu *format.TimeUnit) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: TimeUnit.Millis: expected STRUCT, got %d", typ)
			}
			tu.Millis = &format.MilliSeconds{}
			if err := b.skipStruct(); err != nil {
				return err
			}
		case 2:
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: TimeUnit.Micros: expected STRUCT, got %d", typ)
			}
			tu.Micros = &format.MicroSeconds{}
			if err := b.skipStruct(); err != nil {
				return err
			}
		case 3:
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: TimeUnit.Nanos: expected STRUCT, got %d", typ)
			}
			tu.Nanos = &format.NanoSeconds{}
			if err := b.skipStruct(); err != nil {
				return err
			}
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeTimestampType(tt *format.TimestampType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			tt.IsAdjustedToUTC, err = b.readBool(typ)
		case 2:
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: TimestampType.Unit: expected STRUCT, got %d", typ)
			}
			err = b.decodeTimeUnit(&tt.Unit)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeTimeType(tt *format.TimeType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1:
			tt.IsAdjustedToUTC, err = b.readBool(typ)
		case 2:
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: TimeType.Unit: expected STRUCT, got %d", typ)
			}
			err = b.decodeTimeUnit(&tt.Unit)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeLogicalType(lt *format.LogicalType) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // UTF8/String
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.UTF8: expected STRUCT, got %d", typ)
			}
			lt.UTF8 = &format.StringType{}
			err = b.skipStruct()
		case 2: // Map
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Map: expected STRUCT, got %d", typ)
			}
			lt.Map = &format.MapType{}
			err = b.skipStruct()
		case 3: // List
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.List: expected STRUCT, got %d", typ)
			}
			lt.List = &format.ListType{}
			err = b.skipStruct()
		case 4: // Enum
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Enum: expected STRUCT, got %d", typ)
			}
			lt.Enum = &format.EnumType{}
			err = b.skipStruct()
		case 5: // Decimal
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Decimal: expected STRUCT, got %d", typ)
			}
			lt.Decimal = &format.DecimalType{}
			err = b.decodeDecimalType(lt.Decimal)
		case 6: // Date
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Date: expected STRUCT, got %d", typ)
			}
			lt.Date = &format.DateType{}
			err = b.skipStruct()
		case 7: // Time
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Time: expected STRUCT, got %d", typ)
			}
			lt.Time = &format.TimeType{}
			err = b.decodeTimeType(lt.Time)
		case 8: // Timestamp
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Timestamp: expected STRUCT, got %d", typ)
			}
			lt.Timestamp = &format.TimestampType{}
			err = b.decodeTimestampType(lt.Timestamp)
		case 10: // Integer
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Integer: expected STRUCT, got %d", typ)
			}
			lt.Integer = &format.IntType{}
			err = b.decodeIntType(lt.Integer)
		case 11: // Unknown/Null
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Unknown: expected STRUCT, got %d", typ)
			}
			lt.Unknown = &format.NullType{}
			err = b.skipStruct()
		case 12: // Json
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Json: expected STRUCT, got %d", typ)
			}
			lt.Json = &format.JsonType{}
			err = b.skipStruct()
		case 13: // Bson
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Bson: expected STRUCT, got %d", typ)
			}
			lt.Bson = &format.BsonType{}
			err = b.skipStruct()
		case 14: // UUID
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.UUID: expected STRUCT, got %d", typ)
			}
			lt.UUID = &format.UUIDType{}
			err = b.skipStruct()
		case 15: // Float16
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Float16: expected STRUCT, got %d", typ)
			}
			lt.Float16 = &format.Float16Type{}
			err = b.skipStruct()
		case 16: // Variant
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Variant: expected STRUCT, got %d", typ)
			}
			lt.Variant = &format.VariantType{}
			err = b.skipStruct()
		case 17: // Geometry
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Geometry: expected STRUCT, got %d", typ)
			}
			lt.Geometry = &format.GeometryType{}
			err = b.decodeGeometryType(lt.Geometry)
		case 18: // Geography
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: LogicalType.Geography: expected STRUCT, got %d", typ)
			}
			lt.Geography = &format.GeographyType{}
			err = b.decodeGeographyType(lt.Geography)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeSchemaElement(se *format.SchemaElement) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Type
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.Type: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			t := format.Type(v)
			se.Type = &t
		case 2: // TypeLength
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.TypeLength: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			se.TypeLength = &v
		case 3: // RepetitionType
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.RepetitionType: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			rt := format.FieldRepetitionType(v)
			se.RepetitionType = &rt
		case 4: // Name
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: SchemaElement.Name: expected BINARY, got %d", typ)
			}
			se.Name, err = b.readStringRef()
		case 5: // NumChildren
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.NumChildren: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			se.NumChildren = &v
		case 6: // ConvertedType
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.ConvertedType: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			ct := deprecated.ConvertedType(v)
			se.ConvertedType = &ct
		case 7: // Scale
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.Scale: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			se.Scale = &v
		case 8: // Precision
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.Precision: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			se.Precision = &v
		case 9: // FieldID
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: SchemaElement.FieldID: expected I32, got %d", typ)
			}
			se.FieldID, err = b.readI32()
		case 10: // LogicalType
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: SchemaElement.LogicalType: expected STRUCT, got %d", typ)
			}
			se.LogicalType = &format.LogicalType{}
			err = b.decodeLogicalType(se.LogicalType)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeColumnMetaData(cmd *format.ColumnMetaData) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Type
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.Type: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			cmd.Type = format.Type(v)
		case 2: // Encoding
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnMetaData.Encoding: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.Encoding: expected I32 elements, got %d", elemType)
			}
			if cap(cmd.Encoding) >= size {
				cmd.Encoding = cmd.Encoding[:size]
			} else {
				cmd.Encoding = make([]format.Encoding, size)
			}
			for i := range size {
				v, err := b.readI32()
				if err != nil {
					return err
				}
				cmd.Encoding[i] = format.Encoding(v)
			}
		case 3: // PathInSchema
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnMetaData.PathInSchema: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeBinary {
				return fmt.Errorf("thriftdecode: ColumnMetaData.PathInSchema: expected BINARY elements, got %d", elemType)
			}
			if cap(cmd.PathInSchema) >= size {
				cmd.PathInSchema = cmd.PathInSchema[:size]
			} else {
				cmd.PathInSchema = make([]string, size)
			}
			for i := range size {
				cmd.PathInSchema[i], err = b.readStringRef()
				if err != nil {
					return err
				}
			}
		case 4: // Codec
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.Codec: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			cmd.Codec = format.CompressionCodec(v)
		case 5: // NumValues
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.NumValues: expected I64, got %d", typ)
			}
			cmd.NumValues, err = b.readI64()
		case 6: // TotalUncompressedSize
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.TotalUncompressedSize: expected I64, got %d", typ)
			}
			cmd.TotalUncompressedSize, err = b.readI64()
		case 7: // TotalCompressedSize
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.TotalCompressedSize: expected I64, got %d", typ)
			}
			cmd.TotalCompressedSize, err = b.readI64()
		case 8: // KeyValueMetadata
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnMetaData.KeyValueMetadata: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnMetaData.KeyValueMetadata: expected STRUCT elements, got %d", elemType)
			}
			if cap(cmd.KeyValueMetadata) >= size {
				cmd.KeyValueMetadata = cmd.KeyValueMetadata[:size]
			} else {
				cmd.KeyValueMetadata = make([]format.KeyValue, size)
			}
			for i := range size {
				if err := b.decodeKeyValue(&cmd.KeyValueMetadata[i]); err != nil {
					return err
				}
			}
		case 9: // DataPageOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.DataPageOffset: expected I64, got %d", typ)
			}
			cmd.DataPageOffset, err = b.readI64()
		case 10: // IndexPageOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.IndexPageOffset: expected I64, got %d", typ)
			}
			cmd.IndexPageOffset, err = b.readI64()
		case 11: // DictionaryPageOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.DictionaryPageOffset: expected I64, got %d", typ)
			}
			cmd.DictionaryPageOffset, err = b.readI64()
		case 12: // Statistics
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnMetaData.Statistics: expected STRUCT, got %d", typ)
			}
			err = b.decodeStatistics(&cmd.Statistics)
		case 13: // EncodingStats
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnMetaData.EncodingStats: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnMetaData.EncodingStats: expected STRUCT elements, got %d", elemType)
			}
			if cap(cmd.EncodingStats) >= size {
				cmd.EncodingStats = cmd.EncodingStats[:size]
			} else {
				cmd.EncodingStats = make([]format.PageEncodingStats, size)
			}
			for i := range size {
				if err := b.decodePageEncodingStats(&cmd.EncodingStats[i]); err != nil {
					return err
				}
			}
		case 14: // BloomFilterOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.BloomFilterOffset: expected I64, got %d", typ)
			}
			cmd.BloomFilterOffset, err = b.readI64()
		case 15: // BloomFilterLength
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnMetaData.BloomFilterLength: expected I32, got %d", typ)
			}
			cmd.BloomFilterLength, err = b.readI32()
		case 16: // SizeStatistics
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnMetaData.SizeStatistics: expected STRUCT, got %d", typ)
			}
			err = b.decodeSizeStatistics(&cmd.SizeStatistics)
		case 17: // GeospatialStatistics
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnMetaData.GeospatialStatistics: expected STRUCT, got %d", typ)
			}
			err = b.decodeGeospatialStatistics(&cmd.GeospatialStatistics)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeGeospatialStatistics(gs *format.GeospatialStatistics) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // BBox
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: GeospatialStatistics.BBox: expected STRUCT, got %d", typ)
			}
			err = b.decodeBoundingBox(&gs.BBox)
		case 2: // GeoSpatialTypes
			if typ != typeList {
				return fmt.Errorf("thriftdecode: GeospatialStatistics.GeoSpatialTypes: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI32 {
				return fmt.Errorf("thriftdecode: GeospatialStatistics.GeoSpatialTypes: expected I32 elements, got %d", elemType)
			}
			if cap(gs.GeoSpatialTypes) >= size {
				gs.GeoSpatialTypes = gs.GeoSpatialTypes[:size]
			} else {
				gs.GeoSpatialTypes = make([]int32, size)
			}
			for i := range size {
				gs.GeoSpatialTypes[i], err = b.readI32()
				if err != nil {
					return err
				}
			}
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeColumnCryptoMetaData(ccmd *format.ColumnCryptoMetaData) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // EncryptionWithFooterKey
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnCryptoMetaData.EncryptionWithFooterKey: expected STRUCT, got %d", typ)
			}
			ccmd.EncryptionWithFooterKey = &format.EncryptionWithFooterKey{}
			err = b.skipStruct()
		case 2: // EncryptionWithColumnKey
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnCryptoMetaData.EncryptionWithColumnKey: expected STRUCT, got %d", typ)
			}
			ccmd.EncryptionWithColumnKey = &format.EncryptionWithColumnKey{}
			err = b.decodeEncryptionWithColumnKey(ccmd.EncryptionWithColumnKey)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeColumnChunk(cc *format.ColumnChunk) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // FilePath
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: ColumnChunk.FilePath: expected BINARY, got %d", typ)
			}
			cc.FilePath, err = b.readStringRef()
		case 2: // FileOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnChunk.FileOffset: expected I64, got %d", typ)
			}
			cc.FileOffset, err = b.readI64()
		case 3: // MetaData
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnChunk.MetaData: expected STRUCT, got %d", typ)
			}
			err = b.decodeColumnMetaData(&cc.MetaData)
		case 4: // OffsetIndexOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnChunk.OffsetIndexOffset: expected I64, got %d", typ)
			}
			cc.OffsetIndexOffset, err = b.readI64()
		case 5: // OffsetIndexLength
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnChunk.OffsetIndexLength: expected I32, got %d", typ)
			}
			cc.OffsetIndexLength, err = b.readI32()
		case 6: // ColumnIndexOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnChunk.ColumnIndexOffset: expected I64, got %d", typ)
			}
			cc.ColumnIndexOffset, err = b.readI64()
		case 7: // ColumnIndexLength
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnChunk.ColumnIndexLength: expected I32, got %d", typ)
			}
			cc.ColumnIndexLength, err = b.readI32()
		case 8: // CryptoMetadata
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnChunk.CryptoMetadata: expected STRUCT, got %d", typ)
			}
			err = b.decodeColumnCryptoMetaData(&cc.CryptoMetadata)
		case 9: // EncryptedColumnMetadata
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: ColumnChunk.EncryptedColumnMetadata: expected BINARY, got %d", typ)
			}
			cc.EncryptedColumnMetadata, err = b.readBytesRef()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeRowGroup(rg *format.RowGroup) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Columns
			if typ != typeList {
				return fmt.Errorf("thriftdecode: RowGroup.Columns: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: RowGroup.Columns: expected STRUCT elements, got %d", elemType)
			}
			if cap(rg.Columns) >= size {
				rg.Columns = rg.Columns[:size]
			} else {
				rg.Columns = make([]format.ColumnChunk, size)
			}
			for i := range size {
				if err := b.decodeColumnChunk(&rg.Columns[i]); err != nil {
					return err
				}
			}
		case 2: // TotalByteSize
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: RowGroup.TotalByteSize: expected I64, got %d", typ)
			}
			rg.TotalByteSize, err = b.readI64()
		case 3: // NumRows
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: RowGroup.NumRows: expected I64, got %d", typ)
			}
			rg.NumRows, err = b.readI64()
		case 4: // SortingColumns
			if typ != typeList {
				return fmt.Errorf("thriftdecode: RowGroup.SortingColumns: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: RowGroup.SortingColumns: expected STRUCT elements, got %d", elemType)
			}
			if cap(rg.SortingColumns) >= size {
				rg.SortingColumns = rg.SortingColumns[:size]
			} else {
				rg.SortingColumns = make([]format.SortingColumn, size)
			}
			for i := range size {
				if err := b.decodeSortingColumn(&rg.SortingColumns[i]); err != nil {
					return err
				}
			}
		case 5: // FileOffset
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: RowGroup.FileOffset: expected I64, got %d", typ)
			}
			rg.FileOffset, err = b.readI64()
		case 6: // TotalCompressedSize
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: RowGroup.TotalCompressedSize: expected I64, got %d", typ)
			}
			rg.TotalCompressedSize, err = b.readI64()
		case 7: // Ordinal
			if typ != typeI16 {
				return fmt.Errorf("thriftdecode: RowGroup.Ordinal: expected I16, got %d", typ)
			}
			rg.Ordinal, err = b.readI16()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeColumnOrder(co *format.ColumnOrder) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // TypeOrder
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: ColumnOrder.TypeOrder: expected STRUCT, got %d", typ)
			}
			co.TypeOrder = &format.TypeDefinedOrder{}
			err = b.skipStruct()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeEncryptionAlgorithm(ea *format.EncryptionAlgorithm) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // AesGcmV1
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: EncryptionAlgorithm.AesGcmV1: expected STRUCT, got %d", typ)
			}
			ea.AesGcmV1 = &format.AesGcmV1{}
			err = b.decodeAesGcmV1(ea.AesGcmV1)
		case 2: // AesGcmCtrV1
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: EncryptionAlgorithm.AesGcmCtrV1: expected STRUCT, got %d", typ)
			}
			ea.AesGcmCtrV1 = &format.AesGcmCtrV1{}
			err = b.decodeAesGcmCtrV1(ea.AesGcmCtrV1)
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (b *buffer) decodeFileMetaData(fmd *format.FileMetaData) error {
	var lastID int16
	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Version
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: FileMetaData.Version: expected I32, got %d", typ)
			}
			fmd.Version, err = b.readI32()
		case 2: // Schema
			if typ != typeList {
				return fmt.Errorf("thriftdecode: FileMetaData.Schema: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: FileMetaData.Schema: expected STRUCT elements, got %d", elemType)
			}
			if cap(fmd.Schema) >= size {
				fmd.Schema = fmd.Schema[:size]
			} else {
				fmd.Schema = make([]format.SchemaElement, size)
			}
			for i := range size {
				if err := b.decodeSchemaElement(&fmd.Schema[i]); err != nil {
					return err
				}
			}
		case 3: // NumRows
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: FileMetaData.NumRows: expected I64, got %d", typ)
			}
			fmd.NumRows, err = b.readI64()
		case 4: // RowGroups
			if typ != typeList {
				return fmt.Errorf("thriftdecode: FileMetaData.RowGroups: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: FileMetaData.RowGroups: expected STRUCT elements, got %d", elemType)
			}
			if cap(fmd.RowGroups) >= size {
				fmd.RowGroups = fmd.RowGroups[:size]
			} else {
				fmd.RowGroups = make([]format.RowGroup, size)
			}
			for i := range size {
				if err := b.decodeRowGroup(&fmd.RowGroups[i]); err != nil {
					return err
				}
			}
		case 5: // KeyValueMetadata
			if typ != typeList {
				return fmt.Errorf("thriftdecode: FileMetaData.KeyValueMetadata: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: FileMetaData.KeyValueMetadata: expected STRUCT elements, got %d", elemType)
			}
			if cap(fmd.KeyValueMetadata) >= size {
				fmd.KeyValueMetadata = fmd.KeyValueMetadata[:size]
			} else {
				fmd.KeyValueMetadata = make([]format.KeyValue, size)
			}
			for i := range size {
				if err := b.decodeKeyValue(&fmd.KeyValueMetadata[i]); err != nil {
					return err
				}
			}
		case 6: // CreatedBy
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: FileMetaData.CreatedBy: expected BINARY, got %d", typ)
			}
			fmd.CreatedBy, err = b.readStringRef()
		case 7: // ColumnOrders
			if typ != typeList {
				return fmt.Errorf("thriftdecode: FileMetaData.ColumnOrders: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeStruct {
				return fmt.Errorf("thriftdecode: FileMetaData.ColumnOrders: expected STRUCT elements, got %d", elemType)
			}
			if cap(fmd.ColumnOrders) >= size {
				fmd.ColumnOrders = fmd.ColumnOrders[:size]
			} else {
				fmd.ColumnOrders = make([]format.ColumnOrder, size)
			}
			for i := range size {
				if err := b.decodeColumnOrder(&fmd.ColumnOrders[i]); err != nil {
					return err
				}
			}
		case 8: // EncryptionAlgorithm
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: FileMetaData.EncryptionAlgorithm: expected STRUCT, got %d", typ)
			}
			err = b.decodeEncryptionAlgorithm(&fmd.EncryptionAlgorithm)
		case 9: // FooterSigningKeyMetadata
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: FileMetaData.FooterSigningKeyMetadata: expected BINARY, got %d", typ)
			}
			fmd.FooterSigningKeyMetadata, err = b.readBytesRef()
		default:
			err = b.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

// DecodeFileMetaData decodes a FileMetaData from the compact thrift data in src.
//
// The returned FileMetaData references data in src, so src must remain valid
// for the lifetime of the FileMetaData.
func DecodeFileMetaData(data []byte, fmd *format.FileMetaData) error {
	return (&buffer{data: data}).decodeFileMetaData(fmd)
}

// DecodeColumnIndex decodes a ColumnIndex from the compact thrift data in src.
//
// The returned ColumnIndex references data in src, so src must remain valid
// for the lifetime of the ColumnIndex.
func DecodeColumnIndex(data []byte, c *format.ColumnIndex) error {
	b := &buffer{data: data}
	var lastID int16

	for {
		id, typ, err := b.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}

		switch id {
		case 1:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnIndex.NullPages: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if cap(c.NullPages) >= size {
				c.NullPages = c.NullPages[:size]
			} else {
				c.NullPages = make([]bool, size)
			}
			switch elemType {
			case typeFalse, typeTrue:
				for i := range size {
					v, err := b.ReadByte()
					if err != nil {
						return err
					}
					c.NullPages[i] = v != 0 && v != 2
				}
			default:
				return fmt.Errorf("thriftdecode: ColumnIndex.NullPages: expected BOOL elements, got %d", elemType)
			}

		case 2:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnIndex.MinValues: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeBinary {
				return fmt.Errorf("thriftdecode: ColumnIndex.MinValues: expected BINARY elements, got %d", elemType)
			}
			if cap(c.MinValues) >= size {
				c.MinValues = c.MinValues[:size]
			} else {
				c.MinValues = make([][]byte, size)
			}
			for i := range size {
				c.MinValues[i], err = b.readBytesRef()
				if err != nil {
					return err
				}
			}

		case 3:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnIndex.MaxValues: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeBinary {
				return fmt.Errorf("thriftdecode: ColumnIndex.MaxValues: expected BINARY elements, got %d", elemType)
			}
			if cap(c.MaxValues) >= size {
				c.MaxValues = c.MaxValues[:size]
			} else {
				c.MaxValues = make([][]byte, size)
			}
			for i := range size {
				c.MaxValues[i], err = b.readBytesRef()
				if err != nil {
					return err
				}
			}

		case 4:
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: ColumnIndex.BoundaryOrder: expected I32, got %d", typ)
			}
			v, err := b.readI32()
			if err != nil {
				return err
			}
			c.BoundaryOrder = format.BoundaryOrder(v)

		case 5:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnIndex.NullCounts: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnIndex.NullCounts: expected I64 elements, got %d", elemType)
			}
			if cap(c.NullCounts) >= size {
				c.NullCounts = c.NullCounts[:size]
			} else {
				c.NullCounts = make([]int64, size)
			}
			for i := range size {
				c.NullCounts[i], err = b.readI64()
				if err != nil {
					return err
				}
			}

		case 6:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnIndex.RepetitionLevelHistogram: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnIndex.RepetitionLevelHistogram: expected I64 elements, got %d", elemType)
			}
			if cap(c.RepetitionLevelHistogram) >= size {
				c.RepetitionLevelHistogram = c.RepetitionLevelHistogram[:size]
			} else {
				c.RepetitionLevelHistogram = make([]int64, size)
			}
			for i := range size {
				c.RepetitionLevelHistogram[i], err = b.readI64()
				if err != nil {
					return err
				}
			}

		case 7:
			if typ != typeList {
				return fmt.Errorf("thriftdecode: ColumnIndex.DefinitionLevelHistogram: expected LIST, got %d", typ)
			}
			size, elemType, err := b.readList()
			if err != nil {
				return err
			}
			if elemType != typeI64 {
				return fmt.Errorf("thriftdecode: ColumnIndex.DefinitionLevelHistogram: expected I64 elements, got %d", elemType)
			}
			if cap(c.DefinitionLevelHistogram) >= size {
				c.DefinitionLevelHistogram = c.DefinitionLevelHistogram[:size]
			} else {
				c.DefinitionLevelHistogram = make([]int64, size)
			}
			for i := range size {
				c.DefinitionLevelHistogram[i], err = b.readI64()
				if err != nil {
					return err
				}
			}

		default:
			err = b.skipValue(typ)
		}

		if err != nil {
			return err
		}
		lastID = id
	}
}

// streamReader is a streaming thrift reader for use with buffered I/O.
type streamReader struct {
	r io.ByteReader
}

func (s *streamReader) readUvarint() (uint64, error) {
	var x uint64
	var shift uint
	for i := 0; ; i++ {
		b, err := s.r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			if i >= 10 || i == 9 && b > 1 {
				return 0, errors.New("thriftdecode: varint overflows uint64")
			}
			return x | uint64(b)<<shift, nil
		}
		x |= uint64(b&0x7f) << shift
		shift += 7
	}
}

func (s *streamReader) readVarint() (int64, error) {
	ux, err := s.readUvarint()
	if err != nil {
		return 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}

func (s *streamReader) readI32() (int32, error) {
	v, err := s.readVarint()
	return int32(v), err
}

func (s *streamReader) readField(lastID int16) (id int16, typ byte, err error) {
	v, err := s.r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	typ = v & 0x0F
	if typ == typeStop {
		return 0, typeStop, nil
	}

	delta := v >> 4
	if delta != 0 {
		id = lastID + int16(delta)
	} else {
		v, err := s.readVarint()
		if err != nil {
			return 0, 0, err
		}
		id = int16(v)
	}

	return id, typ, nil
}

func (s *streamReader) readLength() (int, error) {
	n, err := s.readUvarint()
	return int(n), err
}

func (s *streamReader) readBytes(buf []byte) error {
	if r, ok := s.r.(io.Reader); ok {
		_, err := io.ReadFull(r, buf)
		return err
	}
	for i := range buf {
		b, err := s.r.ReadByte()
		if err != nil {
			return err
		}
		buf[i] = b
	}
	return nil
}

func (s *streamReader) skipBytes() error {
	n, err := s.readLength()
	if err != nil {
		return err
	}
	for range n {
		if _, err := s.r.ReadByte(); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamReader) skipValue(typ byte) error {
	switch typ {
	case typeTrue, typeFalse:
		return nil
	case typeI8:
		_, err := s.r.ReadByte()
		return err
	case typeI16, typeI32, typeI64:
		_, err := s.readVarint()
		return err
	case typeDouble:
		for range 8 {
			if _, err := s.r.ReadByte(); err != nil {
				return err
			}
		}
		return nil
	case typeBinary:
		return s.skipBytes()
	case typeList, typeSet:
		v, err := s.r.ReadByte()
		if err != nil {
			return err
		}
		elemType := v & 0x0F
		size := int(v >> 4)
		if size == 0x0F {
			n, err := s.readUvarint()
			if err != nil {
				return err
			}
			size = int(n)
		}
		for range size {
			if err := s.skipValue(elemType); err != nil {
				return err
			}
		}
		return nil
	case typeStruct:
		return s.skipStruct()
	default:
		return fmt.Errorf("thriftdecode: unknown type %d", typ)
	}
}

func (s *streamReader) skipStruct() error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		if err := s.skipValue(typ); err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeDataPageHeader(h *format.DataPageHeader) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // NumValues
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeader.NumValues: expected I32, got %d", typ)
			}
			h.NumValues, err = s.readI32()
		case 2: // Encoding
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeader.Encoding: expected I32, got %d", typ)
			}
			v, err := s.readI32()
			if err != nil {
				return err
			}
			h.Encoding = format.Encoding(v)
		case 3: // DefinitionLevelEncoding
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeader.DefinitionLevelEncoding: expected I32, got %d", typ)
			}
			v, err := s.readI32()
			if err != nil {
				return err
			}
			h.DefinitionLevelEncoding = format.Encoding(v)
		case 4: // RepetitionLevelEncoding
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeader.RepetitionLevelEncoding: expected I32, got %d", typ)
			}
			v, err := s.readI32()
			if err != nil {
				return err
			}
			h.RepetitionLevelEncoding = format.Encoding(v)
		case 5: // Statistics
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: DataPageHeader.Statistics: expected STRUCT, got %d", typ)
			}
			err = s.decodeStatistics(&h.Statistics)
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeStatistics(st *format.Statistics) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Max
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.Max: expected BINARY, got %d", typ)
			}
			n, err := s.readLength()
			if err != nil {
				return err
			}
			if cap(st.Max) >= n {
				st.Max = st.Max[:n]
			} else {
				st.Max = make([]byte, n)
			}
			err = s.readBytes(st.Max)
		case 2: // Min
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.Min: expected BINARY, got %d", typ)
			}
			n, err := s.readLength()
			if err != nil {
				return err
			}
			if cap(st.Min) >= n {
				st.Min = st.Min[:n]
			} else {
				st.Min = make([]byte, n)
			}
			err = s.readBytes(st.Min)
		case 3: // NullCount
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: Statistics.NullCount: expected I64, got %d", typ)
			}
			st.NullCount, err = s.readVarint()
		case 4: // DistinctCount
			if typ != typeI64 {
				return fmt.Errorf("thriftdecode: Statistics.DistinctCount: expected I64, got %d", typ)
			}
			st.DistinctCount, err = s.readVarint()
		case 5: // MaxValue
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.MaxValue: expected BINARY, got %d", typ)
			}
			n, err := s.readLength()
			if err != nil {
				return err
			}
			if cap(st.MaxValue) >= n {
				st.MaxValue = st.MaxValue[:n]
			} else {
				st.MaxValue = make([]byte, n)
			}
			err = s.readBytes(st.MaxValue)
		case 6: // MinValue
			if typ != typeBinary {
				return fmt.Errorf("thriftdecode: Statistics.MinValue: expected BINARY, got %d", typ)
			}
			n, err := s.readLength()
			if err != nil {
				return err
			}
			if cap(st.MinValue) >= n {
				st.MinValue = st.MinValue[:n]
			} else {
				st.MinValue = make([]byte, n)
			}
			err = s.readBytes(st.MinValue)
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeDictionaryPageHeader(h *format.DictionaryPageHeader) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // NumValues
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DictionaryPageHeader.NumValues: expected I32, got %d", typ)
			}
			h.NumValues, err = s.readI32()
		case 2: // Encoding
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DictionaryPageHeader.Encoding: expected I32, got %d", typ)
			}
			v, err := s.readI32()
			if err != nil {
				return err
			}
			h.Encoding = format.Encoding(v)
		case 3: // IsSorted
			if typ != typeTrue && typ != typeFalse {
				return fmt.Errorf("thriftdecode: DictionaryPageHeader.IsSorted: expected BOOL, got %d", typ)
			}
			h.IsSorted = typ == typeTrue
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeDataPageHeaderV2(h *format.DataPageHeaderV2) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // NumValues
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.NumValues: expected I32, got %d", typ)
			}
			h.NumValues, err = s.readI32()
		case 2: // NumNulls
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.NumNulls: expected I32, got %d", typ)
			}
			h.NumNulls, err = s.readI32()
		case 3: // NumRows
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.NumRows: expected I32, got %d", typ)
			}
			h.NumRows, err = s.readI32()
		case 4: // Encoding
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.Encoding: expected I32, got %d", typ)
			}
			v, err := s.readI32()
			if err != nil {
				return err
			}
			h.Encoding = format.Encoding(v)
		case 5: // DefinitionLevelsByteLength
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.DefinitionLevelsByteLength: expected I32, got %d", typ)
			}
			h.DefinitionLevelsByteLength, err = s.readI32()
		case 6: // RepetitionLevelsByteLength
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.RepetitionLevelsByteLength: expected I32, got %d", typ)
			}
			h.RepetitionLevelsByteLength, err = s.readI32()
		case 7: // IsCompressed
			if typ != typeTrue && typ != typeFalse {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.IsCompressed: expected BOOL, got %d", typ)
			}
			v := typ == typeTrue
			h.IsCompressed = &v
		case 8: // Statistics
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: DataPageHeaderV2.Statistics: expected STRUCT, got %d", typ)
			}
			err = s.decodeStatistics(&h.Statistics)
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodePageHeader(h *format.PageHeader) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Type
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageHeader.Type: expected I32, got %d", typ)
			}
			v, err := s.readI32()
			if err != nil {
				return err
			}
			h.Type = format.PageType(v)
		case 2: // UncompressedPageSize
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageHeader.UncompressedPageSize: expected I32, got %d", typ)
			}
			h.UncompressedPageSize, err = s.readI32()
		case 3: // CompressedPageSize
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageHeader.CompressedPageSize: expected I32, got %d", typ)
			}
			h.CompressedPageSize, err = s.readI32()
		case 4: // CRC
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: PageHeader.CRC: expected I32, got %d", typ)
			}
			h.CRC, err = s.readI32()
		case 5: // DataPageHeader
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: PageHeader.DataPageHeader: expected STRUCT, got %d", typ)
			}
			if h.DataPageHeader == nil {
				h.DataPageHeader = &format.DataPageHeader{}
			}
			err = s.decodeDataPageHeader(h.DataPageHeader)
		case 6: // IndexPageHeader
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: PageHeader.IndexPageHeader: expected STRUCT, got %d", typ)
			}
			if h.IndexPageHeader == nil {
				h.IndexPageHeader = &format.IndexPageHeader{}
			}
			err = s.skipStruct() // IndexPageHeader is empty
		case 7: // DictionaryPageHeader
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: PageHeader.DictionaryPageHeader: expected STRUCT, got %d", typ)
			}
			if h.DictionaryPageHeader == nil {
				h.DictionaryPageHeader = &format.DictionaryPageHeader{}
			}
			err = s.decodeDictionaryPageHeader(h.DictionaryPageHeader)
		case 8: // DataPageHeaderV2
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: PageHeader.DataPageHeaderV2: expected STRUCT, got %d", typ)
			}
			if h.DataPageHeaderV2 == nil {
				h.DataPageHeaderV2 = &format.DataPageHeaderV2{}
			}
			err = s.decodeDataPageHeaderV2(h.DataPageHeaderV2)
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

// PageHeaderDecoder is an optimized decoder for PageHeader structs.
// It reads from a streaming source (e.g., bufio.Reader).
type PageHeaderDecoder struct {
	r streamReader
}

// NewPageHeaderDecoder creates a new PageHeaderDecoder that reads from r.
func NewPageHeaderDecoder(r io.ByteReader) *PageHeaderDecoder {
	return &PageHeaderDecoder{r: streamReader{r: r}}
}

// Decode decodes a PageHeader from the underlying reader.
func (d *PageHeaderDecoder) Decode(h *format.PageHeader) error {
	return d.r.decodePageHeader(h)
}

// Reset resets the decoder to read from a new reader.
func (d *PageHeaderDecoder) Reset(r io.ByteReader) {
	d.r.r = r
}

func (s *streamReader) decodeBloomFilterAlgorithm(a *format.BloomFilterAlgorithm) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Block
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: BloomFilterAlgorithm.Block: expected STRUCT, got %d", typ)
			}
			a.Block = &format.SplitBlockAlgorithm{}
			err = s.skipStruct()
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeBloomFilterHash(h *format.BloomFilterHash) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // XxHash
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: BloomFilterHash.XxHash: expected STRUCT, got %d", typ)
			}
			h.XxHash = &format.XxHash{}
			err = s.skipStruct()
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeBloomFilterCompression(c *format.BloomFilterCompression) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // Uncompressed
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: BloomFilterCompression.Uncompressed: expected STRUCT, got %d", typ)
			}
			c.Uncompressed = &format.BloomFilterUncompressed{}
			err = s.skipStruct()
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

func (s *streamReader) decodeBloomFilterHeader(h *format.BloomFilterHeader) error {
	var lastID int16
	for {
		id, typ, err := s.readField(lastID)
		if err != nil {
			return err
		}
		if typ == typeStop {
			return nil
		}
		switch id {
		case 1: // NumBytes
			if typ != typeI32 {
				return fmt.Errorf("thriftdecode: BloomFilterHeader.NumBytes: expected I32, got %d", typ)
			}
			h.NumBytes, err = s.readI32()
		case 2: // Algorithm
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: BloomFilterHeader.Algorithm: expected STRUCT, got %d", typ)
			}
			err = s.decodeBloomFilterAlgorithm(&h.Algorithm)
		case 3: // Hash
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: BloomFilterHeader.Hash: expected STRUCT, got %d", typ)
			}
			err = s.decodeBloomFilterHash(&h.Hash)
		case 4: // Compression
			if typ != typeStruct {
				return fmt.Errorf("thriftdecode: BloomFilterHeader.Compression: expected STRUCT, got %d", typ)
			}
			err = s.decodeBloomFilterCompression(&h.Compression)
		default:
			err = s.skipValue(typ)
		}
		if err != nil {
			return err
		}
		lastID = id
	}
}

// BloomFilterHeaderDecoder is an optimized decoder for BloomFilterHeader structs.
// It reads from a streaming source (e.g., bufio.Reader).
type BloomFilterHeaderDecoder struct {
	r streamReader
}

// NewBloomFilterHeaderDecoder creates a new BloomFilterHeaderDecoder that reads from r.
func NewBloomFilterHeaderDecoder(r io.ByteReader) *BloomFilterHeaderDecoder {
	return &BloomFilterHeaderDecoder{r: streamReader{r: r}}
}

// Decode decodes a BloomFilterHeader from the underlying reader.
func (d *BloomFilterHeaderDecoder) Decode(h *format.BloomFilterHeader) error {
	return d.r.decodeBloomFilterHeader(h)
}

// Reset resets the decoder to read from a new reader.
func (d *BloomFilterHeaderDecoder) Reset(r io.ByteReader) {
	d.r.r = r
}
