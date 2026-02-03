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
