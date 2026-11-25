package delta

import (
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

type LengthByteArrayEncoding struct {
	encoding.NotSupported
}

func (e *LengthByteArrayEncoding) String() string {
	return "DELTA_LENGTH_BYTE_ARRAY"
}

func (e *LengthByteArrayEncoding) Encoding() format.Encoding {
	return format.DeltaLengthByteArray
}

func (e *LengthByteArrayEncoding) EncodeByteArray(dst []byte, src []byte, offsets []uint32) ([]byte, error) {
	if len(offsets) == 0 {
		return dst[:0], nil
	}

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	length.resize(len(offsets) - 1)
	encodeByteArrayLengths(length.values, offsets)

	dst = dst[:0]
	dst = encodeInt32(dst, length.values)
	dst = append(dst, src...)
	return dst, nil
}

func (e *LengthByteArrayEncoding) DecodeByteArray(dst []byte, src []byte, offsets []uint32) ([]byte, []uint32, error) {
	dst, offsets = dst[:0], offsets[:0]

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	payload, err := length.decode(src)
	if err != nil {
		return dst, offsets, e.wrap(err)
	}

	if size := len(length.values) + 1; cap(offsets) < size {
		offsets = make([]uint32, size, 2*size)
	} else {
		offsets = offsets[:size]
	}

	lastOffset, invalidLength := decodeByteArrayLengths(offsets, length.values)
	if invalidLength != 0 {
		return dst, offsets, e.wrap(errInvalidNegativeValueLength(int(invalidLength)))
	}
	if int(lastOffset) > len(payload) {
		return dst, offsets, e.wrap(errValueLengthOutOfBounds(int(lastOffset), len(payload)))
	}

	if lastOffset > 0 && cap(dst) >= len(src)-len(payload)+int(lastOffset) && unsafe.SliceData(dst) == unsafe.SliceData(src) {
		return payload[:lastOffset:lastOffset], offsets, nil
	}
	return append(dst, payload[:lastOffset]...), offsets, nil
}

func (e *LengthByteArrayEncoding) EstimateDecodeByteArraySize(src []byte) int {
	length := getInt32Buffer()
	defer putInt32Buffer(length)
	payload, err := length.decode(src)
	if err != nil {
		return 0
	}
	// Values are the payload after the length list. A sum that fits in the
	// whole page can still be larger than those bytes.
	return sumNonNegative(length.values, len(payload))
}

func (e *LengthByteArrayEncoding) CanDecodeInPlace() bool {
	return true
}

func (e *LengthByteArrayEncoding) wrap(err error) error {
	if err != nil {
		err = encoding.Error(e, err)
	}
	return err
}
