package delta

import (
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	"github.com/parquet-go/parquet-go/internal/deltalength"
)

type LengthByteArrayEncoding struct {
	encoding.NotSupported
}

func init() {
	// Page decoding borrows this view. DecodeByteArray itself always returns a
	// slice that starts at the caller's destination.
	deltalength.PayloadView = lengthByteArrayPayload
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
	dst = dst[:0]
	payload, offsets, err := lengthByteArrayPayload(src, offsets)
	if err != nil {
		return dst, offsets, e.wrap(err)
	}
	return placeByteArrayPayload(dst, src, payload), offsets, nil
}

// lengthByteArrayPayload decodes the length header of a DELTA_LENGTH_BYTE_ARRAY
// page. payload is a subslice of src with no spare capacity, or nil when the
// page has no value bytes. offsets are indexes into payload.
func lengthByteArrayPayload(src []byte, offsets []uint32) ([]byte, []uint32, error) {
	offsets = offsets[:0]

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	payload, err := length.decode(src)
	if err != nil {
		return nil, offsets, err
	}

	if size := len(length.values) + 1; cap(offsets) < size {
		offsets = make([]uint32, size, 2*size)
	} else {
		offsets = offsets[:size]
	}

	lastOffset, invalidLength := decodeByteArrayLengths(offsets, length.values)
	if invalidLength != 0 {
		return nil, offsets, errInvalidNegativeValueLength(int(invalidLength))
	}
	if int(lastOffset) > len(payload) {
		return nil, offsets, errValueLengthOutOfBounds(int(lastOffset), len(payload))
	}
	if lastOffset == 0 {
		return nil, offsets, nil
	}
	return payload[:lastOffset:lastOffset], offsets, nil
}

// placeByteArrayPayload copies payload onto dst. The result starts at dst[0]
// when cap(dst) holds the payload. An aliased dst overlaps payload, so the
// bytes are moved with copy.
func placeByteArrayPayload(dst, src, payload []byte) []byte {
	if len(payload) == 0 {
		return dst
	}
	if cap(dst) >= len(payload) && sliceAlias(dst, src) {
		copy(dst[:len(payload)], payload)
		return dst[:len(payload)]
	}
	return append(dst, payload...)
}

func sliceAlias(dst, src []byte) bool {
	return cap(dst) > 0 && cap(src) > 0 && unsafe.SliceData(dst) == unsafe.SliceData(src)
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
