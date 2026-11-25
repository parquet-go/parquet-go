//go:build !purego

package parquet

import "github.com/parquet-go/parquet-go/sparse"

func nullIndexBool(bits []uint64, rows sparse.Array) {
	nullIndex8(&bits[0], rows)
}

func nullIndexInt(bits []uint64, rows sparse.Array) {
	nullIndex64(&bits[0], rows)
}

func nullIndexInt8(bits []uint64, rows sparse.Array) {
	nullIndex8(&bits[0], rows)
}

func nullIndexInt16(bits []uint64, rows sparse.Array) {
	nullIndex[int16](bits, rows)
}

func nullIndexInt32(bits []uint64, rows sparse.Array) {
	nullIndex32(&bits[0], rows)
}

func nullIndexInt64(bits []uint64, rows sparse.Array) {
	nullIndex64(&bits[0], rows)
}

func nullIndexUint(bits []uint64, rows sparse.Array) {
	nullIndex64(&bits[0], rows)
}

func nullIndexUint8(bits []uint64, rows sparse.Array) {
	nullIndex8(&bits[0], rows)
}

func nullIndexUint16(bits []uint64, rows sparse.Array) {
	nullIndex[uint16](bits, rows)
}

func nullIndexUint32(bits []uint64, rows sparse.Array) {
	nullIndex32(&bits[0], rows)
}

func nullIndexUint64(bits []uint64, rows sparse.Array) {
	nullIndex64(&bits[0], rows)
}

func nullIndexUint128(bits []uint64, rows sparse.Array) {
	nullIndex128(&bits[0], rows)
}

func nullIndexFloat32(bits []uint64, rows sparse.Array) {
	nullIndex32(&bits[0], rows)
}

func nullIndexFloat64(bits []uint64, rows sparse.Array) {
	nullIndex64(&bits[0], rows)
}

func nullIndexPointer(bits []uint64, rows sparse.Array) {
	nullIndex64(&bits[0], rows)
}
