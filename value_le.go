//go:build !s390x

package parquet

const (
	firstByteOffsetOfBooleanValue = 0
	firstByteOffsetOf32BitsValue  = 0
)

// valueWords packs the level word of a Value (kind, definition level,
// repetition level, column index) for a single store.
func valueWords(kind int8, definitionLevel, repetitionLevel byte, columnIndex uint16) uint64 {
	return uint64(uint8(kind)) |
		uint64(definitionLevel)<<8 |
		uint64(repetitionLevel)<<16 |
		uint64(columnIndex)<<32
}
