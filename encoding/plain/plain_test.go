package plain_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/plain"
)

func TestAppendBoolean(t *testing.T) {
	values := []byte{}

	for i := range 100 {
		values = plain.AppendBoolean(values, i, (i%2) != 0)
	}

	if !bytes.Equal(values, []byte{
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b10101010,
		0b00001010,
	}) {
		t.Errorf("%08b\n", values)
	}
}
