package variant

import (
	"fmt"
	"reflect"
	"strings"
)

// Marshal converts a Go value to variant binary encoding, returning the
// metadata and value byte slices.
func Marshal(v any) (metadata, value []byte, err error) {
	var b MetadataBuilder
	enc := encoder{
		b:       &b,
		scratch: make([]byte, 0, 1024),
	}
	start, end, err := enc.encodeReflect(reflect.ValueOf(v))
	if err != nil {
		return nil, nil, err
	}
	valueBytes := make([]byte, end-start)
	copy(valueBytes, enc.scratch[start:end])

	_, metadataBytes := b.Build()
	return metadataBytes, valueBytes, nil
}

// Unmarshal decodes variant binary data into a Go value.
func Unmarshal(metadata, value []byte) (any, error) {
	m, err := DecodeMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("variant unmarshal: %w", err)
	}
	v, err := Decode(m, value)
	if err != nil {
		return nil, fmt.Errorf("variant unmarshal: %w", err)
	}
	return v.GoValue(), nil
}

// fieldName returns the name to use for a struct field, checking variant and
// json struct tags.
func fieldName(sf reflect.StructField) string {
	if tag, ok := sf.Tag.Lookup("variant"); ok {
		name, _, _ := strings.Cut(tag, ",")
		if name != "" {
			return name
		}
	}
	if tag, ok := sf.Tag.Lookup("json"); ok {
		name, _, _ := strings.Cut(tag, ",")
		if name != "" {
			return name
		}
	}
	return sf.Name
}
