package parquet

import (
	"fmt"
	"reflect"
	"strings"
)

type Tag struct {
	s strings.Builder
}

func (t *Tag) Add(tag string) {
	if t.s.Len() > 0 {
		t.s.WriteByte(',')
	}
	t.s.WriteString(tag)
}

type TagOptions func(*Tag)

func WithOptional() TagOptions {
	return func(t *Tag) {
		t.Add("optional")
	}
}

func WithEnum() TagOptions {
	return func(t *Tag) {
		t.Add("enum")
	}
}

func WithUUID() TagOptions {
	return func(t *Tag) {
		t.Add("uuid")
	}
}

func WithDate() TagOptions {
	return func(t *Tag) {
		t.Add("date")
	}
}

func WithTimestamp() TagOptions {
	return func(t *Tag) {
		t.Add("timestamp")
	}
}

type CompressionType uint

const (
	UncompressedType CompressionType = 1 + iota
	SnappyType
	GzipType
	BrotliType
	Lz4Type
	ZstdType
)

func (c CompressionType) String() string {
	switch c {
	case UncompressedType:
		return "uncompressed"
	case SnappyType:
		return "snappy"
	case GzipType:
		return "gzip"
	case Lz4Type:
		return "lz4"
	case ZstdType:
		return "zstd"
	default:
		return ""
	}
}

func WithCompression(compressionType CompressionType) TagOptions {
	return func(t *Tag) {
		t.Add(compressionType.String())
	}
}

type EncodingType uint

const (
	PlainType EncodingType = 1 + iota
	DictType
	DeltaType
	SplitType
)

func (e EncodingType) String() string {
	switch e {
	case PlainType:
		return "plain"
	case DictType:
		return "dict"
	case DeltaType:
		return "delta"
	case SplitType:
		return "split"
	default:
		return ""
	}
}

func WithEncoding(encodingType EncodingType) TagOptions {
	return func(t *Tag) {
		t.Add(encodingType.String())
	}
}

func WithDecimal(scale, precision int) TagOptions {
	return func(t *Tag) {
		t.Add(fmt.Sprintf("decimal(%d:%d)", scale, precision))
	}
}

// ProtobufTagProvider implements tagSource by reading protobuf tag and
// constructing parquet tags from it.
//
// Protobuf field name is used as column field name.
type ProtobufTagProvider struct {
	// Implementation can decide to set additional field tags  based on on f.
	Resolve func(f *reflect.StructField, o *ResolveOptions)
}

// Tags builds parquet tags based on protobuf struct tag.
func (p *ProtobufTagProvider) Tags(f *reflect.StructField) [3]string {
	proto := f.Tag.Get("protobuf")
	if proto == "" {
		return [3]string{}
	}
	var name string
	for _, v := range strings.Split(proto, ",") {
		if strings.HasPrefix(v, "name=") {
			_, name, _ = strings.Cut(v, "=")
			break
		}
	}
	var r ResolveOptions
	if p.Resolve != nil {
		p.Resolve(f, &r)
	}
	return r.Tags(name)
}

type ResolveOptions struct {
	Parquet  []TagOptions
	MapKey   []TagOptions
	MapValue []TagOptions
}

func (r *ResolveOptions) Tags(name string) [3]string {
	var t Tag
	t.Add(name)
	for i := range r.Parquet {
		r.Parquet[i](&t)
	}
	parquetTag := t.s.String()
	t.s.Reset()
	for i := range r.MapKey {
		r.MapKey[i](&t)
	}
	mapKeyTag := t.s.String()
	t.s.Reset()
	for i := range r.MapValue {
		r.MapValue[i](&t)
	}
	mapValueTag := t.s.String()
	return [3]string{parquetTag, mapKeyTag, mapValueTag}
}
