package schema

import "reflect"

// Options provides configurations for creating a new *parquet.Schema.
type Options struct {
	tagSource TagSource
}

// TagSource returns configured TagSource.
func (o *Options) TagSource() TagSource {
	return o.tagSource
}

// Apply applies opts on o and returning o with the modified settings.
func (o *Options) Apply(opts ...Option) *Options {
	for _, f := range opts {
		f(o)
	}
	return o
}

// DefaultOptions returns *Options with default settings.
func DefaultOptions() *Options {
	return &Options{tagSource: defaultTagSource{}}
}

// Option is an interface for passing *parquet.Schema configuration options.
type Option func(*Options)

// WithTagSource returns an Option that sets source as the default TagSource.
func WithTagSource(source TagSource) Option {
	return func(o *Options) {
		o.tagSource = source
	}
}

// StructTag contains struct tag values that are use by parquet-go to configure
// nodes.
type StructTag struct {
	// Value set on parquet struct tag
	Parquet string
	// Value set on parquet-key struct tag
	MapKey string
	// Value set on parquet-value struct tag
	MapValue string
}

// TagSource is an interface for supplying metadata information about fields to
// *parquet.Schema.
type TagSource interface {
	Tags(f reflect.StructField) StructTag
	isTagSource()
}

type defaultTagSource struct{}

var _ TagSource = defaultTagSource{}

func (defaultTagSource) Tags(f reflect.StructField) StructTag {
	return StructTag{
		Parquet:  f.Tag.Get("parquet"),
		MapKey:   f.Tag.Get("parquet-key"),
		MapValue: f.Tag.Get("parquet-value"),
	}
}

func (defaultTagSource) isTagSource() {}
