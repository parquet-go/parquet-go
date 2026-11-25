// Package deltalength carries the DELTA_LENGTH_BYTE_ARRAY payload view used
// while decoding a column page. The view is a subslice of the encoded page,
// so it is not a result of encoding.Encoding.DecodeByteArray.
package deltalength

// PayloadView decodes src and returns the concatenated values as a subslice
// of src. The subslice has no spare capacity past the payload. offsets are
// indexes into that subslice. A page with no value bytes returns a nil payload.
//
// The encoding/delta package sets PayloadView from init.
var PayloadView func(src []byte, offsets []uint32) (payload []byte, offsetsOut []uint32, err error)
