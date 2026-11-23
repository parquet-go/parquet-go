package thrift_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

const hugeSize = int32(math.MaxInt32)

// allocLimit is far below the allocation a decoder would make if it honored
// hugeSize directly, and above the scratch a rejected decode actually needs.
const allocLimit = 1 << 20

type sliceReader struct {
	b []byte
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

func writeField(p thrift.Protocol, w thrift.Writer, id int16, typ thrift.Type) error {
	// The first field's compact delta from zero is the field id itself.
	return w.WriteField(thrift.Field{ID: id, Type: typ})
}

func containerBytes(t *testing.T, p thrift.Protocol, write func(thrift.Writer) error) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := p.NewWriter(&buf)
	if err := write(w); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func allocatedDuring(fn func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func requireSmallAlloc(t *testing.T, declared int, fn func() error) {
	t.Helper()
	if err := fn(); err == nil {
		t.Fatal("expected an error")
	}
	var err error
	got := allocatedDuring(func() { err = fn() })
	if err == nil {
		t.Fatal("expected an error")
	}
	if got > allocLimit {
		t.Fatalf("allocated %d bytes for declared size %d (limit %d): %v", got, declared, allocLimit, err)
	}
	allocs := testing.AllocsPerRun(10, func() { _ = fn() })
	if allocs > 32 {
		t.Fatalf("AllocsPerRun = %g, want <= 32", allocs)
	}
}

type intList struct {
	Values []int32 `thrift:"1"`
}

type intSlice struct {
	Values thrift.Slice[int32] `thrift:"1"`
}

type intMap struct {
	Values map[int32]int32 `thrift:"1"`
}

type intSet struct {
	Values map[int32]struct{} `thrift:"1"`
}

type byteField struct {
	Value []byte `thrift:"1"`
}

type stringField struct {
	Value string `thrift:"1"`
}

func TestUnmarshalHugeContainerSizes(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			t.Run("list", func(t *testing.T) {
				in := containerBytes(t, p.proto, func(w thrift.Writer) error {
					if err := writeField(p.proto, w, 1, thrift.LIST); err != nil {
						return err
					}
					return w.WriteList(thrift.List{Size: hugeSize, Type: thrift.I32})
				})
				var seed intList
				if b, err := thrift.Marshal(p.proto, intList{Values: []int32{1}}); err != nil {
					t.Fatal(err)
				} else if err := thrift.Unmarshal(p.proto, b, &seed); err != nil {
					t.Fatal(err)
				}
				requireSmallAlloc(t, int(hugeSize), func() error {
					return thrift.Unmarshal(p.proto, in, new(intList))
				})
				got := new(intList)
				err := thrift.Unmarshal(p.proto, in, got)
				if err == nil || !strings.Contains(err.Error(), "exceeds remaining input") {
					t.Fatalf("error = %v", err)
				}
				if cap(got.Values) != 0 {
					t.Fatalf("cap = %d", cap(got.Values))
				}
			})

			t.Run("slice", func(t *testing.T) {
				in := containerBytes(t, p.proto, func(w thrift.Writer) error {
					if err := writeField(p.proto, w, 1, thrift.LIST); err != nil {
						return err
					}
					return w.WriteList(thrift.List{Size: hugeSize, Type: thrift.I32})
				})
				var seed intSlice
				if b, err := thrift.Marshal(p.proto, intSlice{Values: thrift.Slice[int32]{1}}); err != nil {
					t.Fatal(err)
				} else if err := thrift.Unmarshal(p.proto, b, &seed); err != nil {
					t.Fatal(err)
				}
				requireSmallAlloc(t, int(hugeSize), func() error {
					return thrift.Unmarshal(p.proto, in, new(intSlice))
				})
				got := new(intSlice)
				err := thrift.Unmarshal(p.proto, in, got)
				if err == nil || !strings.Contains(err.Error(), "exceeds remaining input") {
					t.Fatalf("error = %v", err)
				}
				if cap(got.Values) != 0 {
					t.Fatalf("cap = %d", cap(got.Values))
				}
			})

			t.Run("map", func(t *testing.T) {
				in := containerBytes(t, p.proto, func(w thrift.Writer) error {
					if err := writeField(p.proto, w, 1, thrift.MAP); err != nil {
						return err
					}
					return w.WriteMap(thrift.Map{Size: hugeSize, Key: thrift.I32, Value: thrift.I32})
				})
				var seed intMap
				if b, err := thrift.Marshal(p.proto, intMap{Values: map[int32]int32{1: 2}}); err != nil {
					t.Fatal(err)
				} else if err := thrift.Unmarshal(p.proto, b, &seed); err != nil {
					t.Fatal(err)
				}
				requireSmallAlloc(t, int(hugeSize), func() error {
					return thrift.Unmarshal(p.proto, in, new(intMap))
				})
				err := thrift.Unmarshal(p.proto, in, new(intMap))
				if err == nil || !strings.Contains(err.Error(), "exceeds remaining input") {
					t.Fatalf("error = %v", err)
				}
			})

			t.Run("set", func(t *testing.T) {
				in := containerBytes(t, p.proto, func(w thrift.Writer) error {
					if err := writeField(p.proto, w, 1, thrift.SET); err != nil {
						return err
					}
					return w.WriteSet(thrift.Set{Size: hugeSize, Type: thrift.I32})
				})
				var seed intSet
				if b, err := thrift.Marshal(p.proto, intSet{Values: map[int32]struct{}{1: {}}}); err != nil {
					t.Fatal(err)
				} else if err := thrift.Unmarshal(p.proto, b, &seed); err != nil {
					t.Fatal(err)
				}
				requireSmallAlloc(t, int(hugeSize), func() error {
					return thrift.Unmarshal(p.proto, in, new(intSet))
				})
				err := thrift.Unmarshal(p.proto, in, new(intSet))
				if err == nil || !strings.Contains(err.Error(), "exceeds remaining input") {
					t.Fatalf("error = %v", err)
				}
			})

			t.Run("bytes", func(t *testing.T) {
				in := containerBytes(t, p.proto, func(w thrift.Writer) error {
					if err := writeField(p.proto, w, 1, thrift.BINARY); err != nil {
						return err
					}
					return w.WriteLength(int(hugeSize))
				})
				var seed byteField
				if b, err := thrift.Marshal(p.proto, byteField{Value: []byte{1}}); err != nil {
					t.Fatal(err)
				} else if err := thrift.Unmarshal(p.proto, b, &seed); err != nil {
					t.Fatal(err)
				}
				requireSmallAlloc(t, int(hugeSize), func() error {
					return thrift.Unmarshal(p.proto, in, new(byteField))
				})
				err := thrift.Unmarshal(p.proto, in, new(byteField))
				if err == nil {
					t.Fatal("expected an error")
				}
			})

			t.Run("string", func(t *testing.T) {
				in := containerBytes(t, p.proto, func(w thrift.Writer) error {
					if err := writeField(p.proto, w, 1, thrift.BINARY); err != nil {
						return err
					}
					return w.WriteLength(int(hugeSize))
				})
				var seed stringField
				if b, err := thrift.Marshal(p.proto, stringField{Value: "a"}); err != nil {
					t.Fatal(err)
				} else if err := thrift.Unmarshal(p.proto, b, &seed); err != nil {
					t.Fatal(err)
				}
				requireSmallAlloc(t, int(hugeSize), func() error {
					return thrift.Unmarshal(p.proto, in, new(stringField))
				})
				err := thrift.Unmarshal(p.proto, in, new(stringField))
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})
	}
}

func TestUnmarshalNegativeContainerSize(t *testing.T) {
	for _, p := range protocols {
		if _, ok := p.proto.(*thrift.CompactProtocol); ok {
			// Compact sizes are unsigned. The binary protocol can carry a
			// negative count in the 32-bit size field.
			continue
		}
		t.Run(p.name, func(t *testing.T) {
			in := containerBytes(t, p.proto, func(w thrift.Writer) error {
				if err := writeField(p.proto, w, 1, thrift.LIST); err != nil {
					return err
				}
				return w.WriteList(thrift.List{Size: -1, Type: thrift.I32})
			})
			err := thrift.Unmarshal(p.proto, in, new(intList))
			if err == nil || !strings.Contains(err.Error(), "negative thrift list size") {
				t.Fatalf("error = %v", err)
			}
			in = containerBytes(t, p.proto, func(w thrift.Writer) error {
				if err := writeField(p.proto, w, 1, thrift.MAP); err != nil {
					return err
				}
				return w.WriteMap(thrift.Map{Size: -1, Key: thrift.I32, Value: thrift.I32})
			})
			err = thrift.Unmarshal(p.proto, in, new(intMap))
			if err == nil || !strings.Contains(err.Error(), "negative thrift map size") {
				t.Fatalf("error = %v", err)
			}
			in = containerBytes(t, p.proto, func(w thrift.Writer) error {
				if err := writeField(p.proto, w, 1, thrift.SET); err != nil {
					return err
				}
				return w.WriteSet(thrift.Set{Size: -1, Type: thrift.I32})
			})
			err = thrift.Unmarshal(p.proto, in, new(intSet))
			if err == nil || !strings.Contains(err.Error(), "negative thrift set size") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestUnmarshalLargeContainers(t *testing.T) {
	const n = 5000
	ints := make([]int32, n)
	m := make(map[int32]int32, n)
	set := make(map[int32]struct{}, n)
	for i := range ints {
		ints[i] = int32(i)
		m[int32(i)] = int32(n - i)
		set[int32(i)] = struct{}{}
	}
	items := make([]structItem, n)
	for i := range items {
		items[i] = structItem{A: int32(i), B: "x"}
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			assertRoundTrip(t, p.proto, intList{Values: ints})
			assertRoundTrip(t, p.proto, intSlice{Values: thrift.Slice[int32](ints)})
			assertRoundTrip(t, p.proto, intMap{Values: m})
			assertRoundTrip(t, p.proto, intSet{Values: set})
			assertRoundTrip(t, p.proto, structList{Values: items})
			assertRoundTrip(t, p.proto, structSlice{Values: thrift.Slice[structItem](items)})

			var reused intSlice
			large, err := thrift.Marshal(p.proto, intSlice{Values: thrift.Slice[int32](ints)})
			if err != nil {
				t.Fatal(err)
			}
			if err := thrift.Unmarshal(p.proto, large, &reused); err != nil {
				t.Fatal(err)
			}
			small := intSlice{Values: thrift.Slice[int32]{7, 8, 9}}
			short, err := thrift.Marshal(p.proto, small)
			if err != nil {
				t.Fatal(err)
			}
			if err := thrift.Unmarshal(p.proto, short, &reused); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(reused, small) {
				t.Fatalf("reused slice = %#v, want %#v", reused.Values, small.Values)
			}
			if err := thrift.Unmarshal(p.proto, large, &reused); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual([]int32(reused.Values), ints) {
				t.Fatal("large slice did not round-trip after reuse")
			}
		})
	}
}

type structItem struct {
	A int32  `thrift:"1"`
	B string `thrift:"2"`
}

type structList struct {
	Values []structItem `thrift:"1"`
}

type structSlice struct {
	Values thrift.Slice[structItem] `thrift:"1"`
}

func assertRoundTrip(t *testing.T, p thrift.Protocol, value any) {
	t.Helper()
	b, err := thrift.Marshal(p, value)
	if err != nil {
		t.Fatal(err)
	}
	got := reflect.New(reflect.TypeOf(value))
	if err := thrift.Unmarshal(p, b, got.Interface()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value, got.Elem().Interface()) {
		t.Fatalf("value mismatch:\nwant: %#v\ngot:  %#v", value, got.Elem().Interface())
	}
}

func TestDecodeHugeContainersStreaming(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			cases := []struct {
				name  string
				in    []byte
				warm  any
				value func() any
				capOf func(any) int
			}{
				{
					name: "list",
					in: containerBytes(t, p.proto, func(w thrift.Writer) error {
						if err := writeField(p.proto, w, 1, thrift.LIST); err != nil {
							return err
						}
						return w.WriteList(thrift.List{Size: hugeSize, Type: thrift.I32})
					}),
					warm:  intList{Values: []int32{1}},
					value: func() any { return new(intList) },
					capOf: func(v any) int { return cap(v.(*intList).Values) },
				},
				{
					name: "slice",
					in: containerBytes(t, p.proto, func(w thrift.Writer) error {
						if err := writeField(p.proto, w, 1, thrift.LIST); err != nil {
							return err
						}
						return w.WriteList(thrift.List{Size: hugeSize, Type: thrift.I32})
					}),
					warm:  intSlice{Values: thrift.Slice[int32]{1}},
					value: func() any { return new(intSlice) },
					capOf: func(v any) int { return cap(v.(*intSlice).Values) },
				},
				{
					name: "map",
					in: containerBytes(t, p.proto, func(w thrift.Writer) error {
						if err := writeField(p.proto, w, 1, thrift.MAP); err != nil {
							return err
						}
						return w.WriteMap(thrift.Map{Size: hugeSize, Key: thrift.I32, Value: thrift.I32})
					}),
					warm:  intMap{Values: map[int32]int32{1: 2}},
					value: func() any { return new(intMap) },
				},
				{
					name: "set",
					in: containerBytes(t, p.proto, func(w thrift.Writer) error {
						if err := writeField(p.proto, w, 1, thrift.SET); err != nil {
							return err
						}
						return w.WriteSet(thrift.Set{Size: hugeSize, Type: thrift.I32})
					}),
					warm:  intSet{Values: map[int32]struct{}{1: {}}},
					value: func() any { return new(intSet) },
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					b, err := thrift.Marshal(p.proto, tc.warm)
					if err != nil {
						t.Fatal(err)
					}
					if err := thrift.NewDecoder(p.proto.NewReader(bytes.NewReader(b))).Decode(reflect.New(reflect.TypeOf(tc.warm)).Interface()); err != nil {
						t.Fatal(err)
					}
					decode := func() error {
						return thrift.NewDecoder(p.proto.NewReader(&sliceReader{b: bytes.Clone(tc.in)})).Decode(tc.value())
					}
					requireSmallAlloc(t, int(hugeSize), decode)
					got := tc.value()
					if err := thrift.NewDecoder(p.proto.NewReader(&sliceReader{b: bytes.Clone(tc.in)})).Decode(got); err == nil {
						t.Fatal("expected an error")
					}
					if tc.capOf != nil && tc.capOf(got) > 8192 {
						t.Fatalf("cap = %d", tc.capOf(got))
					}
				})
			}
		})
	}
}

func TestReadBytesHugeLength(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := p.proto.NewWriter(&buf)
			if err := w.WriteLength(int(hugeSize)); err != nil {
				t.Fatal(err)
			}
			encoded := buf.Bytes()

			readers := []struct {
				name string
				new  func() thrift.Reader
			}{
				{name: "bytes", new: func() thrift.Reader { return p.proto.NewReaderFromBytes(encoded) }},
				{name: "known", new: func() thrift.Reader { return p.proto.NewReader(bytes.NewReader(encoded)) }},
				{name: "stream", new: func() thrift.Reader { return p.proto.NewReader(&sliceReader{b: bytes.Clone(encoded)}) }},
			}
			for _, r := range readers {
				t.Run(r.name, func(t *testing.T) {
					requireSmallAlloc(t, int(hugeSize), func() error {
						_, err := r.new().ReadBytes()
						return err
					})
					got, err := r.new().ReadBytes()
					if err == nil {
						t.Fatal("expected an error")
					}
					if got != nil {
						t.Fatalf("got %d bytes", len(got))
					}
				})
			}
		})
	}
}

func TestReadBytesLargerThanPrealloc(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 5000)
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := p.proto.NewWriter(&buf)
			if err := w.WriteBytes(payload); err != nil {
				t.Fatal(err)
			}
			encoded := buf.Bytes()
			readers := []func() thrift.Reader{
				func() thrift.Reader { return p.proto.NewReaderFromBytes(encoded) },
				func() thrift.Reader { return p.proto.NewReader(bytes.NewReader(encoded)) },
				func() thrift.Reader { return p.proto.NewReader(&sliceReader{b: bytes.Clone(encoded)}) },
			}
			for _, newReader := range readers {
				got, err := newReader().ReadBytes()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("len %d", len(got))
				}
			}
		})
	}
}

func TestReadMessageHugeName(t *testing.T) {
	p := &thrift.BinaryProtocol{NonStrict: true}
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(hugeSize))
	readers := []func() thrift.Reader{
		func() thrift.Reader { return p.NewReaderFromBytes(encoded[:]) },
		func() thrift.Reader { return p.NewReader(bytes.NewReader(encoded[:])) },
		func() thrift.Reader { return p.NewReader(&sliceReader{b: bytes.Clone(encoded[:])}) },
	}
	for _, newReader := range readers {
		requireSmallAlloc(t, int(hugeSize), func() error {
			_, err := newReader().ReadMessage()
			return err
		})
		_, err := newReader().ReadMessage()
		if err == nil {
			t.Fatal("expected an error")
		}
	}
}
