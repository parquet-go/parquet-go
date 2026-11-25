package delta

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestLengthByteArrayDecodeStartsAtDst(t *testing.T) {
	e := &LengthByteArrayEncoding{}
	counts := []struct {
		name string
		n    int
	}{
		{name: "zero", n: 0},
		{name: "one", n: 1},
		{name: "many", n: 300},
	}
	fills := []bool{false, true}
	modes := []string{"nil", "spare", "dirty", "exact", "short", "alias", "alias-short"}

	for _, count := range counts {
		for _, nonempty := range fills {
			if count.n == 0 && nonempty {
				continue
			}
			want, wantOffsets := byteArraySample(count.n, nonempty)
			encoded, err := e.EncodeByteArray(nil, want, wantOffsets)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range modes {
				name := count.name
				if nonempty {
					name += "/values/"
				} else {
					name += "/empty-values/"
				}
				name += mode
				t.Run(name, func(t *testing.T) {
					dst, src := destinationFor(mode, encoded, len(want))
					got, offsets, err := e.DecodeByteArray(dst, src, nil)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) || !equalOffsets(offsets, wantOffsets) {
						t.Fatalf("got %q %v, want %q %v", got, offsets, want, wantOffsets)
					}
					fits := cap(dst) >= len(want)
					switch {
					case fits && cap(dst) > 0:
						if unsafe.SliceData(got) != unsafe.SliceData(dst) {
							t.Fatal("result does not start at dst")
						}
						if cap(got) != cap(dst) {
							t.Fatalf("cap = %d, want %d", cap(got), cap(dst))
						}
					case fits:
						if len(got) != 0 {
							t.Fatalf("empty result = %q", got)
						}
					default:
						saved := append([]byte(nil), src...)
						clear(src)
						if !bytes.Equal(got, want) {
							t.Fatal("result aliases a destination that cannot hold it")
						}
						copy(src, saved)
						if cap(dst) > 0 && len(got) > 0 && unsafe.SliceData(got) == unsafe.SliceData(dst) {
							t.Fatal("undersized dst was reused")
						}
					}
				})
			}
		}
	}
}

func TestLengthByteArrayDecodeRetainsCap(t *testing.T) {
	e := &LengthByteArrayEncoding{}
	small, smallEncoded := mustEncodeSample(t, e, 1, true)
	large, largeEncoded := mustEncodeSample(t, e, 32, true)
	if len(large) <= len(small) {
		t.Fatal("sample did not grow")
	}

	t.Run("aliased-then-reuse", func(t *testing.T) {
		buf := make([]byte, len(smallEncoded), len(smallEncoded)+len(large)+64)
		copy(buf, smallEncoded)
		got, offsets, err := e.DecodeByteArray(buf, buf[:len(smallEncoded)], nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, small) {
			t.Fatalf("got %q", got)
		}
		if unsafe.SliceData(got) != unsafe.SliceData(buf) || cap(got) != cap(buf) {
			t.Fatalf("aliased result lost the destination buffer (len=%d cap=%d, buf cap=%d)", len(got), cap(got), cap(buf))
		}
		got, _, err = e.DecodeByteArray(got, largeEncoded, offsets)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, large) {
			t.Fatalf("reused decode = %q", got)
		}
		if unsafe.SliceData(got) != unsafe.SliceData(buf) {
			t.Fatal("reusing the aliased result allocated")
		}
	})

	t.Run("separate-then-reuse", func(t *testing.T) {
		dst := make([]byte, 0, len(large)+64)
		got, offsets, err := e.DecodeByteArray(dst, smallEncoded, nil)
		if err != nil {
			t.Fatal(err)
		}
		if unsafe.SliceData(got) != unsafe.SliceData(dst) {
			t.Fatal("separate decode did not start at dst")
		}
		got, _, err = e.DecodeByteArray(got, largeEncoded, offsets)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, large) || unsafe.SliceData(got) != unsafe.SliceData(dst) {
			t.Fatal("reusing a separate destination allocated")
		}
	})

	t.Run("exact-then-larger-allocates", func(t *testing.T) {
		dst := make([]byte, 0, len(small))
		got, offsets, err := e.DecodeByteArray(dst, smallEncoded, nil)
		if err != nil {
			t.Fatal(err)
		}
		if unsafe.SliceData(got) != unsafe.SliceData(dst) {
			t.Fatal("exact decode did not start at dst")
		}
		got, _, err = e.DecodeByteArray(got, largeEncoded, offsets)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, large) {
			t.Fatalf("larger decode = %q", got)
		}
		if len(got) > 0 && unsafe.SliceData(got) == unsafe.SliceData(dst) {
			t.Fatal("payload larger than cap reused dst")
		}
	})
}

func TestLengthByteArrayDecodeReuseNoAlloc(t *testing.T) {
	e := &LengthByteArrayEncoding{}
	for _, tc := range []struct {
		name     string
		n        int
		nonempty bool
		aliased  bool
	}{
		{name: "zero", n: 0},
		{name: "one-empty", n: 1},
		{name: "one", n: 1, nonempty: true},
		{name: "many-empty", n: 300},
		{name: "many", n: 300, nonempty: true},
		{name: "aliased-one", n: 1, nonempty: true, aliased: true},
		{name: "aliased-many", n: 300, nonempty: true, aliased: true},
		{name: "aliased-zero", n: 0, aliased: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, src := mustEncodeSample(t, e, tc.n, tc.nonempty)
			offsets := make([]uint32, 0, tc.n+2)
			var dst []byte
			var buf []byte
			if tc.aliased {
				buf = make([]byte, len(src))
			} else {
				dst = make([]byte, 0, len(src)+64)
			}
			allocs := testing.AllocsPerRun(100, func() {
				var err error
				if tc.aliased {
					copy(buf, src)
					dst, offsets, err = e.DecodeByteArray(buf, buf, offsets)
				} else {
					dst, offsets, err = e.DecodeByteArray(dst, src, offsets)
				}
				if err != nil {
					panic(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("allocs = %v", allocs)
			}
			if !tc.aliased && cap(dst) > 0 {
				base := unsafe.SliceData(dst)
				var err error
				dst, _, err = e.DecodeByteArray(dst, src, offsets)
				if err != nil {
					t.Fatal(err)
				}
				if unsafe.SliceData(dst) != base {
					t.Fatal("reused destination changed backing array")
				}
			}
		})
	}
}

func TestLengthByteArrayDecodeRejectsCorrupt(t *testing.T) {
	e := &LengthByteArrayEncoding{}
	_, valid := mustEncodeSample(t, e, 4, true)
	truncated := valid[:len(valid)/2]
	cases := [][]byte{
		nil,
		{},
		{0xff, 0xff, 0xff, 0xff, 0xff},
		encodeInt32(nil, []int32{-1}),
		encodeInt32(nil, []int32{1 << 30}),
		truncated,
	}
	for _, src := range cases {
		t.Run("", func(t *testing.T) {
			dst := bytes.Repeat([]byte{0x5A}, 32)
			saved := append([]byte(nil), dst...)
			if _, _, err := e.DecodeByteArray(dst, src, nil); err == nil {
				t.Fatalf("decode of %x succeeded", src)
			}
			if !bytes.Equal(dst, saved) {
				t.Fatal("failed decode wrote to dst")
			}

			aliased := append([]byte(nil), src...)
			aliasedSaved := append([]byte(nil), aliased...)
			if _, _, err := e.DecodeByteArray(aliased, aliased, nil); err == nil {
				t.Fatalf("aliased decode of %x succeeded", src)
			}
			if !bytes.Equal(aliased, aliasedSaved) {
				t.Fatal("failed aliased decode wrote to dst")
			}
		})
	}
}

func byteArraySample(n int, nonempty bool) ([]byte, []uint32) {
	offsets := make([]uint32, n+1)
	if n == 0 || !nonempty {
		return nil, offsets
	}
	var data []byte
	for i := range n {
		offsets[i] = uint32(len(data))
		data = append(data, byte('A'+i%26))
		// One long value makes the payload overlap the length header on an
		// aliased decode, and keeps a mix of empty and non-empty values.
		if i%5 == 0 {
			data = append(data, bytes.Repeat([]byte{'x'}, 64)...)
		}
		if i%7 == 0 && i != 0 {
			// empty value: offsets stay put until the next byte
			continue
		}
	}
	offsets[n] = uint32(len(data))
	return data, offsets
}

func mustEncodeSample(t *testing.T, e *LengthByteArrayEncoding, n int, nonempty bool) ([]byte, []byte) {
	t.Helper()
	data, offsets := byteArraySample(n, nonempty)
	encoded, err := e.EncodeByteArray(nil, data, offsets)
	if err != nil {
		t.Fatal(err)
	}
	return data, encoded
}

func destinationFor(mode string, encoded []byte, payloadLen int) (dst, src []byte) {
	src = append([]byte(nil), encoded...)
	switch mode {
	case "nil":
		return nil, src
	case "spare":
		return make([]byte, 0, payloadLen+64), src
	case "dirty":
		dst = bytes.Repeat([]byte{0xFF}, payloadLen+32)
		return dst, src
	case "exact":
		if payloadLen == 0 {
			return make([]byte, 0), src
		}
		return make([]byte, 0, payloadLen), src
	case "short":
		if payloadLen == 0 {
			return make([]byte, 0, 1), src
		}
		return make([]byte, 0, 1), src
	case "alias":
		buf := make([]byte, len(src), len(src)+payloadLen+64)
		copy(buf, src)
		return buf, buf
	case "alias-short":
		buf := make([]byte, len(src), len(src)+8)
		copy(buf, src)
		if payloadLen == 0 {
			return buf[:0:1], buf
		}
		return buf[:0:1], buf
	default:
		panic("unknown destination mode " + mode)
	}
}

func equalOffsets(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
