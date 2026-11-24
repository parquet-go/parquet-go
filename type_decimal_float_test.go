package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"runtime"
	"testing"
)

func captureDecimal(node Node, value any) (out []byte, panicked any) {
	defer func() { panicked = recover() }()
	schema := NewSchema("row", Group{"amount": node})
	cols := makeColumnBuffersForSchema(schema)
	_, write := writeValueFuncOf(0, schema)
	write(cols, columnLevels{}, reflect.ValueOf(map[string]any{"amount": value}))
	if cols[0].Len() != 1 {
		panic(fmt.Sprintf("wrote %d values", cols[0].Len()))
	}
	vals := make([]Value, 1)
	if n, err := cols[0].ReadValuesAt(vals, 0); err != nil || n != 1 {
		panic(fmt.Sprintf("read: n=%d err=%v", n, err))
	}
	switch node.Type().Kind() {
	case Int32:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(vals[0].Int32()))
		return append([]byte(nil), b[:]...), nil
	case Int64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(vals[0].Int64()))
		return append([]byte(nil), b[:]...), nil
	default:
		return append([]byte(nil), vals[0].ByteArray()...), nil
	}
}

func fixedBytesFromInt64(width int, v int64) []byte {
	var eight [8]byte
	binary.BigEndian.PutUint64(eight[:], uint64(v))
	if width == 8 {
		return append([]byte(nil), eight[:]...)
	}
	if width > 8 {
		buf := make([]byte, width)
		if v < 0 {
			for i := range width - 8 {
				buf[i] = 0xFF
			}
		}
		copy(buf[width-8:], eight[:])
		return buf
	}
	return append([]byte(nil), eight[8-width:]...)
}

func TestFixedLenDecimalNonFiniteFloat(t *testing.T) {
	values := []struct {
		name string
		v    any
		f    float64
	}{
		{name: "float64 NaN", v: math.NaN(), f: math.NaN()},
		{name: "float64 +Inf", v: math.Inf(1), f: math.Inf(1)},
		{name: "float64 -Inf", v: math.Inf(-1), f: math.Inf(-1)},
		{name: "float32 NaN", v: float32(math.NaN()), f: float64(float32(math.NaN()))},
		{name: "float32 +Inf", v: float32(math.Inf(1)), f: float64(float32(math.Inf(1)))},
		{name: "float32 -Inf", v: float32(math.Inf(-1)), f: float64(float32(math.Inf(-1)))},
	}
	for _, scale := range []int{0, 2} {
		for _, width := range []int{1, 4, 8, 16} {
			for _, val := range values {
				t.Run(fmt.Sprintf("scale=%d/width=%d/%s", scale, width, val.name), func(t *testing.T) {
					refNode := Decimal(scale, 18, Int64Type)
					refBytes, panicked := captureDecimal(refNode, val.v)
					if panicked != nil {
						t.Fatalf("int64 path panicked: %v", panicked)
					}
					ref := int64(binary.BigEndian.Uint64(refBytes))
					if runtime.GOARCH == "amd64" && ref != math.MinInt64 {
						t.Fatalf("int64 path = %d, want %d", ref, int64(math.MinInt64))
					}
					if got := int64(math.Round(val.f * math.Pow10(scale))); got != ref {
						t.Fatalf("int64 path = %d, conversion = %d", ref, got)
					}

					ba, panicked := captureDecimal(Decimal(scale, 18, ByteArrayType), val.v)
					if panicked != nil {
						t.Fatalf("byte array path panicked: %v", panicked)
					}
					if !bytes.Equal(ba, refBytes) {
						t.Fatalf("byte array = %x, want %x", ba, refBytes)
					}

					if width == 4 {
						i32, panicked := captureDecimal(Decimal(scale, 9, Int32Type), val.v)
						if panicked != nil {
							t.Fatalf("int32 path panicked: %v", panicked)
						}
						want := fixedBytesFromInt64(4, ref)
						if !bytes.Equal(i32, want) {
							t.Fatalf("int32 = %x, want %x", i32, want)
						}
					}

					got, panicked := captureDecimal(Decimal(scale, 38, FixedLenByteArrayType(width)), val.v)
					if panicked != nil {
						t.Fatalf("fixed path panicked: %v", panicked)
					}
					want := fixedBytesFromInt64(width, ref)
					if !bytes.Equal(got, want) {
						t.Fatalf("fixed = %x, want %x", got, want)
					}
				})
			}
		}
	}

	for _, sign := range []int{1, -1} {
		t.Run(fmt.Sprintf("big.Float Inf %d", sign), func(t *testing.T) {
			f := new(big.Float).SetInf(sign < 0)
			floatRef, panicked := captureDecimal(Decimal(0, 38, FixedLenByteArrayType(16)), math.Inf(sign))
			if panicked != nil {
				t.Fatalf("float64 path panicked: %v", panicked)
			}
			got, panicked := captureDecimal(Decimal(0, 38, FixedLenByteArrayType(16)), f)
			if panicked != nil {
				t.Fatalf("big.Float path panicked: %v", panicked)
			}
			if !bytes.Equal(got, floatRef) {
				t.Fatalf("big.Float = %x, float64 = %x", got, floatRef)
			}
		})
	}
}

func TestFixedLenDecimalMostNegative(t *testing.T) {
	for width := 1; width <= 16; width++ {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			shift := uint(8*width - 1)
			mag := new(big.Int).Lsh(big.NewInt(1), shift)
			neg := new(big.Int).Neg(mag)
			posMax := new(big.Int).Sub(mag, big.NewInt(1))
			tooNeg := new(big.Int).Sub(neg, big.NewInt(1))
			tooPos := new(big.Int).Add(posMax, big.NewInt(1))

			node := Decimal(0, 38, FixedLenByteArrayType(width))
			wantNeg := make([]byte, width)
			wantNeg[0] = 0x80
			wantPos := bytes.Repeat([]byte{0xFF}, width)
			wantPos[0] = 0x7F

			bigNeg := new(big.Float).SetPrec(256).SetInt(neg)
			bigPos := new(big.Float).SetPrec(256).SetInt(posMax)
			for _, val := range []any{bigNeg, math.Ldexp(-1, int(shift)), float32(math.Ldexp(-1, int(shift)))} {
				got, panicked := captureDecimal(node, val)
				if panicked != nil {
					t.Fatalf("%T %v panicked: %v", val, val, panicked)
				}
				if !bytes.Equal(got, wantNeg) {
					t.Fatalf("%T = %x, want %x", val, got, wantNeg)
				}
			}
			got, panicked := captureDecimal(node, bigPos)
			if panicked != nil {
				t.Fatalf("max positive panicked: %v", panicked)
			}
			if !bytes.Equal(got, wantPos) {
				t.Fatalf("max positive = %x, want %x", got, wantPos)
			}

			for _, val := range []any{
				new(big.Float).SetPrec(256).SetInt(tooNeg),
				new(big.Float).SetPrec(256).SetInt(tooPos),
			} {
				_, panicked := captureDecimal(node, val)
				if panicked == nil {
					t.Fatalf("%v produced a value, want a width panic", val)
				}
				msg := fmt.Sprint(panicked)
				if !bytes.Contains([]byte(msg), []byte("fixed length")) {
					t.Fatalf("panic = %v, want the fixed-length width check", panicked)
				}
			}
		})
	}

	// -128.5 rounds away from zero to -129, which does not fit in one byte.
	_, panicked := captureDecimal(Decimal(0, 38, FixedLenByteArrayType(1)), float64(-128.5))
	if panicked == nil {
		t.Fatal("expected -128.5 rounded to -129 to exceed a 1-byte decimal")
	}
}

func TestFixedLenDecimalFloatBytes(t *testing.T) {
	values := []float64{
		0, 1, -1, 0.4, -0.4, 0.5, -0.5, 1.5, -1.5, 2.5, -2.5,
		1.25, -1.25, 1.125, -1.125,
		127, -127, 128, -128, 255, -255, 256, -256,
		32767, -32768, 1e6, -1e6,
		float64(math.MinInt64), float64(math.MaxInt64),
		0x1p63, -0x1p63, 0x1p64, -0x1p64, 0x1p127, -0x1p127,
	}
	for range 2 { // second pass reuses scratch buffers
		for _, scale := range []int{0, 2} {
			for _, width := range []int{1, 4, 8, 16} {
				node := Decimal(scale, 38, FixedLenByteArrayType(width))
				for _, v := range values {
					f := new(big.Float).SetFloat64(v)
					want, ok := referenceFixedDecimal(f, scale, 38, width)
					for _, input := range []any{v, float32(v), f} {
						got, panicked := captureDecimal(node, input)
						// float32 cannot represent every float64 in the table.
						if _, is32 := input.(float32); is32 && float64(float32(v)) != v {
							continue
						}
						if !ok {
							if panicked == nil {
								t.Fatalf("scale=%d width=%d %v (%T) = %x, want panic", scale, width, v, input, got)
							}
							continue
						}
						if panicked != nil {
							t.Fatalf("scale=%d width=%d %v (%T) panicked: %v", scale, width, v, input, panicked)
						}
						if !bytes.Equal(got, want) {
							t.Fatalf("scale=%d width=%d %v (%T) = %x, want %x", scale, width, v, input, got, want)
						}
					}
				}
			}
		}
	}
}

// legacyBigIntToByteArray is the pre-fix encoder. Variable-length output has
// to stay byte-identical, including the extra sign byte on -2^(8*k-1).
func legacyBigIntToByteArray(i *big.Int) []byte {
	if i.Sign() >= 0 {
		b := i.Bytes()
		if len(b) > 0 && b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	abs := new(big.Int).Abs(i)
	b := abs.Bytes()
	if len(b) == 0 || b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	for j := range b {
		b[j] = ^b[j]
	}
	carry := byte(1)
	for j := len(b) - 1; j >= 0 && carry > 0; j-- {
		sum := b[j] + carry
		b[j] = sum
		if sum != 0 {
			carry = 0
		}
	}
	return b
}

func TestBigFloatDecimalByteArrayUnchanged(t *testing.T) {
	node := Decimal(0, 40, ByteArrayType)
	values := []*big.Int{
		big.NewInt(0), big.NewInt(1), big.NewInt(-1),
		big.NewInt(127), big.NewInt(128), big.NewInt(-127), big.NewInt(-128),
		big.NewInt(-129), big.NewInt(255), big.NewInt(256), big.NewInt(-256),
		big.NewInt(-32768), big.NewInt(-32769),
	}
	min := new(big.Int).Lsh(big.NewInt(1), 63)
	values = append(values, new(big.Int).Neg(min), new(big.Int).Sub(min, big.NewInt(1)))
	for _, n := range values {
		f := new(big.Float).SetPrec(256).SetInt(n)
		got, panicked := captureDecimal(node, f)
		if panicked != nil {
			t.Fatalf("%v panicked: %v", n, panicked)
		}
		want := legacyBigIntToByteArray(n)
		if !bytes.Equal(got, want) {
			t.Fatalf("%v = %x, want %x", n, got, want)
		}
	}
}

func TestFixedLenDecimalNonFiniteRoundTrip(t *testing.T) {
	schema := NewSchema("row", Group{
		"amount": Decimal(0, 38, FixedLenByteArrayType(16)),
	})
	type raw struct {
		Amount [16]byte `parquet:"amount,decimal(0:38)"`
	}
	inputs := []any{math.NaN(), math.Inf(1), math.Inf(-1), float64(-128), math.Ldexp(-1, 127)}
	var buf bytes.Buffer
	w := NewGenericWriter[any](&buf, schema)
	rows := make([]any, len(inputs))
	for i, v := range inputs {
		rows[i] = map[string]any{"amount": v}
	}
	if n, err := w.Write(rows); n != len(rows) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if f.NumRows() != int64(len(rows)) {
		t.Fatalf("rows = %d, want %d", f.NumRows(), len(rows))
	}
	r := NewReader(bytes.NewReader(buf.Bytes()))
	defer r.Close()
	for i, v := range inputs {
		var got raw
		if err := r.Read(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		direct, panicked := captureDecimal(Decimal(0, 38, FixedLenByteArrayType(16)), v)
		if panicked != nil {
			t.Fatalf("row %d capture: %v", i, panicked)
		}
		if !bytes.Equal(got.Amount[:], direct) {
			t.Fatalf("row %d file = %x, buffer = %x", i, got.Amount[:], direct)
		}
	}
}

func TestFixedLenDecimalNonFiniteOptional(t *testing.T) {
	node := Decimal(0, 38, FixedLenByteArrayType(16))
	schema := NewSchema("row", Group{"amount": Optional(node)})
	type raw struct {
		Amount *[16]byte `parquet:"amount,decimal(0:38)"`
	}
	inputs := []any{math.Inf(1), nil, float64(math.MinInt64)}
	var buf bytes.Buffer
	w := NewGenericWriter[any](&buf, schema)
	rows := make([]any, len(inputs))
	for i, v := range inputs {
		rows[i] = map[string]any{"amount": v}
	}
	if n, err := w.Write(rows); n != len(rows) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := NewReader(bytes.NewReader(buf.Bytes()))
	defer r.Close()
	for i, v := range inputs {
		var got raw
		if err := r.Read(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if v == nil {
			if got.Amount != nil {
				t.Fatalf("row %d = %x, want null", i, *got.Amount)
			}
			continue
		}
		want, panicked := captureDecimal(node, v)
		if panicked != nil {
			t.Fatalf("row %d: %v", i, panicked)
		}
		if got.Amount == nil || !bytes.Equal(got.Amount[:], want) {
			t.Fatalf("row %d = %v, want %x", i, got.Amount, want)
		}
	}
}

func referenceFixedDecimal(f *big.Float, scale, precision, width int) ([]byte, bool) {
	minPrec := uint(precision)*4 + 64
	prec := max(f.Prec(), minPrec)
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaleFactor := new(big.Float).SetPrec(prec).SetInt(pow)
	scaled := new(big.Float).SetPrec(prec).Mul(f, scaleFactor)
	half := new(big.Float).SetPrec(prec).SetFloat64(0.5)
	if scaled.Sign() >= 0 {
		scaled.Add(scaled, half)
	} else {
		scaled.Sub(scaled, half)
	}
	unscaled, _ := scaled.Int(nil)
	if unscaled == nil {
		return nil, false
	}
	return encodeFixedBigInt(unscaled, width)
}

func encodeFixedBigInt(v *big.Int, width int) ([]byte, bool) {
	lim := new(big.Int).Lsh(big.NewInt(1), uint(8*width-1))
	maxV := new(big.Int).Sub(lim, big.NewInt(1))
	minV := new(big.Int).Neg(lim)
	if v.Cmp(minV) < 0 || v.Cmp(maxV) > 0 {
		return nil, false
	}
	buf := make([]byte, width)
	enc := new(big.Int).Set(v)
	if enc.Sign() < 0 {
		mod := new(big.Int).Lsh(big.NewInt(1), uint(8*width))
		enc.Add(enc, mod)
	}
	enc.FillBytes(buf)
	return buf, true
}
