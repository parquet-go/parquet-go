package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

// idKeyRetriever resolves column keys from the key metadata stored in the file
// ("kc1", "kc2", ...). partial reports ErrKeyNotFound for anything else so
// plaintext columns can still be read.
type idKeyRetriever struct {
	footer  []byte
	byID    map[string][]byte
	partial bool
}

func (k *idKeyRetriever) FooterKey([]byte) ([]byte, error) { return k.footer, nil }

func (k *idKeyRetriever) ColumnKey(path []string, keyMetadata []byte) ([]byte, error) {
	if key, ok := k.byID[string(keyMetadata)]; ok {
		return key, nil
	}
	if k.partial {
		return nil, fmt.Errorf("no key for %s: %w", strings.Join(path, "."), parquet.ErrKeyNotFound)
	}
	return k.footer, nil
}

func arrowAES128Keys() *idKeyRetriever {
	return &idKeyRetriever{
		footer: []byte("0123456789012345"),
		byID: map[string][]byte{
			"kc1": []byte("1234567890123450"),
			"kc2": []byte("1234567890123451"),
		},
	}
}

func aes256Keys() *idKeyRetriever {
	return &idKeyRetriever{
		footer: []byte("01234567890123456789012345678901"),
		byID: map[string][]byte{
			"kc1": []byte("12345678901234567890123456789012"),
			"kc2": []byte("12345678901234567890123456789013"),
			"kc3": []byte("12345678901234567890123456789014"),
			"kc4": []byte("12345678901234567890123456789015"),
			"kc5": []byte("12345678901234567890123456789016"),
			"kc6": []byte("12345678901234567890123456789017"),
			"kc7": []byte("12345678901234567890123456789018"),
			"kc8": []byte("12345678901234567890123456789019"),
		},
	}
}

func openEncrypted(t *testing.T, path string, keys parquet.KeyRetriever) *parquet.File {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
	if err != nil {
		t.Fatalf("open %s: %v", filepath.Base(path), err)
	}
	return f
}

func readColumn(t *testing.T, f *parquet.File, col int) []parquet.Value {
	t.Helper()
	pages := f.RowGroups()[0].ColumnChunks()[col].Pages()
	defer pages.Close()
	var out []parquet.Value
	buf := make([]parquet.Value, 64)
	for {
		page, err := pages.ReadPage()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("column %d: %v", col, err)
		}
		values := page.Values()
		for {
			n, err := values.ReadValues(buf)
			for _, v := range buf[:n] {
				out = append(out, cloneValue(v))
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("column %d values: %v", col, err)
			}
			if n == 0 {
				break
			}
		}
	}
}

func cloneValue(v parquet.Value) parquet.Value {
	if v.Kind() != parquet.ByteArray && v.Kind() != parquet.FixedLenByteArray {
		return v
	}
	b := slices.Clone(v.ByteArray())
	return parquet.ValueOf(b).Level(v.RepetitionLevel(), v.DefinitionLevel(), v.Column())
}

func TestEncryptionInteropArrowGCM(t *testing.T) {
	files := []string{
		"testdata/encryption/interop/uniform_encryption.parquet.encrypted",
		"testdata/encryption/interop/encrypt_columns_and_footer.parquet.encrypted",
		"testdata/encryption/interop/encrypt_columns_and_footer_aad.parquet.encrypted",
		"testdata/encryption/interop/encrypt_columns_plaintext_footer.parquet.encrypted",
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f := openEncrypted(t, path, arrowAES128Keys())
			if f.NumRows() != 50 {
				t.Fatalf("rows = %d, want 50", f.NumRows())
			}
			assertArrowColumns(t, f)
			assertPageIndexPresent(t, f)
		})
	}
}

func TestEncryptionInteropAES256GCM(t *testing.T) {
	files := []string{
		"testdata/encryption/aes256/uniform_encryption.parquet.encrypted",
		"testdata/encryption/aes256/encrypt_columns_and_footer.parquet.encrypted",
		"testdata/encryption/aes256/encrypt_columns_plaintext_footer.parquet.encrypted",
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f := openEncrypted(t, path, aes256Keys())
			if f.NumRows() != 50 {
				t.Fatalf("rows = %d, want 50", f.NumRows())
			}
			assertArrowColumns(t, f)
			assertPageIndexPresent(t, f)
		})
	}
}

func TestEncryptionInteropBloomFilter(t *testing.T) {
	f := openEncrypted(t, "testdata/encryption/interop/encrypt_columns_and_footer_bloom_filter.parquet.encrypted", arrowAES128Keys())
	if f.NumRows() != 2000 {
		t.Fatalf("rows = %d, want 2000", f.NumRows())
	}
	cols := f.RowGroups()[0].ColumnChunks()
	if len(cols) != 4 {
		t.Fatalf("columns = %d, want 4", len(cols))
	}
	for i, name := range []string{"double_field", "float_field", "int32_field", "name"} {
		path := f.Metadata().RowGroups[0].Columns[i].MetaData.PathInSchema
		if len(path) == 0 || path[len(path)-1] != name {
			t.Fatalf("column %d path = %v, want %s", i, path, name)
		}
		vals := readColumn(t, f, i)
		assertBloomColumn(t, name, vals)
		if name == "double_field" || name == "float_field" {
			bf := cols[i].BloomFilter()
			if bf == nil {
				t.Fatalf("column %s: missing bloom filter", name)
			}
			if err := checkBloom(t, bf, vals[0], true); err != nil {
				t.Fatalf("bloom %s: %v", name, err)
			}
		}
	}
	assertPageIndexPresent(t, f)
}

func TestEncryptionInteropPlaintextColumnsWithoutColumnKeys(t *testing.T) {
	keys := &idKeyRetriever{
		footer:  []byte("0123456789012345"),
		partial: true,
	}
	f := openEncrypted(t, "testdata/encryption/interop/encrypt_columns_and_footer.parquet.encrypted", keys)
	bools := readColumn(t, f, 0)
	if len(bools) != 50 || !bools[0].Boolean() || bools[1].Boolean() {
		t.Fatalf("plaintext boolean column = %d values, first two %v %v", len(bools), bools[0].Boolean(), bools[1].Boolean())
	}
	ints := readColumn(t, f, 1)
	if len(ints) != 50 || ints[0].Int32() != 0 || ints[49].Int32() != 49 {
		t.Fatalf("plaintext int32 column mismatch: n=%d first=%d last=%d", len(ints), ints[0].Int32(), ints[len(ints)-1].Int32())
	}
	_, err := f.RowGroups()[0].ColumnChunks()[4].Pages().ReadPage()
	if !errors.Is(err, parquet.ErrKeyNotFound) {
		t.Fatalf("encrypted column without a key: got %v, want ErrKeyNotFound", err)
	}
}

func TestEncryptionInteropUnsupportedFiles(t *testing.T) {
	t.Run("ctr", func(t *testing.T) {
		f := openEncrypted(t, "testdata/encryption/interop/encrypt_columns_and_footer_ctr.parquet.encrypted", arrowAES128Keys())
		bools := readColumn(t, f, 0)
		if len(bools) != 50 {
			t.Fatalf("plaintext boolean values = %d", len(bools))
		}
		_, err := f.RowGroups()[0].ColumnChunks()[4].Pages().ReadPage()
		if err == nil || !strings.Contains(err.Error(), "AES_GCM_CTR_V1") {
			t.Fatalf("ctr page error = %v", err)
		}
	})
	t.Run("disable-aad-storage", func(t *testing.T) {
		data, err := os.ReadFile("testdata/encryption/interop/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted")
		if err != nil {
			t.Fatal(err)
		}
		_, err = parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(arrowAES128Keys()))
		if err == nil || !strings.Contains(err.Error(), "AAD prefix") {
			t.Fatalf("error = %v", err)
		}
	})
}

func assertArrowColumns(t *testing.T, f *parquet.File) {
	t.Helper()
	const rows = 50
	bools := readColumn(t, f, 0)
	int32s := readColumn(t, f, 1)
	int64s := readColumn(t, f, 2)
	int96s := readColumn(t, f, 3)
	floats := readColumn(t, f, 4)
	doubles := readColumn(t, f, 5)
	bas := readColumn(t, f, 6)
	flbas := readColumn(t, f, 7)

	if len(bools) != rows || len(int32s) != rows || len(int96s) != rows || len(floats) != rows || len(doubles) != rows || len(bas) != rows || len(flbas) != rows {
		t.Fatalf("value counts bool=%d i32=%d i96=%d f32=%d f64=%d ba=%d flba=%d",
			len(bools), len(int32s), len(int96s), len(floats), len(doubles), len(bas), len(flbas))
	}
	if len(int64s) != rows*2 {
		t.Fatalf("int64 values = %d, want %d", len(int64s), rows*2)
	}
	for i := range rows {
		if bools[i].Boolean() != (i%2 == 0) {
			t.Fatalf("bool[%d] = %v", i, bools[i].Boolean())
		}
		if int32s[i].Int32() != int32(i) {
			t.Fatalf("int32[%d] = %d", i, int32s[i].Int32())
		}
		want96 := deprecated.Int96{uint32(i), uint32(i + 1), uint32(i + 2)}
		if int96s[i].Int96() != want96 {
			t.Fatalf("int96[%d] = %v, want %v", i, int96s[i].Int96(), want96)
		}
		if floats[i].Float() != float32(i)*float32(1.1) {
			t.Fatalf("float[%d] = %v", i, floats[i].Float())
		}
		if doubles[i].Double() != float64(i)*1.1111111 {
			t.Fatalf("double[%d] = %v", i, doubles[i].Double())
		}
		if i%2 == 0 {
			want := arrowByteArray(i)
			if bas[i].IsNull() || !bytes.Equal(bas[i].ByteArray(), want) {
				t.Fatalf("ba[%d] = %q, want %q", i, bas[i].ByteArray(), want)
			}
		} else if !bas[i].IsNull() {
			t.Fatalf("ba[%d] = %q, want null", i, bas[i].ByteArray())
		}
		flba := flbas[i].ByteArray()
		if len(flba) != 10 {
			t.Fatalf("flba[%d] len = %d", i, len(flba))
		}
		for _, b := range flba {
			if b != byte(i) {
				t.Fatalf("flba[%d] = %v", i, flba)
			}
		}
	}
	for i := range rows * 2 {
		want := int64(i) * 1000 * 1000 * 1000 * 1000
		if int64s[i].Int64() != want {
			t.Fatalf("int64[%d] = %d, want %d", i, int64s[i].Int64(), want)
		}
		wantRep := 1
		if i%2 == 0 {
			wantRep = 0
		}
		if int64s[i].RepetitionLevel() != wantRep {
			t.Fatalf("int64[%d] repetition = %d, want %d", i, int64s[i].RepetitionLevel(), wantRep)
		}
	}
}

type legacyEncRow struct {
	Name  string `parquet:"name,dict"`
	Value int64  `parquet:"value"`
	Note  string `parquet:"note"`
}

func TestEncryptionLegacyFilesRemainReadable(t *testing.T) {
	footerKey := bytes.Repeat([]byte{0x11}, 16)
	nameKey := bytes.Repeat([]byte{0x22}, 16)
	keys := &staticKeyRetriever{
		footerKey: footerKey,
		columnKeys: map[string][]byte{
			"name": nameKey,
		},
	}
	notes := []string{"alpha", "beta", "gamma"}
	want := make([]legacyEncRow, 24)
	for i := range want {
		want[i] = legacyEncRow{Name: notes[i%3], Value: int64(i)*10 + 7, Note: notes[(i+1)%3]}
	}
	for _, path := range []string{
		"testdata/encryption/legacy-encrypted-footer.parquet",
		"testdata/encryption/legacy-plaintext-footer.parquet",
	} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.WithDecryption(keys))
			if err != nil {
				t.Fatal(err)
			}
			r := parquet.NewGenericReader[legacyEncRow](f)
			got := make([]legacyEncRow, 0, len(want))
			buf := make([]legacyEncRow, 8)
			for {
				n, err := r.Read(buf)
				got = append(got, buf[:n]...)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			r.Close()
			assertRowsEqual(t, want, got)

			cols := f.RowGroups()[0].ColumnChunks()
			if f.Metadata().RowGroups[0].Columns[0].MetaData.DictionaryPageOffset == 0 {
				t.Fatal("name column has no dictionary page")
			}
			bf := cols[0].BloomFilter()
			if bf == nil {
				t.Fatal("missing bloom filter on name")
			}
			if err := checkBloom(t, bf, parquet.ValueOf("alpha"), true); err != nil {
				t.Fatal(err)
			}
			if err := checkBloom(t, bf, parquet.ValueOf("nope"), false); err != nil {
				t.Fatal(err)
			}
			for _, cc := range cols {
				oi, err := cc.OffsetIndex()
				if err != nil || oi == nil || oi.NumPages() == 0 {
					t.Fatalf("offset index col %d: %v", cc.Column(), err)
				}
				ci, err := cc.ColumnIndex()
				if err != nil || ci == nil {
					t.Fatalf("column index col %d: %v", cc.Column(), err)
				}
			}
		})
	}
}

// checkBloom reports whether v is in bf. Filters in these fixtures were
// written on little-endian hosts. Block words are overlaid as host uint32s,
// so a probe on a big-endian machine does not match; decryption is still
// checked by requiring a non-nil filter.
func hostLittleEndian() bool {
	var n uint16 = 1
	return (*[2]byte)(unsafe.Pointer(&n))[0] == 1
}

func checkBloom(t *testing.T, bf parquet.BloomFilter, v parquet.Value, want bool) error {
	t.Helper()
	if bf == nil {
		return errors.New("missing bloom filter")
	}
	if !hostLittleEndian() {
		return nil
	}
	ok, err := bf.Check(v)
	if err != nil {
		return err
	}
	if ok != want {
		return fmt.Errorf("bloom check(%v) = %v, want %v", v, ok, want)
	}
	return nil
}

func assertBloomColumn(t *testing.T, name string, vals []parquet.Value) {
	t.Helper()
	if len(vals) != 2000 {
		t.Fatalf("%s values = %d, want 2000", name, len(vals))
	}
	for i, v := range vals {
		switch name {
		case "double_field":
			if v.Double() != float64(i)+0.5 {
				t.Fatalf("double[%d] = %v", i, v.Double())
			}
		case "float_field":
			if v.Float() != float32(i)+0.25 {
				t.Fatalf("float[%d] = %v", i, v.Float())
			}
		case "int32_field":
			if v.IsNull() || v.Int32() != int32(i) {
				t.Fatalf("int32[%d] = %v null=%v", i, v.Int32(), v.IsNull())
			}
		case "name":
			want := fmt.Sprintf("name_%d", i)
			if string(v.ByteArray()) != want {
				t.Fatalf("name[%d] = %q, want %q", i, v.ByteArray(), want)
			}
		}
	}
}

func arrowByteArray(i int) []byte {
	b := []byte("parquet000")
	b[7] = byte('0' + i/100)
	b[8] = byte('0' + (i/10)%10)
	b[9] = byte('0' + i%10)
	return b
}

func assertPageIndexPresent(t *testing.T, f *parquet.File) {
	t.Helper()
	for _, cc := range f.RowGroups()[0].ColumnChunks() {
		name := strings.Join(f.Metadata().RowGroups[0].Columns[cc.Column()].MetaData.PathInSchema, ".")
		oi, err := cc.OffsetIndex()
		if err != nil {
			t.Fatalf("offset index %s: %v", name, err)
		}
		if oi == nil || oi.NumPages() == 0 {
			t.Fatalf("offset index %s missing", name)
		}
		ci, err := cc.ColumnIndex()
		meta := f.Metadata().RowGroups[0].Columns[cc.Column()]
		if meta.ColumnIndexOffset == 0 {
			if err != nil && !errors.Is(err, parquet.ErrMissingColumnIndex) {
				t.Fatalf("column index %s: %v", name, err)
			}
			continue
		}
		if err != nil || ci == nil {
			t.Fatalf("column index %s: %v", name, err)
		}
	}
}
