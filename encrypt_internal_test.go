package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// TestValidateRowGroupOrdinalsAllZeroBackfilled covers the "writer omitted the
// optional Ordinal field" case: every value is zero, so the validator should
// back-fill sequential ordinals and not return an error.
func TestValidateRowGroupOrdinalsAllZeroBackfilled(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 0}, {Ordinal: 0}, {Ordinal: 0}}
	if err := validateRowGroupOrdinals(rgs); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	for i, rg := range rgs {
		if int(rg.Ordinal) != i {
			t.Errorf("rgs[%d].Ordinal = %d, want %d", i, rg.Ordinal, i)
		}
	}
}

// TestValidateRowGroupOrdinalsSequentialAccepted covers explicit, sequential,
// in-order ordinals: the validator must accept them unchanged.
func TestValidateRowGroupOrdinalsSequentialAccepted(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 0}, {Ordinal: 1}, {Ordinal: 2}}
	if err := validateRowGroupOrdinals(rgs); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	for i, rg := range rgs {
		if int(rg.Ordinal) != i {
			t.Errorf("rgs[%d].Ordinal = %d (mutated), want %d", i, rg.Ordinal, i)
		}
	}
}

// TestValidateRowGroupOrdinalsMismatchRejected verifies that files with
// non-sequential row group ordinals (e.g. [0,2]) are rejected.  Downstream
// code — page-index lookup and AAD construction for column encryption —
// assumes rg.Ordinal == slice index, so accepting [0,2] would silently
// produce wrong AADs or out-of-range page-index lookups.  Regression for
// codex finding 5.
func TestValidateRowGroupOrdinalsMismatchRejected(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 0}, {Ordinal: 2}}
	err := validateRowGroupOrdinals(rgs)
	if err == nil {
		t.Fatal("expected validateRowGroupOrdinals([0,2]) to fail, got nil")
	}
	if !strings.Contains(err.Error(), "does not match its position") {
		t.Errorf("expected 'does not match its position' in error, got %q", err.Error())
	}
}

// TestValidateRowGroupOrdinalsOutOfOrderRejected covers a different mismatch:
// every ordinal is unique and within range, but the order does not match the
// slice — also incompatible with the rg.Ordinal == slice-index assumption.
func TestValidateRowGroupOrdinalsOutOfOrderRejected(t *testing.T) {
	rgs := []format.RowGroup{{Ordinal: 1}, {Ordinal: 0}}
	err := validateRowGroupOrdinals(rgs)
	if err == nil {
		t.Fatal("expected validateRowGroupOrdinals([1,0]) to fail, got nil")
	}
}

type footerMetadataKeys struct {
	schemeKeys
	metadata []byte
}

func (k footerMetadataKeys) FooterKey(metadata []byte) ([]byte, error) {
	if !bytes.Equal(metadata, k.metadata) {
		return nil, fmt.Errorf("footer key metadata = %q, want %q", metadata, k.metadata)
	}
	return k.footer, nil
}

func TestEncryptionFooterKeyMetadata(t *testing.T) {
	key := bytes.Repeat([]byte{0x41}, 16)
	metadata := []byte("footer-key-id")
	rows := []schemeRow{{Name: "alpha", Value: 1}, {Name: "beta", Value: 2}}
	for _, encryptedFooter := range []bool{true, false} {
		t.Run(fmt.Sprintf("encryptedFooter=%t", encryptedFooter), func(t *testing.T) {
			cfg := &EncryptionConfig{FooterKey: key, EncryptedFooter: encryptedFooter}
			var buf bytes.Buffer
			w := NewGenericWriter[schemeRow](&buf, WithEncryption(cfg), MaxRowsPerRowGroup(1))
			if _, err := w.Write(rows); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			data := withFooterKeyMetadata(t, buf.Bytes(), key, metadata)
			keys := footerMetadataKeys{schemeKeys: schemeKeys{footer: key}, metadata: metadata}
			for _, skipPageIndex := range []bool{false, true} {
				t.Run(fmt.Sprintf("skipPageIndex=%t", skipPageIndex), func(t *testing.T) {
					f, err := OpenFile(bytes.NewReader(data), int64(len(data)), WithDecryption(keys), SkipPageIndex(skipPageIndex))
					if err != nil {
						t.Fatalf("open file: %v", err)
					}
					ci, oi, err := f.ReadPageIndex()
					if err != nil || len(ci) != len(rows)*2 || len(oi) != len(ci) {
						t.Fatalf("read page index: column=%d offset=%d err=%v", len(ci), len(oi), err)
					}
					r := NewGenericReader[schemeRow](f)
					defer r.Close()
					got := make([]schemeRow, len(rows))
					n, err := r.Read(got)
					if (err != nil && err != io.EOF) || !slices.Equal(got[:n], rows) {
						t.Fatalf("read rows: got %v, err=%v, want %v", got[:n], err, rows)
					}
				})
			}
		})
	}
}

// The writer does not expose key metadata, so replace just the footer and
// re-sign it in plaintext-footer mode. Column modules and their offsets stay intact.
func withFooterKeyMetadata(t *testing.T, data, key, metadata []byte) []byte {
	t.Helper()
	footerStart := len(data) - 8 - int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	footer := data[footerStart : len(data)-8]
	protocol := new(thrift.CompactProtocol)
	reader := protocol.NewReaderFromBytes(footer)
	var updated []byte
	var err error
	if string(data[len(data)-4:]) == "PARE" {
		var crypto format.FileCryptoMetaData
		if err := thrift.NewDecoder(reader).Decode(&crypto); err != nil {
			t.Fatal(err)
		}
		crypto.KeyMetadata = metadata
		updated, err = thrift.Marshal(protocol, &crypto)
		updated = append(updated, footer[reader.BytesRead():]...)
	} else {
		var meta format.FileMetaData
		if err := thrift.NewDecoder(reader).Decode(&meta); err != nil {
			t.Fatal(err)
		}
		meta.FooterSigningKeyMetadata = metadata
		updated, err = thrift.Marshal(protocol, &meta)
		if err != nil {
			t.Fatal(err)
		}
		unique, prefix, _, _ := encryptionAADParams(meta.EncryptionAlgorithm)
		var sig []byte
		sig, err = signFooter(key, makeAAD(prefix, unique, footerModule), updated)
		updated = append(updated, sig...)
	}
	if err != nil {
		t.Fatal(err)
	}
	out := append(bytes.Clone(data[:footerStart]), updated...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(updated)))
	return append(out, data[len(data)-4:]...)
}
