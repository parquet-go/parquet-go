package parquet

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
)

// Module type constants per the Apache Parquet encryption spec
// (Encryption.md section 4.4.2).
const (
	footerModule          byte = 0
	columnMetaDataModule  byte = 1
	dataPageBodyModule    byte = 2
	dictPageBodyModule    byte = 3
	dataPageHeaderModule  byte = 4
	dictPageHeaderModule  byte = 5
	columnIndexModule     byte = 6
	offsetIndexModule     byte = 7
	bloomFilterHdrModule  byte = 8
	bloomFilterBitsModule byte = 9
)

// Module type ids written by parquet-go v0.30.1 through v0.32.1. Dictionary
// page headers already used the spec id, but the writer appended a page
// ordinal of 0. The spec includes a page ordinal only for data pages and
// data page headers.
const (
	legacyDataPageHeaderModule  byte = 3
	legacyDictPageBodyModule    byte = 4
	legacyBloomFilterHdrModule  byte = 6
	legacyBloomFilterBitsModule byte = 7
	legacyColumnIndexModule     byte = 8
	legacyOffsetIndexModule     byte = 9
)

// errGCMAuth marks an AES-GCM tag mismatch. A retry with the legacy AAD is
// safe only for this error: GCM still authenticated the ciphertext, so the
// plaintext is the one the writer produced.
var errGCMAuth = errors.New("parquet decryption: AES-GCM authentication failed")

func errSupplyAADPrefix() error {
	return errors.New("parquet decryption: file requires an AAD prefix that is not stored in the file")
}

func errCTRPage() error {
	return errors.New("parquet decryption: AES_GCM_CTR_V1 page encryption is not supported")
}

const (
	encNonceSize = 12
	encTagSize   = 16
	// encOverhead is the total per-module overhead: 4-byte length + nonce + GCM tag.
	encOverhead = 4 + encNonceSize + encTagSize
)

// EncryptionAlgorithmType selects the Parquet encryption algorithm.
type EncryptionAlgorithmType int

const (
	// AES_GCM_V1 is the default algorithm (AES-GCM authenticated encryption).
	AES_GCM_V1 EncryptionAlgorithmType = iota
	// AES_GCM_CTR_V1 is the CTR variant (deprecated, included for spec completeness).
	AES_GCM_CTR_V1
)

// EncryptionConfig holds the write-side encryption configuration.
type EncryptionConfig struct {
	// FooterKey is the AES key used to encrypt the footer and any columns
	// that do not have a per-column key. Must be 16, 24, or 32 bytes.
	FooterKey []byte

	// ColumnKeys maps a dot-joined column path (e.g. "a.b.c") to its AES key.
	// Columns not listed here use FooterKey.
	ColumnKeys map[string][]byte

	// Algorithm selects AES_GCM_V1 (default) or AES_GCM_CTR_V1.
	Algorithm EncryptionAlgorithmType

	// EncryptedFooter controls the file layout:
	//   true  → encrypted footer, file uses "PARE" magic.
	//   false → plaintext footer with GCM signature appended, file uses "PAR1" magic.
	EncryptedFooter bool

	// AadPrefix is an optional byte prefix that is prepended to every AAD.
	AadPrefix []byte

	// FileIdentifier is an 8-byte per-file unique value embedded in every AAD.
	// If nil, 8 random bytes are generated when the writer is created.
	FileIdentifier []byte
}

// ErrKeyNotFound is the sentinel error that a KeyRetriever should return (or
// wrap with %w) when the caller intentionally does not have access to a
// particular column key.  OpenFile treats this as a non-fatal signal and
// leaves that column inaccessible rather than aborting the open.  Any other
// error from ColumnKey is treated as a hard failure and propagated to the
// caller.
var ErrKeyNotFound = errors.New("parquet: encryption key not found")

// KeyRetriever resolves AES keys from the metadata bytes stored in the file.
// Implement this interface to supply keys when opening an encrypted parquet file.
type KeyRetriever interface {
	// FooterKey returns the AES key for the file footer.
	// keyMetadata is the optional bytes stored in FileCryptoMetaData.KeyMetadata
	// (may be nil).
	FooterKey(keyMetadata []byte) ([]byte, error)

	// ColumnKey returns the AES key for an encrypted column.
	// path is the column's path in the schema; keyMetadata is the optional bytes
	// stored in EncryptionWithColumnKey.KeyMetadata (may be nil).
	//
	// To signal that a particular column's key is intentionally unavailable
	// (e.g. the caller only holds keys for a subset of columns), return an
	// error that wraps ErrKeyNotFound:
	//
	//	return nil, fmt.Errorf("no key for %v: %w", path, parquet.ErrKeyNotFound)
	//
	// OpenFile treats ErrKeyNotFound as non-fatal and leaves that column
	// inaccessible; any other non-nil error is propagated as a hard failure.
	ColumnKey(path []string, keyMetadata []byte) ([]byte, error)
}

// DecryptionConfig holds the read-side decryption configuration.
type DecryptionConfig struct {
	Keys KeyRetriever
}

// WithDecryption returns a FileOption that configures decryption with the given KeyRetriever.
func WithDecryption(keys KeyRetriever) FileOption {
	return &fileDecryptionOption{cfg: &DecryptionConfig{Keys: keys}}
}

type fileDecryptionOption struct{ cfg *DecryptionConfig }

func (o *fileDecryptionOption) ConfigureFile(c *FileConfig) { c.Decryption = o.cfg }

// WithEncryption returns a WriterOption that configures encryption.
func WithEncryption(cfg *EncryptionConfig) WriterOption {
	return &writerEncryptionOption{cfg: cfg}
}

type writerEncryptionOption struct{ cfg *EncryptionConfig }

func (o *writerEncryptionOption) ConfigureWriter(c *WriterConfig) { c.Encryption = o.cfg }

// -----------------------------------------------------------------------
// Internal state
// -----------------------------------------------------------------------

// fileEncryptionState is the per-file encryption context used during writing.
type fileEncryptionState struct {
	cfg        *EncryptionConfig
	fileUnique []byte // 8-byte random per-file identifier stored in AesGcmV1.AadFileUnique
}

func newFileEncryptionState(cfg *EncryptionConfig) (*fileEncryptionState, error) {
	if cfg.Algorithm == AES_GCM_CTR_V1 {
		return nil, fmt.Errorf("parquet encryption: AES_GCM_CTR_V1 is not yet implemented; use AES_GCM_V1")
	}
	s := &fileEncryptionState{cfg: cfg}
	if len(cfg.FileIdentifier) > 0 {
		s.fileUnique = cfg.FileIdentifier
	} else {
		s.fileUnique = make([]byte, 8)
		if _, err := io.ReadFull(rand.Reader, s.fileUnique); err != nil {
			return nil, fmt.Errorf("parquet encryption: generating file identifier: %w", err)
		}
	}
	return s, nil
}

// columnKeyFor returns the AES key for the given dot-joined column path,
// falling back to FooterKey when no per-column key is configured.
func (s *fileEncryptionState) columnKeyFor(path string) []byte {
	if s.cfg.ColumnKeys != nil {
		if key, ok := s.cfg.ColumnKeys[path]; ok {
			return key
		}
	}
	return s.cfg.FooterKey
}

// -----------------------------------------------------------------------
// Core cryptographic primitives
// -----------------------------------------------------------------------

// encryptModule encrypts plaintext as a Parquet module envelope.
//
// Wire format: 4-byte LE length | 12-byte nonce | AES-GCM(plaintext) | 16-byte tag
// The length field equals len(plaintext) + encNonceSize + encTagSize.
func encryptModule(key, aad, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("parquet encryption: creating AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("parquet encryption: creating GCM: %w", err)
	}

	var nonce [encNonceSize]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, fmt.Errorf("parquet encryption: generating nonce: %w", err)
	}

	// cipherLen = len(plaintext) + gcm.Overhead()  (= tag size = 16)
	cipherLen := len(plaintext) + gcm.Overhead()
	// moduleLen = nonce + ciphertext+tag (= what the 4-byte length field encodes)
	moduleLen := encNonceSize + cipherLen

	out := make([]byte, 4+moduleLen)
	binary.LittleEndian.PutUint32(out[:4], uint32(moduleLen))
	copy(out[4:4+encNonceSize], nonce[:])
	gcm.Seal(out[4+encNonceSize:4+encNonceSize], nonce[:], plaintext, aad)
	return out, nil
}

// decryptModule decrypts a Parquet module envelope.
//
// envelope must start with the 4-byte length field.
// Returns the decrypted plaintext.
func decryptModule(key, aad, envelope []byte) ([]byte, error) {
	if len(envelope) < 4 {
		return nil, fmt.Errorf("parquet decryption: module envelope too short (%d bytes)", len(envelope))
	}
	moduleLen := int(binary.LittleEndian.Uint32(envelope[:4]))
	if len(envelope) < 4+moduleLen {
		return nil, fmt.Errorf("parquet decryption: envelope truncated: need %d bytes, have %d", 4+moduleLen, len(envelope))
	}
	if moduleLen < encNonceSize+encTagSize {
		return nil, fmt.Errorf("parquet decryption: module too short to contain nonce+tag (%d bytes)", moduleLen)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("parquet decryption: creating AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("parquet decryption: creating GCM: %w", err)
	}

	nonce := envelope[4 : 4+encNonceSize]
	ciphertext := envelope[4+encNonceSize : 4+moduleLen]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errGCMAuth, err)
	}
	return plaintext, nil
}

// moduleHasLegacy reports whether module was written with a different AAD by
// parquet-go before the spec fix. Data page bodies, column metadata, and the
// footer used the spec ids all along.
func moduleHasLegacy(module byte) bool {
	switch module {
	case dataPageHeaderModule, dictPageHeaderModule, dictPageBodyModule,
		columnIndexModule, offsetIndexModule,
		bloomFilterHdrModule, bloomFilterBitsModule:
		return true
	default:
		return false
	}
}

// legacyModuleAAD is the AAD parquet-go v0.30.1 through v0.32.1 produced for
// module. ordinals are the spec ordinals (no page ordinal on dictionary pages).
func legacyModuleAAD(prefix, fileUnique []byte, module byte, ordinals []int16) []byte {
	switch module {
	case dataPageHeaderModule:
		return makeAAD(prefix, fileUnique, legacyDataPageHeaderModule, ordinals...)
	case dictPageHeaderModule:
		ords := append(append([]int16{}, ordinals...), 0)
		return makeAAD(prefix, fileUnique, dictPageHeaderModule, ords...)
	case dictPageBodyModule:
		ords := append(append([]int16{}, ordinals...), 0)
		return makeAAD(prefix, fileUnique, legacyDictPageBodyModule, ords...)
	case columnIndexModule:
		return makeAAD(prefix, fileUnique, legacyColumnIndexModule, ordinals...)
	case offsetIndexModule:
		return makeAAD(prefix, fileUnique, legacyOffsetIndexModule, ordinals...)
	case bloomFilterHdrModule:
		return makeAAD(prefix, fileUnique, legacyBloomFilterHdrModule, ordinals...)
	case bloomFilterBitsModule:
		return makeAAD(prefix, fileUnique, legacyBloomFilterBitsModule, ordinals...)
	default:
		return nil
	}
}

// decryptWithLegacy decrypts envelope with the spec AAD. If authentication
// fails, it retries with the AAD written by parquet-go before the spec fix.
// A successful retry stores true in legacy so later modules skip the spec
// attempt. legacy may be nil.
func decryptWithLegacy(key, envelope, prefix, fileUnique []byte, module byte, ordinals []int16, legacy *atomic.Bool) ([]byte, error) {
	if moduleHasLegacy(module) && legacy != nil && legacy.Load() {
		return decryptModule(key, legacyModuleAAD(prefix, fileUnique, module, ordinals), envelope)
	}
	plain, err := decryptModule(key, makeAAD(prefix, fileUnique, module, ordinals...), envelope)
	if err == nil || !moduleHasLegacy(module) || !errors.Is(err, errGCMAuth) {
		return plain, err
	}
	plain, lerr := decryptModule(key, legacyModuleAAD(prefix, fileUnique, module, ordinals), envelope)
	if lerr != nil {
		return nil, err
	}
	if legacy != nil {
		legacy.Store(true)
	}
	return plain, nil
}

// makeAAD constructs the Additional Authenticated Data for a module.
//
// Layout: aadPrefix || fileUnique || moduleType [ || ordinals... (int16 LE each) ]
func makeAAD(aadPrefix, fileUnique []byte, moduleType byte, ordinals ...int16) []byte {
	buf := make([]byte, 0, len(aadPrefix)+len(fileUnique)+1+len(ordinals)*2)
	buf = append(buf, aadPrefix...)
	buf = append(buf, fileUnique...)
	buf = append(buf, moduleType)
	for _, ord := range ordinals {
		buf = append(buf, byte(ord), byte(ord>>8))
	}
	return buf
}

// signFooter produces the 28-byte footer signature (nonce || GCM tag) used in
// plaintext-footer mode. The signature is the nonce and tag from AES-GCM
// encryption of the serialized FileMetaData with the footer AAD. The
// ciphertext is discarded, per Encryption.md section 5.5.
func signFooter(key, aad, footerBytes []byte) ([]byte, error) {
	var nonce [encNonceSize]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, fmt.Errorf("parquet encryption: footer sign: generating nonce: %w", err)
	}
	tag, err := gcmTag(key, nonce[:], footerBytes, aad)
	if err != nil {
		return nil, fmt.Errorf("parquet encryption: footer sign: %w", err)
	}
	sig := make([]byte, encNonceSize+encTagSize)
	copy(sig[:encNonceSize], nonce[:])
	copy(sig[encNonceSize:], tag)
	return sig, nil
}

// verifyFooterSignature checks the 28-byte signature appended to the footer.
// legacy is true when the signature matched the construction written by
// parquet-go before the spec fix (empty plaintext, AAD = footer AAD ||
// serialized footer) rather than the spec.
func verifyFooterSignature(key, aad, footerBytes, sig []byte) (legacy bool, err error) {
	if len(sig) != encNonceSize+encTagSize {
		return false, fmt.Errorf("parquet decryption: footer signature has wrong length %d (want %d)", len(sig), encNonceSize+encTagSize)
	}
	nonce := sig[:encNonceSize]
	tag := sig[encNonceSize:]
	ok, err := footerTagMatches(key, nonce, footerBytes, aad, tag)
	if err != nil {
		return false, fmt.Errorf("parquet decryption: footer verify: %w", err)
	}
	if ok {
		return false, nil
	}
	legacyAAD := make([]byte, 0, len(aad)+len(footerBytes))
	legacyAAD = append(legacyAAD, aad...)
	legacyAAD = append(legacyAAD, footerBytes...)
	ok, err = footerTagMatches(key, nonce, nil, legacyAAD, tag)
	if err != nil {
		return false, fmt.Errorf("parquet decryption: footer verify: %w", err)
	}
	if ok {
		return true, nil
	}
	return false, fmt.Errorf("parquet decryption: footer signature verification failed")
}

// gcmTag returns the AES-GCM tag for plaintext authenticated with aad.
func gcmTag(key, nonce, plaintext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	tag := make([]byte, encTagSize)
	copy(tag, sealed[len(sealed)-encTagSize:])
	return tag, nil
}

func footerTagMatches(key, nonce, plaintext, aad, tag []byte) (bool, error) {
	got, err := gcmTag(key, nonce, plaintext, aad)
	if err != nil {
		return false, err
	}
	return subtle.ConstantTimeCompare(got, tag) == 1, nil
}

// columnPathString returns the dot-joined string for a column path.
func columnPathString(path []string) string {
	return strings.Join(path, ".")
}
