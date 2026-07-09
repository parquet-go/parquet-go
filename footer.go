package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// A Footer is a decoded, validated parquet file footer.
//
// Footer values are immutable after construction and safe for concurrent use:
// a single Footer may back any number of Files opened with the WithFooter
// option, across goroutines, without synchronization. This makes Footer
// suitable for caching: programs opening the same file repeatedly can read
// the footer once with ReadFooter (from the file, or from cached raw footer
// bytes) and amortize the decoding cost across opens.
//
// All footer normalization performed by OpenFile happens at construction
// time: key/value metadata is sorted, row group ordinals are back-filled,
// and encrypted column metadata is decrypted. Files opened with WithFooter
// never write to the footer.
//
// A cached Footer retains the decoded metadata graph, one backing byte
// buffer, and the schema (including a column tree with per-leaf slices
// proportional to the number of row groups). The page index is not part of
// the footer: files opened with WithFooter read it from the file unless
// SkipPageIndex is used.
type Footer struct {
	metadata format.FileMetaData
	data     []byte // backing buffer aliased by metadata strings and byte slices
	size     int64  // size of the file the footer was read from, 0 if unknown

	// The schema is built eagerly by ReadFooter (which reports schema
	// validation errors), and lazily through schemaOnce for
	// footers created internally by OpenFile and exposed via File.Footer.
	schemaOnce sync.Once
	schema     *Schema
	schemaErr  error

	// Decryption state carried over to files opened from this footer;
	// non-nil only when the file has encryption metadata.
	decryption *DecryptionConfig
	fileUnique []byte
	aadPrefix  []byte
}

// ReadFooter reads and decodes the footer of the parquet file of the given
// size in r. Only the footer is read; use OpenFile with the WithFooter
// option to open the file for reading rows.
//
// Options are interpreted the same way as OpenFile: WithDecryption is
// required for files with encrypted or signed footers, SkipMagicBytes skips
// the header magic check, and OptimisticRead and ReadBufferSize control how
// the footer bytes are fetched. Decryption is resolved at construction time:
// the footer (and the column metadata it contains) is decrypted here, and
// files later opened from it inherit this decryption configuration
// regardless of the options passed to OpenFile.
//
// ReadFooter also accepts a bare footer section — the last footerSize+8
// bytes of a file, as cached by programs that store raw footer bytes
// instead of decoded footers:
//
//	footer, err := parquet.ReadFooter(bytes.NewReader(blob), int64(len(blob)))
//
// The input is recognized as a bare footer when it is exactly footerSize+8
// bytes (a complete file is always larger, since at least the 4-byte header
// magic precedes the footer). In that case the header magic check does not
// apply and the footer records no file size (see Size). ReadFooter does not
// retain the input; it makes a private copy of the parts it needs.
func ReadFooter(r io.ReaderAt, size int64, options ...FileOption) (*Footer, error) {
	c, err := NewFileConfig(options...)
	if err != nil {
		return nil, err
	}
	footer, _, err := readFooter(r, size, c, true)
	if err != nil {
		return nil, err
	}
	if err := footer.buildSchema(); err != nil {
		return nil, err
	}
	return footer, nil
}

// Metadata returns the file metadata of the footer.
//
// The returned value is shared by every file opened from this footer, across
// goroutines: it must be treated as strictly read-only. Mutating it corrupts
// all files backed by the footer and is a data race if any of them is in use
// concurrently.
func (f *Footer) Metadata() *format.FileMetaData { return &f.metadata }

// NumRows returns the number of rows in the file.
func (f *Footer) NumRows() int64 { return f.metadata.NumRows }

// Size returns the size of the file the footer was read from, or 0 when the
// size is unknown (footers read from a bare footer section, which carries no
// information about the rest of the file).
func (f *Footer) Size() int64 { return f.size }

// Lookup returns the value associated with the given key in the file
// key/value metadata.
func (f *Footer) Lookup(key string) (value string, ok bool) {
	return lookupKeyValueMetadata(f.metadata.KeyValueMetadata, key)
}

// Schema returns the schema of the file the footer was read from.
//
// The schema is shared by all callers and safe for concurrent use. For
// footers returned by ReadFooter it was constructed and
// validated when the footer was; for footers obtained from File.Footer it
// is constructed lazily on first call.
func (f *Footer) Schema() *Schema {
	f.buildSchema()
	return f.schema
}

// buildSchema constructs the schema from the footer metadata, validating the
// schema tree in the process. The public footer constructors call it eagerly
// so that a Footer obtained from ReadFooter is guaranteed to
// carry a valid schema; footers created internally by OpenFile skip it
// because OpenFile builds (and validates) its own file-bound column tree
// from the same metadata.
func (f *Footer) buildSchema() error {
	f.schemaOnce.Do(func() {
		root, err := openColumns(nil, &f.metadata, nil, nil)
		if err != nil {
			f.schemaErr = fmt.Errorf("opening columns of parquet footer: %w", err)
			return
		}
		f.schema = NewSchema(root.Name(), root)
	})
	return f.schemaErr
}

// readFooter reads the footer section of the parquet file of the given size
// in r and constructs a Footer from it. The returned io.ReaderAt serves
// reads from the prefetched footer region when the OptimisticRead option is
// enabled (and is r itself otherwise); OpenFile uses it to read the page
// index without going back to the underlying reader.
//
// When allowBareFooter is true and the input is exactly footerSize+8 bytes,
// it is treated as a bare footer section rather than a complete file: the
// header magic check is skipped (there is no header) and the returned footer
// records no file size.
func readFooter(r io.ReaderAt, size int64, c *FileConfig, allowBareFooter bool) (*Footer, io.ReaderAt, error) {
	if cast, ok := r.(interface{ SetMagicFooterSection(offset, length int64) }); ok {
		cast.SetMagicFooterSection(size-8, 8)
	}

	reader := r
	optimisticRead := c.OptimisticRead
	optimisticFooterSize := min(int64(c.ReadBufferSize), size)
	if !optimisticRead || optimisticFooterSize < 8 {
		optimisticFooterSize = 8
	}
	optimisticFooterData := make([]byte, optimisticFooterSize)
	if optimisticRead {
		reader = &optimisticFileReaderAt{
			reader: r,
			offset: size - optimisticFooterSize,
			footer: optimisticFooterData,
		}
	}

	if n, err := readAt(r, optimisticFooterData, size-optimisticFooterSize); n != len(optimisticFooterData) {
		return nil, nil, fmt.Errorf("reading magic footer of parquet file: %w (read: %d)", err, n)
	}
	optimisticFooterSize -= 8
	b := optimisticFooterData[optimisticFooterSize:]
	footerMagic := string(b[4:])
	if footerMagic != "PAR1" && footerMagic != "PARE" {
		return nil, nil, fmt.Errorf("invalid magic footer of parquet file: %q", b[4:])
	}

	footerSize := int64(binary.LittleEndian.Uint32(b[:4]))

	// A complete file is always larger than footerSize+8: at minimum the
	// 4-byte header magic precedes the footer section. An input of exactly
	// footerSize+8 bytes is therefore a bare footer section (cached footer
	// bytes), which has no header to check and no known file size.
	bareFooter := allowBareFooter && size == footerSize+8

	if !bareFooter && !c.SkipMagicBytes {
		var headerMagic [4]byte
		if _, err := readAt(r, headerMagic[:], 0); err != nil {
			return nil, nil, fmt.Errorf("reading magic header of parquet file: %w", err)
		}
		switch string(headerMagic[:]) {
		case "PAR1":
			// plain or plaintext-footer-with-signature
		case "PARE":
			// encrypted footer
			if c.Decryption == nil {
				return nil, nil, fmt.Errorf("parquet file has encrypted footer (magic \"PARE\") but no DecryptionConfig was provided")
			}
		default:
			return nil, nil, fmt.Errorf("invalid magic header of parquet file: %q", headerMagic[:])
		}
	}

	footerData := []byte(nil)

	if footerSize <= optimisticFooterSize {
		footerData = optimisticFooterData[optimisticFooterSize-footerSize : optimisticFooterSize]
	} else {
		footerData = make([]byte, footerSize)
		if cast, ok := reader.(interface{ SetFooterSection(offset, length int64) }); ok {
			cast.SetFooterSection(size-(footerSize+8), footerSize)
		}
		if _, err := readAt(reader, footerData, size-(footerSize+8)); err != nil {
			return nil, nil, fmt.Errorf("reading footer of parquet file: %w", err)
		}
	}

	footer, err := newFooter(footerData, footerMagic, c)
	if err != nil {
		return nil, nil, err
	}
	if !bareFooter {
		footer.size = size
	}
	return footer, reader, nil
}

// newFooter decodes and normalizes a footer from the raw footer section
// bytes (without the trailing size and magic). newFooter does not retain
// data.
func newFooter(data []byte, magic string, c *FileConfig) (*Footer, error) {
	f := new(Footer)
	protocol := new(thrift.CompactProtocol)

	if magic == "PARE" {
		// Encrypted footer: data = FileCryptoMetaData (thrift) || encrypted footer module.
		// Decode FileCryptoMetaData using a bytes-backed reader to count bytes consumed.
		if c.Decryption == nil {
			return nil, fmt.Errorf("parquet file has encrypted footer (magic \"PARE\") but no DecryptionConfig was provided")
		}
		pr := protocol.NewReaderFromBytes(data)
		var cryptoMeta format.FileCryptoMetaData
		if err := thrift.NewDecoder(pr).Decode(&cryptoMeta); err != nil {
			return nil, fmt.Errorf("reading FileCryptoMetaData: %w", err)
		}
		encFooterEnvelope := data[pr.BytesRead():]

		// Extract AAD parameters from the encryption algorithm. The values
		// alias data, which is not retained, so they are cloned below.
		var fileUnique, aadPrefix []byte
		switch algo := cryptoMeta.EncryptionAlgorithm.Value.(type) {
		case *format.AesGcmV1:
			fileUnique = algo.AadFileUnique
			aadPrefix = algo.AadPrefix
		case *format.AesGcmCtrV1:
			fileUnique = algo.AadFileUnique
			aadPrefix = algo.AadPrefix
		}

		footerKey, err := c.Decryption.Keys.FooterKey(cryptoMeta.KeyMetadata)
		if err != nil {
			return nil, fmt.Errorf("retrieving footer key: %w", err)
		}
		footerAAD := makeAAD(aadPrefix, fileUnique, footerModule)
		plainFooter, err := decryptModule(footerKey, footerAAD, encFooterEnvelope)
		if err != nil {
			return nil, fmt.Errorf("decrypting footer: %w", err)
		}
		pr = protocol.NewReaderFromBytes(plainFooter)
		if err := thrift.NewDecoder(pr).Decode(&f.metadata); err != nil {
			return nil, fmt.Errorf("reading parquet file metadata from decrypted footer: %w", err)
		}
		if n := len(plainFooter) - pr.BytesRead(); n != 0 {
			return nil, fmt.Errorf("reading parquet file metadata from decrypted footer: unexpected trailing bytes at the end of thrift input: %d", n)
		}
		f.data = plainFooter
		f.decryption = c.Decryption
		f.fileUnique = slices.Clone(fileUnique)
		f.aadPrefix = slices.Clone(aadPrefix)
	} else {
		// Decoded strings and byte slices alias the input, so make a private
		// copy for the footer to own. Decode using a reader that tracks
		// bytes consumed, so that we can detect a trailing 28-byte AES-GCM
		// signature without treating it as trailing garbage.
		f.data = bytes.Clone(data)
		pr := protocol.NewReaderFromBytes(f.data)
		if err := thrift.NewDecoder(pr).Decode(&f.metadata); err != nil {
			return nil, fmt.Errorf("reading parquet file metadata: %w", err)
		}
		trailing := len(f.data) - pr.BytesRead()
		const sigLen = encNonceSize + encTagSize
		switch {
		case trailing == 0:
			// Plain, unsigned footer — nothing to do.
		case trailing == sigLen:
			// Plaintext footer with AES-GCM signature appended.
			if c.Decryption == nil {
				return nil, fmt.Errorf("parquet file has a signed footer but no DecryptionConfig was provided")
			}
			var fileUnique, aadPrefix []byte
			switch algo := f.metadata.EncryptionAlgorithm.Value.(type) {
			case *format.AesGcmV1:
				fileUnique = algo.AadFileUnique
				aadPrefix = algo.AadPrefix
			case *format.AesGcmCtrV1:
				fileUnique = algo.AadFileUnique
				aadPrefix = algo.AadPrefix
			}
			plainFooterBytes := f.data[:pr.BytesRead()]
			sig := f.data[pr.BytesRead():]

			footerKey, err := c.Decryption.Keys.FooterKey(f.metadata.FooterSigningKeyMetadata)
			if err != nil {
				return nil, fmt.Errorf("retrieving footer signing key: %w", err)
			}
			footerAAD := makeAAD(aadPrefix, fileUnique, footerModule)
			if err := verifyFooterSignature(footerKey, footerAAD, plainFooterBytes, sig); err != nil {
				return nil, err
			}
			f.decryption = c.Decryption
			f.fileUnique = slices.Clone(fileUnique)
			f.aadPrefix = slices.Clone(aadPrefix)
		default:
			return nil, fmt.Errorf("reading parquet file metadata: unexpected trailing bytes at the end of thrift input: %d", trailing)
		}
	}

	if len(f.metadata.Schema) == 0 {
		return nil, ErrMissingRootColumn
	}

	// Normalization: everything below is the reason Footer values are safe
	// to share between files — OpenFile never has to mutate the metadata
	// because construction leaves it fully normalized.

	// Files from writers that omit the optional Ordinal field have all
	// zeros; back-fill sequential values so all downstream code (page-index
	// lookup, AAD construction) can rely on rg.Ordinal == slice index.
	if err := normalizeRowGroupOrdinals(f.metadata.RowGroups); err != nil {
		return nil, err
	}
	if err := validateRowCounts(f.metadata.NumRows, f.metadata.RowGroups); err != nil {
		return nil, err
	}

	// In plaintext-footer mode with encryption, column metadata is stored as
	// EncryptedColumnMetadata. Decrypt it into MetaData before the footer is
	// used — column index/offset index offsets live in MetaData and would
	// otherwise be zero, silently losing the page index for every encrypted
	// plaintext-footer file.
	if f.decryption != nil {
		if err := f.decryptAllColumnMetadata(); err != nil {
			return nil, err
		}
	}

	// Lookup performs a binary search on the key/value metadata.
	sortKeyValueMetadata(f.metadata.KeyValueMetadata)
	return f, nil
}

// decryptAllColumnMetadata decrypts EncryptedColumnMetadata for every column
// chunk in every row group, restoring ColumnChunk.MetaData so that
// openColumns and schema construction see the full encoding/compression
// info. Row group ordinals must have been normalized beforehand because they
// are embedded in every AAD.
func (f *Footer) decryptAllColumnMetadata() error {
	for rgIdx := range f.metadata.RowGroups {
		rg := &f.metadata.RowGroups[rgIdx]
		for colIdx := range rg.Columns {
			chunk := &rg.Columns[colIdx]
			if len(chunk.EncryptedColumnMetadata) == 0 {
				continue
			}
			var key []byte
			switch crypto := chunk.CryptoMetadata.Value.(type) {
			case *format.EncryptionWithFooterKey:
				var err error
				key, err = f.decryption.Keys.FooterKey(nil)
				if err != nil {
					return fmt.Errorf("resolving footer key for column metadata: %w", err)
				}
			case *format.EncryptionWithColumnKey:
				var err error
				key, err = f.decryption.Keys.ColumnKey(crypto.PathInSchema, crypto.KeyMetadata)
				if err != nil {
					// Only treat an explicit ErrKeyNotFound as non-fatal: the
					// caller intentionally omitted this column's key.  Any other
					// error (KMS failure, bad metadata, …) is propagated so the
					// caller sees the real problem instead of silent zero data.
					if errors.Is(err, ErrKeyNotFound) {
						continue
					}
					return fmt.Errorf("resolving column key for column metadata: %w", err)
				}
			default:
				continue
			}
			aad := makeAAD(f.aadPrefix, f.fileUnique, columnMetaDataModule, rg.Ordinal, int16(colIdx))
			plain, err := decryptModule(key, aad, chunk.EncryptedColumnMetadata)
			if err != nil {
				return fmt.Errorf("decrypting column metadata: rowGroup=%d col=%d: %w", rg.Ordinal, colIdx, err)
			}
			compact := thrift.CompactProtocol{}
			if err := thrift.Unmarshal(&compact, plain, &chunk.MetaData); err != nil {
				return fmt.Errorf("decoding column metadata: rowGroup=%d col=%d: %w", rg.Ordinal, colIdx, err)
			}
		}
	}
	return nil
}
