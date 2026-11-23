package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/parquet-go/parquet-go/internal/memory"
)

const (
	defaultDictBufferSize = 8192
	defaultReadBufferSize = 4096
)

// File represents a parquet file. The layout of a Parquet file can be found
// here: https://github.com/apache/parquet-format#file-format
type File struct {
	metadata      format.FileMetaData
	protocol      thrift.CompactProtocol
	reader        io.ReaderAt
	size          int64
	schema        *Schema
	root          *Column
	columnIndexes []format.ColumnIndex
	offsetIndexes []format.OffsetIndex
	rowGroups     []RowGroup
	config        *FileConfig

	// Decryption state; non-nil when the file has encryption metadata.
	decryption *DecryptionConfig
	fileUnique []byte // AadFileUnique from encryption algorithm
	aadPrefix  []byte // AadPrefix from encryption algorithm
	// legacyModules is set when a module authenticates with the AAD parquet-go
	// wrote before the spec module-type fix. Later modules then skip the spec attempt.
	legacyModules atomic.Bool
	// ctrPages is set for AES_GCM_CTR_V1. Modules other than column pages stay
	// AES-GCM; the page bodies use CTR, which this package does not decrypt.
	ctrPages bool
}

type FileView interface {
	Metadata() *format.FileMetaData
	Schema() *Schema
	NumRows() int64
	Lookup(key string) (string, bool)
	Size() int64
	Root() *Column
	RowGroups() []RowGroup
	ColumnIndexes() []format.ColumnIndex
	OffsetIndexes() []format.OffsetIndex
}

// OpenFile opens a parquet file and reads the content between offset 0 and the given
// size in r.
//
// Only the parquet magic bytes and footer are read, column chunks and other
// parts of the file are left untouched; this means that successfully opening
// a file does not validate that the pages have valid checksums.
func OpenFile(r io.ReaderAt, size int64, options ...FileOption) (*File, error) {
	c, err := NewFileConfig(options...)
	if err != nil {
		return nil, err
	}
	f := &File{reader: r, size: size, config: c}

	var headerMagic [4]byte
	if !c.SkipMagicBytes {
		if _, err := readAt(r, headerMagic[:], 0); err != nil {
			return nil, fmt.Errorf("reading magic header of parquet file: %w", err)
		}
		switch string(headerMagic[:]) {
		case "PAR1":
			// plain or plaintext-footer-with-signature
		case "PARE":
			// encrypted footer
			if c.Decryption == nil {
				return nil, fmt.Errorf("parquet file has encrypted footer (magic \"PARE\") but no DecryptionConfig was provided")
			}
		default:
			return nil, fmt.Errorf("invalid magic header of parquet file: %q", headerMagic[:])
		}
	}

	if cast, ok := f.reader.(interface{ SetMagicFooterSection(offset, length int64) }); ok {
		cast.SetMagicFooterSection(size-8, 8)
	}

	optimisticRead := c.OptimisticRead
	optimisticFooterSize := min(int64(c.ReadBufferSize), size)
	if !optimisticRead || optimisticFooterSize < 8 {
		optimisticFooterSize = 8
	}
	optimisticFooterData := make([]byte, optimisticFooterSize)
	if optimisticRead {
		f.reader = &optimisticFileReaderAt{
			reader: f.reader,
			offset: size - optimisticFooterSize,
			footer: optimisticFooterData,
		}
	}

	if n, err := readAt(r, optimisticFooterData, size-optimisticFooterSize); n != len(optimisticFooterData) {
		return nil, fmt.Errorf("reading magic footer of parquet file: %w (read: %d)", err, n)
	}
	optimisticFooterSize -= 8
	b := optimisticFooterData[optimisticFooterSize:]
	footerMagic := string(b[4:])
	if footerMagic != "PAR1" && footerMagic != "PARE" {
		return nil, fmt.Errorf("invalid magic footer of parquet file: %q", b[4:])
	}

	footerSize := int64(binary.LittleEndian.Uint32(b[:4]))
	footerData := []byte(nil)

	if footerSize <= optimisticFooterSize {
		footerData = optimisticFooterData[optimisticFooterSize-footerSize : optimisticFooterSize]
	} else {
		// The footer and its eight byte trailer have to fit in the file. The
		// length comes from the file, so a small file can claim a footer of
		// up to 4 GB; checking it here keeps a damaged file from making the
		// process allocate that much memory before the read fails.
		if footerSize > size-8 {
			return nil, fmt.Errorf("parquet file footer of %d bytes does not fit in a file of %d bytes", footerSize, size)
		}
		footerData = make([]byte, footerSize)
		if cast, ok := f.reader.(interface{ SetFooterSection(offset, length int64) }); ok {
			cast.SetFooterSection(size-(footerSize+8), footerSize)
		}
		if _, err := f.readAt(footerData, size-(footerSize+8)); err != nil {
			return nil, fmt.Errorf("reading footer of parquet file: %w", err)
		}
	}

	if footerMagic == "PARE" {
		// Encrypted footer: footerData = FileCryptoMetaData (thrift) || encrypted footer module.
		// Decode FileCryptoMetaData using a bytes-backed reader to count bytes consumed.
		pr := f.protocol.NewReaderFromBytes(footerData)
		var cryptoMeta format.FileCryptoMetaData
		if err := thrift.NewDecoder(pr).Decode(&cryptoMeta); err != nil {
			return nil, fmt.Errorf("reading FileCryptoMetaData: %w", err)
		}
		encFooterEnvelope := footerData[pr.BytesRead():]

		fileUnique, aadPrefix, supplyPrefix, ctrPages := encryptionAADParams(cryptoMeta.EncryptionAlgorithm)
		if supplyPrefix {
			return nil, errSupplyAADPrefix()
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
		// parquet-mr encrypts the whole TMemoryBuffer backing array, so the
		// authenticated plaintext is the FileMetaData thrift followed by zero
		// padding. The struct is self-delimiting; the zeros are not part of it.
		if err := unmarshalThriftAllowPadding(&f.protocol, plainFooter, &f.metadata); err != nil {
			return nil, fmt.Errorf("reading parquet file metadata from decrypted footer: %w", err)
		}
		f.decryption = c.Decryption
		f.fileUnique = fileUnique
		f.aadPrefix = aadPrefix
		f.ctrPages = ctrPages
	} else {
		// Decode the footer metadata using a reader that tracks bytes consumed,
		// so that we can detect a trailing 28-byte AES-GCM signature without
		// tripping over the "unexpected trailing bytes" check in thrift.Unmarshal.
		pr := f.protocol.NewReaderFromBytes(bytes.Clone(footerData))
		if err := thrift.NewDecoder(pr).Decode(&f.metadata); err != nil {
			return nil, fmt.Errorf("reading parquet file metadata: %w", err)
		}
		trailing := len(footerData) - pr.BytesRead()
		const sigLen = encNonceSize + encTagSize
		switch {
		case trailing == 0:
			// Plain, unsigned footer — nothing to do.
		case trailing == sigLen:
			// Plaintext footer with AES-GCM signature appended.
			if c.Decryption == nil {
				return nil, fmt.Errorf("parquet file has a signed footer but no DecryptionConfig was provided")
			}
			fileUnique, aadPrefix, supplyPrefix, ctrPages := encryptionAADParams(f.metadata.EncryptionAlgorithm)
			if supplyPrefix {
				return nil, errSupplyAADPrefix()
			}
			plainFooterBytes := footerData[:pr.BytesRead()]
			sig := footerData[pr.BytesRead():]

			footerKey, err := c.Decryption.Keys.FooterKey(f.metadata.FooterSigningKeyMetadata)
			if err != nil {
				return nil, fmt.Errorf("retrieving footer signing key: %w", err)
			}
			footerAAD := makeAAD(aadPrefix, fileUnique, footerModule)
			legacySig, err := verifyFooterSignature(footerKey, footerAAD, plainFooterBytes, sig)
			if err != nil {
				return nil, err
			}
			if legacySig {
				f.legacyModules.Store(true)
			}
			f.decryption = c.Decryption
			f.fileUnique = fileUnique
			f.aadPrefix = aadPrefix
			f.ctrPages = ctrPages
		default:
			return nil, fmt.Errorf("reading parquet file metadata: unexpected trailing bytes at the end of thrift input: %d", trailing)
		}
	}

	if len(f.metadata.Schema) == 0 {
		return nil, ErrMissingRootColumn
	}

	// In plaintext-footer mode with encryption, column metadata is stored as
	// EncryptedColumnMetadata. Decrypt it into MetaData before reading the page
	// index — column index/offset index offsets live in MetaData and would
	// otherwise be zero, silently losing the page index for every encrypted
	// plaintext-footer file.
	if f.decryption != nil {
		if err := f.decryptAllColumnMetadata(); err != nil {
			return nil, err
		}
	}

	if !c.SkipPageIndex {
		if f.columnIndexes, f.offsetIndexes, err = f.ReadPageIndex(); err != nil {
			return nil, fmt.Errorf("reading page index of parquet file: %w", err)
		}
	}

	if f.root, err = openColumns(f, &f.metadata, f.columnIndexes, f.offsetIndexes); err != nil {
		return nil, fmt.Errorf("opening columns of parquet file: %w", err)
	}

	if c.Schema != nil {
		f.schema = c.Schema
	} else {
		f.schema = NewSchema(f.root.Name(), f.root)
	}
	columns := makeLeafColumns(f.root)
	rowGroups, err := makeFileRowGroups(f, columns)
	if err != nil {
		return nil, err
	}
	f.rowGroups = makeRowGroups(rowGroups)

	if !c.SkipBloomFilters {
		section := io.NewSectionReader(f.reader, 0, size)
		rbuf, rbufpool := getBufioReader(section, c.ReadBufferSize)
		defer putBufioReader(rbuf, rbufpool)

		header := format.BloomFilterHeader{}
		compact := thrift.CompactProtocol{}
		decoder := thrift.NewDecoder(compact.NewReader(rbuf))

		for i := range rowGroups {
			g := &rowGroups[i]

			for j := range g.columns {
				cc := g.columns[j].(*FileColumnChunk)

				if offset := cc.chunk.MetaData.BloomFilterOffset; offset > 0 {
					// Encrypted columns cannot be decoded here because the bloom
					// filter bytes on disk are AES-GCM envelopes, not plain
					// thrift.  Defer loading to the lazy readBloomFilter path.
					if cc.decryptionKey != nil {
						continue
					}

					// Seek only fails when the target is before the section base.
					// This section starts at byte 0 and the offset is positive.
					_, _ = section.Seek(offset, io.SeekStart)
					rbuf.Reset(section)

					header = format.BloomFilterHeader{}
					if err := decoder.Decode(&header); err != nil {
						return nil, fmt.Errorf("decoding bloom filter header: %w", err)
					}

					offset, _ = section.Seek(0, io.SeekCurrent)
					offset -= int64(rbuf.Buffered())

					if cast, ok := r.(interface{ SetBloomFilterSection(offset, length int64) }); ok {
						bloomFilterOffset := cc.chunk.MetaData.BloomFilterOffset
						bloomFilterLength := (offset - bloomFilterOffset) + int64(header.NumBytes)
						cast.SetBloomFilterSection(bloomFilterOffset, bloomFilterLength)
					}

					if c.PrefetchBloomFilters {
						bloomData := make([]byte, header.NumBytes)
						if _, err := io.ReadFull(rbuf, bloomData); err != nil {
							return nil, fmt.Errorf("reading bloom filter data: %w", err)
						}
						// Create bloom filter using in-memory data.
						bf, err := newBloomFilter(bytes.NewReader(bloomData), 0, &header)
						if err != nil {
							return nil, fmt.Errorf("reading bloom filter: %w", err)
						}
						cc.bloomFilter.Store(bf)
					} else {
						bf, err := newBloomFilter(f, offset, &header)
						if err != nil {
							return nil, fmt.Errorf("reading bloom filter: %w", err)
						}
						cc.bloomFilter.Store(bf)
					}
				}
			}
		}
	}

	sortKeyValueMetadata(f.metadata.KeyValueMetadata)
	f.reader = r // restore in case an optimistic reader was used
	return f, nil
}

const maxPageIndexGap = 1 << 20

type pageIndexRange struct {
	present bool
	offset  int64
	end     int64
	total   int64
}

func (r *pageIndexRange) add(offset int64, length int32, fileSize int64, kind string) error {
	if offset < 0 || length < 0 {
		return fmt.Errorf("invalid %s range: offset=%d length=%d", kind, offset, length)
	}
	if offset == 0 {
		return nil
	}
	n := int64(length)
	if offset > fileSize || n > fileSize-offset {
		return fmt.Errorf("%s range offset=%d length=%d exceeds file size %d", kind, offset, length, fileSize)
	}
	r.present = true
	r.total += n
	if n > 0 {
		end := offset + n
		if r.offset == 0 || offset < r.offset {
			r.offset = offset
		}
		if end > r.end {
			r.end = end
		}
	}
	return nil
}

// ReadPageIndex reads the page index section of the parquet file f.
//
// If the file did not contain a page index, the method returns two empty slices
// and a nil error.
//
// Only leaf columns have indexes, the returned indexes are arranged using the
// following layout:
//
//	------------------
//	| col 0: chunk 0 |
//	------------------
//	| col 1: chunk 0 |
//	------------------
//	| ...            |
//	------------------
//	| col 0: chunk 1 |
//	------------------
//	| col 1: chunk 1 |
//	------------------
//	| ...            |
//	------------------
//
// This method is useful in combination with the SkipPageIndex option to delay
// reading the page index section until after the file was opened. Note that in
// this case the page index is not cached within the file, programs are expected
// to make use of independently from the parquet package.
func (f *File) ReadPageIndex() ([]format.ColumnIndex, []format.OffsetIndex, error) {
	if len(f.metadata.RowGroups) == 0 || len(f.metadata.RowGroups[0].Columns) == 0 {
		return nil, nil, nil
	}

	forEachColumnChunk := func(do func(int, int, *format.ColumnChunk) error) error {
		for i := range f.metadata.RowGroups {
			for j := range f.metadata.RowGroups[i].Columns {
				c := &f.metadata.RowGroups[i].Columns[j]
				if err := do(i, j, c); err != nil {
					return err
				}
			}
		}
		return nil
	}

	var columnIndexRange, offsetIndexRange pageIndexRange
	err := forEachColumnChunk(func(_, _ int, c *format.ColumnChunk) error {
		if err := columnIndexRange.add(c.ColumnIndexOffset, c.ColumnIndexLength, f.size, "column index"); err != nil {
			return err
		}
		return offsetIndexRange.add(c.OffsetIndexOffset, c.OffsetIndexLength, f.size, "offset index")
	})
	if err != nil {
		return nil, nil, err
	}

	if !columnIndexRange.present && !offsetIndexRange.present {
		return nil, nil, nil
	}

	numRowGroups := len(f.metadata.RowGroups)
	numColumns := len(f.metadata.RowGroups[0].Columns)
	numColumnChunks := numRowGroups * numColumns

	// One reader and one decoder are reused across every column chunk. Decoding
	// each chunk with thrift.Unmarshal instead would clone the chunk's bytes and
	// allocate a reader for it, twice per column chunk.
	reader := f.protocol.NewReaderFromBytes(nil)
	decoder := thrift.NewDecoder(reader)

	// allowPad is set for modules decrypted from a parquet-mr file. Those
	// plaintexts carry zero padding after the thrift struct. Plaintext modules
	// stay strict so a truncated index is still reported.
	decode := func(b []byte, v any, allowPad bool) error {
		reader.ResetBytes(b)
		if err := decoder.Decode(v); err != nil {
			return err
		}
		rest := b[reader.BytesRead():]
		if len(rest) == 0 || (allowPad && trailingZeros(rest)) {
			return nil
		}
		return fmt.Errorf("unexpected trailing bytes at the end of thrift input: %d", len(rest))
	}

	// pageIndexKey returns the AES key used to decrypt this column's
	// page-index modules, or nil for plaintext columns.  We resolve keys here
	// (instead of relying on FileRowGroup.init) because ReadPageIndex runs
	// before makeFileRowGroups during OpenFile.
	pageIndexKey := func(c *format.ColumnChunk) ([]byte, error) {
		if f.decryption == nil {
			return nil, nil
		}
		switch crypto := c.CryptoMetadata.Value.(type) {
		case *format.EncryptionWithFooterKey:
			return f.decryption.Keys.FooterKey(nil)
		case *format.EncryptionWithColumnKey:
			return f.decryption.Keys.ColumnKey(crypto.PathInSchema, crypto.KeyMetadata)
		default:
			return nil, nil
		}
	}

	// Column indexes and offset indexes get separate buffers because the values
	// decoded below alias them: ColumnIndex.MinValues and MaxValues are
	// sub-slices of the bytes they were decoded from.
	readSection := func(
		r pageIndexRange,
		kind string,
		module byte,
		setSection func(offset, length int64),
		chunkRange func(*format.ColumnChunk) (int64, int32),
		decodeAt func(idx int, buf []byte, allowPad bool) error,
	) error {
		span := r.end - r.offset
		bulk := span > 0 && (span <= r.total || span-r.total <= maxPageIndexGap)

		var data []byte
		if bulk {
			data = make([]byte, span)
			if setSection != nil {
				setSection(r.offset, span)
			}
			if _, err := f.readAt(data, r.offset); err != nil {
				return fmt.Errorf("reading %d bytes %s at offset %d: %w", span, kind, r.offset, err)
			}
		} else if r.total > 0 {
			data = make([]byte, r.total)
		}

		pos := int64(0)
		return forEachColumnChunk(func(i, j int, c *format.ColumnChunk) error {
			offset, length := chunkRange(c)
			if offset == 0 {
				return nil
			}
			var buffer []byte
			if n := int64(length); n > 0 {
				if bulk {
					rel := offset - r.offset
					buffer = data[rel : rel+n]
				} else {
					buffer = data[pos : pos+n]
					pos += n
					if _, err := f.readAt(buffer, offset); err != nil {
						return fmt.Errorf("reading %d bytes %s at offset %d: %w", n, kind, offset, err)
					}
				}
			}
			key, err := pageIndexKey(c)
			if err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					return nil
				}
				return fmt.Errorf("resolving %s key: rowGroup=%d col=%d: %w", kind, i, j, err)
			}
			if key != nil {
				plain, err := decryptWithLegacy(key, buffer, f.aadPrefix, f.fileUnique, module, []int16{int16(i), int16(j)}, &f.legacyModules)
				if err != nil {
					return fmt.Errorf("decrypting %s: rowGroup=%d col=%d: %w", kind, i, j, err)
				}
				buffer = plain
			}
			if err := decodeAt((i*numColumns)+j, buffer, key != nil); err != nil {
				return fmt.Errorf("decoding %s: rowGroup=%d columnChunk=%d/%d: %w", kind, i, j, numColumns, err)
			}
			return nil
		})
	}

	var columnIndexes []format.ColumnIndex
	if columnIndexRange.present {
		columnIndexes = make([]format.ColumnIndex, numColumnChunks)
		var setSection func(offset, length int64)
		if cast, ok := f.reader.(interface{ SetColumnIndexSection(offset, length int64) }); ok {
			setSection = cast.SetColumnIndexSection
		}
		err := readSection(
			columnIndexRange,
			"column index",
			columnIndexModule,
			setSection,
			func(c *format.ColumnChunk) (int64, int32) {
				return c.ColumnIndexOffset, c.ColumnIndexLength
			},
			func(idx int, buf []byte, allowPad bool) error {
				return decode(buf, &columnIndexes[idx], allowPad)
			},
		)
		if err != nil {
			return nil, nil, err
		}
	}

	var offsetIndexes []format.OffsetIndex
	if offsetIndexRange.present {
		offsetIndexes = make([]format.OffsetIndex, numColumnChunks)
		var setSection func(offset, length int64)
		if cast, ok := f.reader.(interface{ SetOffsetIndexSection(offset, length int64) }); ok {
			setSection = cast.SetOffsetIndexSection
		}
		err := readSection(
			offsetIndexRange,
			"offset index",
			offsetIndexModule,
			setSection,
			func(c *format.ColumnChunk) (int64, int32) {
				return c.OffsetIndexOffset, c.OffsetIndexLength
			},
			func(idx int, buf []byte, allowPad bool) error {
				return decode(buf, &offsetIndexes[idx], allowPad)
			},
		)
		if err != nil {
			return nil, nil, err
		}
	}

	return columnIndexes, offsetIndexes, nil
}

// encryptionAADParams reads the AAD inputs and algorithm variant stored in
// either FileCryptoMetaData or FileMetaData.
func encryptionAADParams(algo format.EncryptionAlgorithm) (fileUnique, aadPrefix []byte, supplyPrefix, ctrPages bool) {
	switch v := algo.Value.(type) {
	case *format.AesGcmV1:
		return v.AadFileUnique, v.AadPrefix, v.SupplyAadPrefix, false
	case *format.AesGcmCtrV1:
		return v.AadFileUnique, v.AadPrefix, v.SupplyAadPrefix, true
	default:
		return nil, nil, false, false
	}
}

// unmarshalThriftAllowPadding decodes one thrift value and accepts a tail of
// zero bytes. parquet-mr encrypts TMemoryBuffer.getArray(), whose capacity is
// larger than the bytes actually written; the unused tail is zeros.
func unmarshalThriftAllowPadding(p thrift.Protocol, b []byte, v any) error {
	pr := p.NewReaderFromBytes(slices.Clone(b))
	if err := thrift.NewDecoder(pr).Decode(v); err != nil {
		return err
	}
	rest := b[pr.BytesRead():]
	for _, c := range rest {
		if c != 0 {
			return fmt.Errorf("unexpected trailing bytes at the end of thrift input: %d", len(rest))
		}
	}
	return nil
}

// trailingZeros reports whether b is empty or contains only zero bytes.
func trailingZeros(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// decryptAllColumnMetadata decrypts EncryptedColumnMetadata for every column
// chunk in every row group, restoring ColumnChunk.MetaData so that
// openColumns and schema construction see the full encoding/compression info.
// This must be called before openColumns.
func (f *File) decryptAllColumnMetadata() error {
	// Normalize ordinals before using them in AADs.  Files from writers that
	// omit the optional Ordinal field have all zeros; validateRowGroupOrdinals
	// back-fills sequential values so rg.Ordinal is guaranteed to equal the
	// slice index — matching exactly what the writer embedded in every AAD.
	if err := validateRowGroupOrdinals(f.metadata.RowGroups); err != nil {
		return err
	}

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
			// rg.Ordinal is reliable here because validateRowGroupOrdinals ran above.
			plain, err := decryptWithLegacy(key, chunk.EncryptedColumnMetadata, f.aadPrefix, f.fileUnique, columnMetaDataModule, []int16{rg.Ordinal, int16(colIdx)}, &f.legacyModules)
			if err != nil {
				return fmt.Errorf("decrypting column metadata: rowGroup=%d col=%d: %w", rg.Ordinal, colIdx, err)
			}
			compact := thrift.CompactProtocol{}
			if err := unmarshalThriftAllowPadding(&compact, plain, &chunk.MetaData); err != nil {
				return fmt.Errorf("decoding column metadata: rowGroup=%d col=%d: %w", rg.Ordinal, colIdx, err)
			}
		}
	}
	return nil
}

// NumRows returns the number of rows in the file.
func (f *File) NumRows() int64 { return f.metadata.NumRows }

// RowGroups returns the list of row groups in the file.
//
// Elements of the returned slice are guaranteed to be of type *FileRowGroup.
func (f *File) RowGroups() []RowGroup { return f.rowGroups }

// Root returns the root column of f.
func (f *File) Root() *Column { return f.root }

// Schema returns the schema of f.
func (f *File) Schema() *Schema { return f.schema }

// Metadata returns the metadata of f.
func (f *File) Metadata() *format.FileMetaData { return &f.metadata }

// Size returns the size of f (in bytes).
func (f *File) Size() int64 { return f.size }

// ReadAt reads bytes into b from f at the given offset.
//
// The method satisfies the io.ReaderAt interface.
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= f.size {
		return 0, io.EOF
	}

	if limit := f.size - off; limit < int64(len(b)) {
		n, err := f.readAt(b[:limit], off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}

	return f.readAt(b, off)
}

// ColumnIndexes returns the column index of the parquet file f.
//
// If the file did not contain any column indexes, the method returns an empty
// slice. In a partially indexed file, chunks without a column index have
// zero-valued entries; prefer ColumnChunk.ColumnIndex to inspect a single chunk.
func (f *File) ColumnIndexes() []format.ColumnIndex { return f.columnIndexes }

// OffsetIndexes returns the page index of the parquet file f.
//
// If the file did not contain an offset index, the method returns an empty
// slice.
func (f *File) OffsetIndexes() []format.OffsetIndex { return f.offsetIndexes }

// Lookup returns the value associated with the given key in the file key/value
// metadata.
//
// The ok boolean will be true if the key was found, false otherwise.
func (f *File) Lookup(key string) (value string, ok bool) {
	return lookupKeyValueMetadata(f.metadata.KeyValueMetadata, key)
}

var _ io.ReaderAt = (*File)(nil)

func sortKeyValueMetadata(keyValueMetadata []format.KeyValue) {
	slices.SortFunc(keyValueMetadata, func(a, b format.KeyValue) int {
		if cmp := strings.Compare(a.Key, b.Key); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Value, b.Value)
	})
}

func lookupKeyValueMetadata(keyValueMetadata []format.KeyValue, key string) (value string, ok bool) {
	i, found := slices.BinarySearchFunc(keyValueMetadata, key, func(kv format.KeyValue, key string) int {
		return strings.Compare(kv.Key, key)
	})
	if found {
		return keyValueMetadata[i].Value, true
	}
	return "", false
}

// FileRowGroup is an implementation of the RowGroup interface on parquet files
// returned by OpenFile.
type FileRowGroup struct {
	file     *File
	rowGroup *format.RowGroup
	columns  []ColumnChunk
	sorting  []SortingColumn
}

func (g *FileRowGroup) init(file *File, columns []*Column, rowGroup *format.RowGroup) error {
	g.file = file
	g.rowGroup = rowGroup
	g.columns = make([]ColumnChunk, len(rowGroup.Columns))
	g.sorting = make([]SortingColumn, len(rowGroup.SortingColumns))
	fileColumnChunks := make([]FileColumnChunk, len(rowGroup.Columns))
	var fileColumnIndexes []FileColumnIndex
	if len(file.columnIndexes) > 0 {
		fileColumnIndexes = make([]FileColumnIndex, len(rowGroup.Columns))
	}
	var fileOffsetIndexes []FileOffsetIndex
	if len(file.offsetIndexes) > 0 {
		fileOffsetIndexes = make([]FileOffsetIndex, len(rowGroup.Columns))
	}

	for i := range g.columns {
		chunk := &rowGroup.Columns[i]
		fileColumnChunks[i] = FileColumnChunk{
			file:            file,
			column:          columns[i],
			rowGroup:        rowGroup,
			chunk:           chunk,
			columnOrdinal:   int16(i),
			rowGroupOrdinal: rowGroup.Ordinal,
		}
		if file.decryption != nil {
			switch crypto := chunk.CryptoMetadata.Value.(type) {
			case *format.EncryptionWithFooterKey:
				key, err := file.decryption.Keys.FooterKey(nil)
				if err != nil {
					return fmt.Errorf("resolving footer key for column %q: %w", columnPathString(columns[i].Path()), err)
				}
				fileColumnChunks[i].decryptionKey = key
			case *format.EncryptionWithColumnKey:
				key, err := file.decryption.Keys.ColumnKey(crypto.PathInSchema, crypto.KeyMetadata)
				switch {
				case err == nil:
					fileColumnChunks[i].decryptionKey = key
				case errors.Is(err, ErrKeyNotFound):
					// Caller intentionally omitted this column's key.  Leave
					// decryptionKey nil and remember the miss so a later read
					// returns ErrKeyNotFound instead of trying to parse ciphertext.
					fileColumnChunks[i].missingKey = true
				default:
					// Real error (KMS failure, bad metadata, …) must surface so
					// the caller learns the cause instead of seeing later
					// "decode page header" errors masking the missing key.
					return fmt.Errorf("resolving column key for %q: %w", columnPathString(crypto.PathInSchema), err)
				}
			}
		}

		if chunk.CryptoMetadata.Value == nil || fileColumnChunks[i].decryptionKey != nil {
			j := (int(rowGroup.Ordinal) * len(columns)) + i
			if len(file.columnIndexes) > 0 && chunk.ColumnIndexOffset > 0 {
				fileColumnIndexes[i] = makeFileColumnIndex(&file.columnIndexes[j], columns[i].Type())
				fileColumnChunks[i].columnIndex.Store(&fileColumnIndexes[i])
			}
			if len(file.offsetIndexes) > 0 && chunk.OffsetIndexOffset > 0 {
				fileOffsetIndexes[i] = FileOffsetIndex{index: &file.offsetIndexes[j]}
				fileColumnChunks[i].offsetIndex.Store(&fileOffsetIndexes[i])
			}
		}

		g.columns[i] = &fileColumnChunks[i]
	}

	for i, sorting := range rowGroup.SortingColumns {
		column := columns[sorting.ColumnIdx]
		if column.ignoreOrder {
			// Later sorting keys are ordered only within ties on this key.
			g.sorting = g.sorting[:i]
			break
		}
		g.sorting[i] = &fileSortingColumn{
			column:     column,
			descending: sorting.Descending,
			nullsFirst: sorting.NullsFirst,
		}
	}
	return nil
}

// File returns the file that this row group belongs to.
func (g *FileRowGroup) File() *File { return g.file }

// Schema returns the schema of the row group.
func (g *FileRowGroup) Schema() *Schema { return g.file.schema }

// NumRows returns the number of rows in the row group.
func (g *FileRowGroup) NumRows() int64 { return g.rowGroup.NumRows }

// ColumnChunks returns the list of column chunks in the row group.
//
// Elements of the returned slice are guaranteed to be of type *FileColumnChunk.
func (g *FileRowGroup) ColumnChunks() []ColumnChunk { return g.columns }

// SortingColumns returns the list of sorting columns in the row group.
func (g *FileRowGroup) SortingColumns() []SortingColumn { return g.sorting }

// Rows returns a row reader for the row group.
func (g *FileRowGroup) Rows() Rows {
	rowGroup := RowGroup(g)
	if g.file.config.ReadMode == ReadModeAsync {
		rowGroup = AsyncRowGroup(rowGroup)
	}
	return NewRowGroupRowReader(rowGroup)
}

// chunkTransparentRowGroup marks FileRowGroup as safe for the chunk-level write
// fast paths: its Rows() reads the file column chunks in order with no
// additional semantics.
func (g *FileRowGroup) chunkTransparentRowGroup() {}

type fileSortingColumn struct {
	column     *Column
	descending bool
	nullsFirst bool
}

func (s *fileSortingColumn) Path() []string   { return s.column.Path() }
func (s *fileSortingColumn) Descending() bool { return s.descending }
func (s *fileSortingColumn) NullsFirst() bool { return s.nullsFirst }
func (s *fileSortingColumn) String() string {
	b := new(strings.Builder)
	if s.nullsFirst {
		b.WriteString("nulls_first+")
	}
	if s.descending {
		b.WriteString("descending(")
	} else {
		b.WriteString("ascending(")
	}
	b.WriteString(columnPath(s.Path()).String())
	b.WriteString(")")
	return b.String()
}

// FileColumnChunk is an implementation of the ColumnChunk interface on parquet
// files returned by OpenFile.
type FileColumnChunk struct {
	file        *File
	column      *Column
	rowGroup    *format.RowGroup
	chunk       *format.ColumnChunk
	columnIndex atomic.Pointer[FileColumnIndex]
	offsetIndex atomic.Pointer[FileOffsetIndex]
	bloomFilter atomic.Pointer[FileBloomFilter]

	// Decryption state; decryptionKey is nil when the column is not encrypted.
	// missingKey is set when the column is encrypted and the retriever returned
	// ErrKeyNotFound.
	decryptionKey   []byte
	missingKey      bool
	columnOrdinal   int16
	rowGroupOrdinal int16
}

func (c *FileColumnChunk) errIfMissingKey() error {
	if !c.missingKey {
		return nil
	}
	return fmt.Errorf("reading encrypted column %q: %w", columnPathString(c.column.Path()), ErrKeyNotFound)
}

// File returns the file that this column chunk belongs to.
func (c *FileColumnChunk) File() *File { return c.file }

// Node returns the node that this column chunk belongs to in the parquet schema.
func (c *FileColumnChunk) Node() Node { return c.column }

// Type returns the type of the column chunk.
func (c *FileColumnChunk) Type() Type { return c.column.Type() }

// Column returns the column index of this chunk in its parent row group.
func (c *FileColumnChunk) Column() int { return int(c.column.Index()) }

// Bounds returns the min and max values found in the column chunk. The values
// are the footer statistics, decoded as stored.
//
// ok is false when either statistic is missing, when the column order is
// undefined, or when a FLOAT or DOUBLE statistic is NaN.
//
// Footer statistics use the type-defined column order. Under that order a
// minimum of +0 also admits -0, and a maximum of -0 also admits +0. Bounds
// returns the stored endpoints with their signs unchanged, so callers that
// need both zeros covered widen a +0 minimum to -0 and a -0 maximum to +0.
func (c *FileColumnChunk) Bounds() (min, max Value, ok bool) {
	if c.column.ignoreOrder {
		return min, max, false
	}
	stats := &c.chunk.MetaData.Statistics
	columnKind := c.Type().Kind()
	hasMinValue := stats.MinValue != nil
	hasMaxValue := stats.MaxValue != nil
	if hasMinValue {
		min = columnKind.Value(stats.MinValue)
	}
	if hasMaxValue {
		max = columnKind.Value(stats.MaxValue)
	}
	ok = hasMinValue && hasMaxValue
	if ok {
		switch columnKind {
		case Float:
			ok = !math.IsNaN(float64(min.Float())) && !math.IsNaN(float64(max.Float()))
		case Double:
			ok = !math.IsNaN(min.Double()) && !math.IsNaN(max.Double())
		case FixedLenByteArray:
			if _, isFloat16 := c.column.Type().(float16Type); isFloat16 {
				ok = !float16IsNaN(min.ByteArray()) && !float16IsNaN(max.ByteArray())
			}
		}
	}
	return min, max, ok
}

// Pages returns a page reader for the column chunk.
func (c *FileColumnChunk) Pages() Pages {
	pages := Pages(c.PagesFrom(c.file.reader))
	if c.file.config.ReadMode == ReadModeAsync {
		pages = AsyncPages(pages)
	}
	return pages
}

// PagesFrom returns a page reader for the column chunk, using the reader passed
// as argument instead of the one that the file was originally opened from.
//
// Note that unlike when calling Pages, the returned reader is not wrapped in an
// AsyncPages reader if the file was opened in async mode.
func (c *FileColumnChunk) PagesFrom(reader io.ReaderAt) *FilePages {
	pages := new(FilePages)
	pages.init(c, reader)
	return pages
}

// ColumnIndex returns the column index of the column chunk, or an error if it
// didn't exist or couldn't be read.
func (c *FileColumnChunk) ColumnIndex() (ColumnIndex, error) {
	index, err := c.ColumnIndexFrom(c.file.reader)
	if err != nil {
		return nil, err
	}
	return index, nil
}

// ColumnIndexFrom is like ColumnIndex but uses the reader passed as argument to
// read the column index.
func (c *FileColumnChunk) ColumnIndexFrom(reader io.ReaderAt) (*FileColumnIndex, error) {
	index, err := c.readColumnIndexFrom(reader)
	if err != nil {
		return nil, err
	}
	if index == nil || c.chunk.ColumnIndexOffset == 0 {
		return nil, ErrMissingColumnIndex
	}
	return index, nil
}

// OffsetIndex returns the offset index of the column chunk, or an error if it
// didn't exist or couldn't be read.
func (c *FileColumnChunk) OffsetIndex() (OffsetIndex, error) {
	index, err := c.OffsetIndexFrom(c.file.reader)
	if err != nil {
		return nil, err
	}
	return index, nil
}

// OffsetIndexFrom is like OffsetIndex but uses the reader passed as argument to
// read the offset index.
func (c *FileColumnChunk) OffsetIndexFrom(reader io.ReaderAt) (*FileOffsetIndex, error) {
	index, err := c.readOffsetIndex(reader)
	if err != nil {
		return nil, err
	}
	if index == nil || c.chunk.OffsetIndexOffset == 0 {
		return nil, ErrMissingOffsetIndex
	}
	return index, nil
}

// BloomFilter returns the bloom filter of the column chunk, or nil if it didn't
// have one.
func (c *FileColumnChunk) BloomFilter() BloomFilter {
	filter, err := c.BloomFilterFrom(c.file.reader)
	switch err {
	case nil:
		return filter
	case ErrMissingBloomFilter:
		return nil
	default:
		return &errorBloomFilter{err: err}
	}
}

// BloomFilterFrom is like BloomFilter but uses the reader passed as argument to
// read the bloom filter.
func (c *FileColumnChunk) BloomFilterFrom(reader io.ReaderAt) (*FileBloomFilter, error) {
	filter, err := c.readBloomFilter(reader)
	if err != nil {
		return nil, err
	}
	if filter == nil || c.chunk.MetaData.BloomFilterOffset == 0 {
		return nil, ErrMissingBloomFilter
	}
	return filter, nil
}

// NumValues returns the number of values in the column chunk.
func (c *FileColumnChunk) NumValues() int64 {
	return c.chunk.MetaData.NumValues
}

// NullCount returns the number of null values in the column chunk.
//
// This value is extracted from the column chunk statistics, parquet writers are
// not required to populate it.
func (c *FileColumnChunk) NullCount() int64 {
	return c.chunk.MetaData.Statistics.NullCount
}

func (c *FileColumnChunk) readColumnIndex() (*FileColumnIndex, error) {
	return c.readColumnIndexFrom(c.file.reader)
}

func (c *FileColumnChunk) readColumnIndexFrom(reader io.ReaderAt) (*FileColumnIndex, error) {
	if err := c.errIfMissingKey(); err != nil {
		return nil, err
	}
	if c.column.ignoreOrder {
		return nil, nil
	}
	if index := c.columnIndex.Load(); index != nil {
		return index, nil
	}
	columnChunk := &c.file.metadata.RowGroups[c.rowGroup.Ordinal].Columns[c.Column()]
	offset, length := columnChunk.ColumnIndexOffset, columnChunk.ColumnIndexLength
	if offset == 0 {
		return nil, nil
	}

	indexData := make([]byte, int(length))
	var columnIndex format.ColumnIndex
	if _, err := readAt(reader, indexData, offset); err != nil {
		return nil, fmt.Errorf("read %d bytes column index at offset %d: %w", length, offset, err)
	}
	plain := indexData
	if c.decryptionKey != nil {
		decrypted, err := decryptWithLegacy(c.decryptionKey, indexData, c.file.aadPrefix, c.file.fileUnique, columnIndexModule, []int16{c.rowGroupOrdinal, c.columnOrdinal}, &c.file.legacyModules)
		if err != nil {
			return nil, fmt.Errorf("decrypting column index: rowGroup=%d col=%d: %w", c.rowGroupOrdinal, c.columnOrdinal, err)
		}
		plain = decrypted
	}
	var decErr error
	if c.decryptionKey != nil {
		decErr = unmarshalThriftAllowPadding(&c.file.protocol, plain, &columnIndex)
	} else {
		decErr = thrift.Unmarshal(&c.file.protocol, plain, &columnIndex)
	}
	if decErr != nil {
		return nil, fmt.Errorf("decode column index: rowGroup=%d columnChunk=%d/%d: %w", c.rowGroup.Ordinal, c.Column(), len(c.rowGroup.Columns), decErr)
	}
	index := new(FileColumnIndex)
	*index = makeFileColumnIndex(&columnIndex, c.column.Type())
	// We do a CAS (and Load on CAS failure) instead of a simple Store for
	// the nice property that concurrent calling goroutines will only ever
	// observe a single pointer value for the result.
	if !c.columnIndex.CompareAndSwap(nil, index) {
		// another goroutine populated it since we last read the pointer
		return c.columnIndex.Load(), nil
	}
	return index, nil
}

func (c *FileColumnChunk) readOffsetIndex(reader io.ReaderAt) (*FileOffsetIndex, error) {
	if err := c.errIfMissingKey(); err != nil {
		return nil, err
	}
	if index := c.offsetIndex.Load(); index != nil {
		return index, nil
	}
	columnChunk := &c.file.metadata.RowGroups[c.rowGroup.Ordinal].Columns[c.Column()]
	offset, length := columnChunk.OffsetIndexOffset, columnChunk.OffsetIndexLength
	if offset == 0 {
		return nil, nil
	}

	indexData := make([]byte, int(length))
	var offsetIndex format.OffsetIndex
	if _, err := readAt(reader, indexData, offset); err != nil {
		return nil, fmt.Errorf("read %d bytes offset index at offset %d: %w", length, offset, err)
	}
	plain := indexData
	if c.decryptionKey != nil {
		decrypted, err := decryptWithLegacy(c.decryptionKey, indexData, c.file.aadPrefix, c.file.fileUnique, offsetIndexModule, []int16{c.rowGroupOrdinal, c.columnOrdinal}, &c.file.legacyModules)
		if err != nil {
			return nil, fmt.Errorf("decrypting offset index: rowGroup=%d col=%d: %w", c.rowGroupOrdinal, c.columnOrdinal, err)
		}
		plain = decrypted
	}
	var decErr error
	if c.decryptionKey != nil {
		decErr = unmarshalThriftAllowPadding(&c.file.protocol, plain, &offsetIndex)
	} else {
		decErr = thrift.Unmarshal(&c.file.protocol, plain, &offsetIndex)
	}
	if decErr != nil {
		return nil, fmt.Errorf("decode offset index: rowGroup=%d columnChunk=%d/%d: %w", c.rowGroup.Ordinal, c.Column(), len(c.rowGroup.Columns), decErr)
	}
	index := &FileOffsetIndex{index: &offsetIndex}
	if !c.offsetIndex.CompareAndSwap(nil, index) {
		// another goroutine populated it since we last read the pointer
		return c.offsetIndex.Load(), nil
	}
	return index, nil
}

func (c *FileColumnChunk) readBloomFilter(reader io.ReaderAt) (*FileBloomFilter, error) {
	if err := c.errIfMissingKey(); err != nil {
		return nil, err
	}
	if filter := c.bloomFilter.Load(); filter != nil {
		return filter, nil
	}
	columnChunkMetaData := &c.file.metadata.RowGroups[c.rowGroup.Ordinal].Columns[c.Column()].MetaData
	offset := columnChunkMetaData.BloomFilterOffset
	length := c.file.size - offset
	if offset == 0 {
		return nil, nil
	}

	section := io.NewSectionReader(reader, offset, length)
	rbuf, rbufpool := getBufioReader(section, 1024)
	defer putBufioReader(rbuf, rbufpool)

	var header format.BloomFilterHeader
	var filter *FileBloomFilter

	if c.decryptionKey != nil {
		hdrPlain, err := decryptModuleReader(rbuf, c.decryptionKey, c.file.aadPrefix, c.file.fileUnique, bloomFilterHdrModule, &c.file.legacyModules, false, c.rowGroupOrdinal, c.columnOrdinal)
		if err != nil {
			return nil, fmt.Errorf("decrypting bloom filter header: %w", err)
		}
		compact := thrift.CompactProtocol{}
		if err := thrift.NewDecoder(compact.NewReaderFromBytes(hdrPlain)).Decode(&header); err != nil {
			return nil, fmt.Errorf("decoding encrypted bloom filter header: %w", err)
		}
		bitsPlain, err := decryptModuleReader(rbuf, c.decryptionKey, c.file.aadPrefix, c.file.fileUnique, bloomFilterBitsModule, &c.file.legacyModules, false, c.rowGroupOrdinal, c.columnOrdinal)
		if err != nil {
			return nil, fmt.Errorf("decrypting bloom filter bitset: %w", err)
		}
		filter = newBloomFilterFromBytes(&header, bitsPlain)
	} else {
		compact := thrift.CompactProtocol{}
		decoder := thrift.NewDecoder(compact.NewReader(rbuf))
		if err := decoder.Decode(&header); err != nil {
			return nil, fmt.Errorf("decoding bloom filter header: %w", err)
		}
		sectionPos, _ := section.Seek(0, io.SeekCurrent)
		dataOffset := offset + sectionPos - int64(rbuf.Buffered())
		var err error
		filter, err = newBloomFilter(reader, dataOffset, &header)
		if err != nil {
			return nil, fmt.Errorf("reading bloom filter: %w", err)
		}
	}

	if !c.bloomFilter.CompareAndSwap(nil, filter) {
		return c.bloomFilter.Load(), nil
	}
	return filter, nil
}

type FilePages struct {
	chunk    *FileColumnChunk
	rbuf     *bufio.Reader
	rbufpool *memory.Pool[bufio.Reader]
	section  io.SectionReader

	protocol thrift.CompactProtocol
	decoder  thrift.Decoder

	baseOffset int64
	dataOffset int64
	dictOffset int64
	index      int
	skip       int64
	dictionary Dictionary

	// track the last page to prevent re-reading the same page
	lastPageIndex int
	lastPage      Page
	serveLastPage bool

	bufferSize int

	// dec is non-nil only when the column is encrypted. Keeping it behind a
	// pointer avoids bloating FilePages for the common (non-encrypted) case.
	dec *filePagesDecryptionState
}

// filePagesDecryptionState holds per-column decryption context for FilePages.
type filePagesDecryptionState struct {
	key             []byte
	fileUnique      []byte
	aadPrefix       []byte
	legacy          *atomic.Bool
	ctrPages        bool
	rowGroupOrdinal int16
	columnOrdinal   int16
	dataPageOrd     int16
	dictPagePending bool // true when next page in the stream is the encrypted dict page
}

func (f *FilePages) init(c *FileColumnChunk, reader io.ReaderAt) {
	f.chunk = c
	f.baseOffset = c.chunk.MetaData.DataPageOffset
	f.dataOffset = f.baseOffset
	f.bufferSize = c.file.config.ReadBufferSize

	if c.chunk.MetaData.DictionaryPageOffset != 0 {
		f.baseOffset = c.chunk.MetaData.DictionaryPageOffset
		f.dictOffset = f.baseOffset
	}

	f.section = *io.NewSectionReader(reader, f.baseOffset, c.chunk.MetaData.TotalCompressedSize)
	f.rbuf, f.rbufpool = getBufioReader(&f.section, f.bufferSize)
	f.decoder.Reset(f.protocol.NewReader(f.rbuf))
	f.index = 0
	Release(f.lastPage)
	f.lastPage = nil
	f.lastPageIndex = -1
	f.serveLastPage = false

	// Populate decryption state from the column chunk (only when encrypted).
	if c.decryptionKey != nil {
		f.dec = &filePagesDecryptionState{
			key:             c.decryptionKey,
			fileUnique:      c.file.fileUnique,
			aadPrefix:       c.file.aadPrefix,
			legacy:          &c.file.legacyModules,
			ctrPages:        c.file.ctrPages,
			rowGroupOrdinal: c.rowGroupOrdinal,
			columnOrdinal:   c.columnOrdinal,
			dataPageOrd:     0,
			dictPagePending: f.dictOffset > 0,
		}
	} else {
		f.dec = nil
	}
}

// ReadDictionary returns the dictionary of the column chunk, or nil if the
// column chunk did not have one.
//
// The program is not required to call this method before calling ReadPage,
// the dictionary is read automatically when needed. It is exposed to allow
// programs to access the dictionary without reading the first page.
func (f *FilePages) ReadDictionary() (Dictionary, error) {
	if f.chunk != nil {
		if err := f.chunk.errIfMissingKey(); err != nil {
			return nil, err
		}
	}
	if f.dictionary == nil && f.dictOffset > 0 {
		if err := f.readDictionary(); err != nil {
			return nil, err
		}
	}
	return f.dictionary, nil
}

func (f *FilePages) isEmpty() bool {
	return f.chunk != nil &&
		f.chunk.rowGroup != nil && f.chunk.rowGroup.NumRows == 0 &&
		f.chunk.chunk != nil && f.chunk.chunk.MetaData.NumValues == 0
}

// ReadPages reads the next from from f.
func (f *FilePages) ReadPage() (Page, error) {
	if f.chunk == nil || f.isEmpty() {
		return nil, io.EOF
	}
	if err := f.chunk.errIfMissingKey(); err != nil {
		return nil, err
	}

	// seekToRowStart indicates whether we are in the process of seeking to the start
	// of requested row to read, as opposed to reading sequentially values and moving through pages
	seekToRowStart := f.skip > 0

	// serve the last page if SeekToRow targeted the same page as last returned
	if f.serveLastPage && f.lastPage != nil {
		f.serveLastPage = false
		f.index = f.lastPageIndex + 1

		numRows := f.lastPage.NumRows()
		if f.skip < numRows {
			tail := f.lastPage.Slice(f.skip, numRows)
			f.skip = 0
			return tail, nil
		}

		f.skip -= numRows // fall through to reading the next page
	}

	for {
		var (
			header *format.PageHeader
			data   *buffer[byte]
			err    error
		)

		if f.dec != nil {
			header, data, err = f.readEncryptedPage()
			if err != nil {
				return nil, err
			}
			// If we've already loaded the dictionary, skip a duplicate dict page.
			if header.Type == format.DictionaryPage && f.dictionary != nil {
				data.unref()
				continue
			}
		} else {
			// Instantiate a new format.PageHeader for each page.
			//
			// A previous implementation reused page headers to save allocations.
			// https://github.com/segmentio/parquet-go/pull/484
			// The optimization turned out to be less effective than expected,
			// because all the values referenced by pointers in the page header
			// are lost when the header is reset and put back in the pool.
			// https://github.com/parquet-go/parquet-go/pull/11
			//
			// Even after being reset, reusing page headers still produced instability
			// issues.
			// https://github.com/parquet-go/parquet-go/issues/70
			header = new(format.PageHeader)
			if err = f.decoder.Decode(header); err != nil {
				return nil, err
			}
			if err = validatePageHeader(header); err != nil {
				return nil, err
			}

			// if this is a dictionary page and we've already read and decoded the dictionary we can skip past it.
			// call f.rbuf.Discard to skip the page data and realign f.rbuf with the next page header
			if header.Type == format.DictionaryPage && f.dictionary != nil {
				// A short read here leaves the column chunk misaligned, so the
				// next header would be decoded from the wrong bytes.
				if _, err := f.rbuf.Discard(int(header.CompressedPageSize)); err != nil {
					return nil, fmt.Errorf("skipping dictionary page of column %q: %w", f.columnPath(), err)
				}
				continue
			}

			data, err = f.readPage(header, f.rbuf)
			if err != nil {
				return nil, err
			}
		}

		var page Page
		switch header.Type {
		case format.DataPageV2:
			page, err = f.readDataPageV2(header, data)
		case format.DataPage:
			page, err = f.readDataPageV1(header, data)
		case format.DictionaryPage:
			// Sometimes parquet files do not have the dictionary page offset
			// recorded in the column metadata. We account for this by lazily
			// reading dictionary pages when we encounter them.
			err = f.readDictionaryPage(header, data)
		default:
			err = fmt.Errorf("cannot read values of type %s from page", header.Type)
		}

		data.unref()

		if err != nil {
			return nil, fmt.Errorf("decoding page %d of column %q: %w", f.index, f.columnPath(), err)
		}

		if page == nil {
			continue
		}

		if f.lastPage != nil {
			Release(f.lastPage) // in case we cached a valid last page, release it now
		}

		// track last page
		f.lastPage = page
		Retain(page)
		f.lastPageIndex = f.index

		f.index++
		if f.skip == 0 {
			// f.skip==0 can be true:
			//  (1) while reading a row of a column which has multiple values (ie. X.list.element) and values continue
			//  across pages. In that case we just want to keep reading without skipping any values.
			//  (2) when seeking to a specific row and trying to reach the start offset of the first
			//  row in a new page.
			if !seekToRowStart || header.Type != format.DataPage {
				// keep reading values from beginning of new page
				return page, nil
			}
			// We need to seek to beginning of row.
			// V1 data pages do not necessarily start at a row boundary.
			if page.NumRows() == 0 {
				// if current page does not have any rows, continue until a page with at least 1 row is reached
				Release(page)
				continue
			}
			repLvls := page.RepetitionLevels()
			if len(repLvls) > 0 && repLvls[0] == 0 {
				// avoid page slice if page starts at a row boundary
				return page, nil
			}
			tail := page.Slice(0, page.NumRows())
			Release(page)
			return tail, nil
		}

		// TODO: what about pages that don't embed the number of rows?
		// (data page v1 with no offset index in the column chunk).
		numRows := page.NumRows()

		if numRows <= f.skip {
			Release(page)
		} else {
			tail := page.Slice(f.skip, numRows)
			Release(page)
			f.skip = 0
			return tail, nil
		}

		f.skip -= numRows
	}
}

func (f *FilePages) readDictionary() error {
	chunk := io.NewSectionReader(f.section.Outer())
	rbuf, pool := getBufioReader(chunk, f.bufferSize)
	defer putBufioReader(rbuf, pool)

	header := new(format.PageHeader)
	var page *buffer[byte]

	if f.dec != nil {
		d := f.dec
		hdrPlain, err := d.readModule(rbuf, dictPageHeaderModule, d.rowGroupOrdinal, d.columnOrdinal)
		if err != nil {
			return err
		}
		pr := f.protocol.NewReaderFromBytes(hdrPlain)
		if err := thrift.NewDecoder(pr).Decode(header); err != nil {
			return fmt.Errorf("parquet decryption: decoding dict page header: %w", err)
		}
		if err := validatePageHeader(header); err != nil {
			return err
		}
		bodyPlain, err := d.readModule(rbuf, dictPageBodyModule, d.rowGroupOrdinal, d.columnOrdinal)
		if err != nil {
			return err
		}
		page = buffers.get(len(bodyPlain))
		copy(page.data.Slice(), bodyPlain)
	} else {
		decoder := thrift.NewDecoder(f.protocol.NewReader(rbuf))
		if err := decoder.Decode(header); err != nil {
			return err
		}
		if err := validatePageHeader(header); err != nil {
			return err
		}
		page = buffers.get(int(header.CompressedPageSize))
		if _, err := io.ReadFull(rbuf, page.data.Slice()); err != nil {
			page.unref()
			return err
		}
	}
	defer page.unref()
	return f.readDictionaryPage(header, page)
}

func (f *FilePages) readDictionaryPage(header *format.PageHeader, page *buffer[byte]) error {
	if !header.DictionaryPageHeader.Valid {
		return ErrMissingPageHeader
	}
	d, err := f.chunk.column.decodeDictionary(DictionaryPageHeader{&header.DictionaryPageHeader.V}, page, header.UncompressedPageSize)
	if err != nil {
		return err
	}
	f.dictionary = d
	return nil
}

func (f *FilePages) readDataPageV1(header *format.PageHeader, page *buffer[byte]) (Page, error) {
	if !header.DataPageHeader.Valid {
		return nil, ErrMissingPageHeader
	}
	if isDictionaryFormat(header.DataPageHeader.V.Encoding) && f.dictionary == nil {
		if err := f.readDictionary(); err != nil {
			return nil, err
		}
	}
	return f.chunk.column.decodeDataPageV1(DataPageHeaderV1{&header.DataPageHeader.V}, page, f.dictionary, header.UncompressedPageSize)
}

func (f *FilePages) readDataPageV2(header *format.PageHeader, page *buffer[byte]) (Page, error) {
	if !header.DataPageHeaderV2.Valid {
		return nil, ErrMissingPageHeader
	}
	if isDictionaryFormat(header.DataPageHeaderV2.V.Encoding) && f.dictionary == nil {
		// If the program seeked to a row passed the first page, the dictionary
		// page may not have been seen, in which case we have to lazily load it
		// from the beginning of column chunk.
		if err := f.readDictionary(); err != nil {
			return nil, err
		}
	}
	return f.chunk.column.decodeDataPageV2(DataPageHeaderV2{&header.DataPageHeaderV2.V}, page, f.dictionary, header.UncompressedPageSize)
}

// validatePageHeader rejects page headers whose size or count fields decoded to
// negative values. Those values end up being trusted by the code later on
// which results in panics if they're off
func validatePageHeader(header *format.PageHeader) error {
	reject := func(field string, value int32) error {
		return fmt.Errorf("invalid page header: %s=%d: %w", field, value, ErrCorrupted)
	}
	if header.CompressedPageSize < 0 {
		return reject("compressed_page_size", header.CompressedPageSize)
	}
	if header.UncompressedPageSize < 0 {
		return reject("uncompressed_page_size", header.UncompressedPageSize)
	}
	if h := header.DataPageHeader; h.Valid && h.V.NumValues < 0 {
		return reject("data_page_header.num_values", h.V.NumValues)
	}
	if h := header.DictionaryPageHeader; h.Valid && h.V.NumValues < 0 {
		return reject("dictionary_page_header.num_values", h.V.NumValues)
	}
	if h := header.DataPageHeaderV2; h.Valid {
		switch {
		case h.V.NumValues < 0:
			return reject("data_page_header_v2.num_values", h.V.NumValues)
		case h.V.NumNulls < 0:
			return reject("data_page_header_v2.num_nulls", h.V.NumNulls)
		case h.V.NumRows < 0:
			return reject("data_page_header_v2.num_rows", h.V.NumRows)
		case h.V.RepetitionLevelsByteLength < 0:
			return reject("data_page_header_v2.repetition_levels_byte_length", h.V.RepetitionLevelsByteLength)
		case h.V.DefinitionLevelsByteLength < 0:
			return reject("data_page_header_v2.definition_levels_byte_length", h.V.DefinitionLevelsByteLength)
		}
	}
	return nil
}

func (f *FilePages) readPage(header *format.PageHeader, reader *bufio.Reader) (*buffer[byte], error) {
	page := buffers.get(int(header.CompressedPageSize))
	defer page.unref()

	if _, err := io.ReadFull(reader, page.data.Slice()); err != nil {
		return nil, err
	}

	if header.CRC != 0 {
		headerChecksum := uint32(header.CRC)
		bufferChecksum := crc32.ChecksumIEEE(page.data.Slice())

		if headerChecksum != bufferChecksum {
			// The parquet specs indicate that corruption errors could be
			// handled gracefully by skipping pages, tho this may not always
			// be practical. Depending on how the pages are consumed,
			// missing rows may cause unpredictable behaviors in algorithms.
			//
			// For now, we assume these errors to be fatal, but we may
			// revisit later and improve error handling to be more resilient
			// to data corruption.
			return nil, fmt.Errorf("crc32 checksum mismatch in page of column %q: want=0x%08X got=0x%08X: %w",
				f.columnPath(),
				headerChecksum,
				bufferChecksum,
				ErrCorrupted,
			)
		}
	}

	page.ref()
	return page, nil
}

// readEnvelope reads one length-prefixed encryption module from r.
func readEnvelope(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	moduleLen := int(binary.LittleEndian.Uint32(lenBuf[:]))
	envelope := make([]byte, 4+moduleLen)
	copy(envelope[:4], lenBuf[:])
	if _, err := io.ReadFull(r, envelope[4:]); err != nil {
		return nil, err
	}
	return envelope, nil
}

// readDecryptedEnvelopeFrom reads a Parquet module envelope from r, decrypts it, and
// returns the plaintext.  The caller supplies the key and pre-built AAD bytes.
func readDecryptedEnvelopeFrom(r io.Reader, key, aad []byte) ([]byte, error) {
	envelope, err := readEnvelope(r)
	if err != nil {
		return nil, err
	}
	return decryptModule(key, aad, envelope)
}

// decryptModuleReader reads one module and decrypts it, retrying with the
// legacy AAD when spec authentication fails. ctrPages rejects column page
// bodies, which AES_GCM_CTR_V1 encrypts with CTR rather than GCM.
func decryptModuleReader(r io.Reader, key, prefix, fileUnique []byte, module byte, legacy *atomic.Bool, ctrPages bool, ordinals ...int16) ([]byte, error) {
	if ctrPages && (module == dataPageBodyModule || module == dictPageBodyModule) {
		return nil, errCTRPage()
	}
	envelope, err := readEnvelope(r)
	if err != nil {
		return nil, err
	}
	return decryptWithLegacy(key, envelope, prefix, fileUnique, module, ordinals, legacy)
}

func (d *filePagesDecryptionState) readModule(r io.Reader, module byte, ordinals ...int16) ([]byte, error) {
	return decryptModuleReader(r, d.key, d.aadPrefix, d.fileUnique, module, d.legacy, d.ctrPages, ordinals...)
}

// readEncryptedPage reads one page (header + body) from the encrypted stream.
// It handles both dictionary pages and data pages; the caller must not call it
// when the stream is exhausted.
func (f *FilePages) readEncryptedPage() (*format.PageHeader, *buffer[byte], error) {
	d := f.dec
	isDictPage := d.dictPagePending

	var hdrModuleType, bodyModuleType byte
	var pageOrd int16
	if isDictPage {
		hdrModuleType = dictPageHeaderModule
		bodyModuleType = dictPageBodyModule
		pageOrd = 0
	} else {
		hdrModuleType = dataPageHeaderModule
		bodyModuleType = dataPageBodyModule
		pageOrd = d.dataPageOrd
	}

	var ordinals []int16
	if isDictPage {
		ordinals = []int16{d.rowGroupOrdinal, d.columnOrdinal}
	} else {
		ordinals = []int16{d.rowGroupOrdinal, d.columnOrdinal, pageOrd}
	}
	hdrPlain, err := d.readModule(f.rbuf, hdrModuleType, ordinals...)
	if err != nil {
		return nil, nil, err
	}

	header := new(format.PageHeader)
	pr := f.protocol.NewReaderFromBytes(hdrPlain)
	if err := thrift.NewDecoder(pr).Decode(header); err != nil {
		return nil, nil, fmt.Errorf("parquet decryption: decoding page header: %w", err)
	}
	if err := validatePageHeader(header); err != nil {
		return nil, nil, err
	}

	bodyPlain, err := d.readModule(f.rbuf, bodyModuleType, ordinals...)
	if err != nil {
		return nil, nil, err
	}

	page := buffers.get(len(bodyPlain))
	copy(page.data.Slice(), bodyPlain)

	if isDictPage {
		d.dictPagePending = false
	} else {
		d.dataPageOrd++
	}
	return header, page, nil
}

// SeekToRow seeks to the given row index in the column chunk.
func (f *FilePages) SeekToRow(rowIndex int64) error {
	if f.chunk == nil {
		return io.ErrClosedPipe
	}

	if index := f.chunk.offsetIndex.Load(); index == nil {
		if f.isEmpty() {
			// Zero-value dictionary-encoded chunks (e.g. from PyArrow or
			// arrow-go) may have dictionary_page_offset > 0 and
			// data_page_offset == 0, which would make dataOffset-baseOffset
			// negative. There are no rows or values to seek to.
			f.skip = rowIndex
			f.index = 0
			f.serveLastPage = false
			if f.dec != nil {
				f.dec.dataPageOrd = 0
				f.dec.dictPagePending = false
			}
			return nil
		}

		_, err := f.section.Seek(f.dataOffset-f.baseOffset, io.SeekStart)
		if err != nil {
			return err
		}

		f.skip = rowIndex
		f.index = 0
		if f.dec != nil {
			f.dec.dataPageOrd = 0
			f.dec.dictPagePending = false
		}
	} else {
		pages := index.index.PageLocations
		if len(pages) == 0 {
			// A 0-row row group may still carry an OffsetIndex with no
			// page locations. Seeking to row 0 is valid in this case;
			// the subsequent ReadPage will return io.EOF.
			if rowIndex == 0 {
				f.skip = 0
				return nil
			}
			return ErrSeekOutOfRange
		}
		target := sort.Search(len(pages), func(i int) bool {
			return pages[i].FirstRowIndex > rowIndex
		}) - 1
		if target < 0 {
			return ErrSeekOutOfRange
		}

		f.skip = rowIndex - pages[target].FirstRowIndex

		// The previous read left this page decoded. Replay that copy instead of
		// reading it again, once the input sits on the following page so the
		// next sequential read stays aligned with f.index.
		if f.lastPage != nil && target == f.lastPageIndex {
			if f.index != target+1 {
				// Drop a stale replay before moving the input. A failed
				// reposition must not serve the cached page from the wrong offset.
				f.serveLastPage = false
				if err := f.positionAfterCachedPage(pages, target); err != nil {
					return err
				}
			}
			f.serveLastPage = true
			return nil
		}
		f.serveLastPage = false

		// already positioned at the target page
		if f.index == target {
			return nil
		}

		f.index = target
		// Sync decryption ordinal so the next readEncryptedPage uses the correct AAD.
		if f.dec != nil {
			f.dec.dataPageOrd = int16(target)
			f.dec.dictPagePending = false
		}
		return f.seekToSectionOffset(pages[target].Offset - f.baseOffset)
	}

	f.rbuf.Reset(&f.section)
	return nil
}

// positionAfterCachedPage moves the input to the first byte after data page
// target and records that position in f.index. The decoded page is left
// cached so ReadPage can replay it.
func (f *FilePages) positionAfterCachedPage(pages []format.PageLocation, target int) error {
	next := pages[target].Offset + int64(pages[target].CompressedPageSize) - f.baseOffset
	if target+1 < len(pages) {
		next = pages[target+1].Offset - f.baseOffset
	}
	if err := f.seekToSectionOffset(next); err != nil {
		return err
	}
	f.index = target + 1
	if f.dec != nil {
		// readEncryptedPage increments the ordinal of the page it consumes.
		// The next page is target+1, which has not been read yet.
		f.dec.dataPageOrd = int16(target + 1)
		f.dec.dictPagePending = false
	}
	return nil
}

// seekToSectionOffset moves the column-chunk input to offset, relative to the
// start of the chunk section. Bytes already sitting in the read buffer are
// discarded when the target falls inside them.
func (f *FilePages) seekToSectionOffset(offset int64) error {
	pos, err := f.section.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	unread := int64(f.rbuf.Buffered())
	skipBytes := offset - (pos - unread)
	if skipBytes == 0 {
		return nil
	}
	if skipBytes > 0 && skipBytes <= unread {
		_, err = f.rbuf.Discard(int(skipBytes))
		return err
	}
	if _, err = f.section.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	f.rbuf.Reset(&f.section)
	return nil
}

// Close closes the page reader.
func (f *FilePages) Close() error {
	putBufioReader(f.rbuf, f.rbufpool)
	f.chunk = nil
	f.section = io.SectionReader{}
	f.rbuf = nil
	f.rbufpool = nil
	f.baseOffset = 0
	f.dataOffset = 0
	f.dictOffset = 0
	f.index = 0
	Release(f.lastPage)
	f.lastPage = nil
	f.lastPageIndex = -1
	f.serveLastPage = false
	f.skip = 0
	f.dictionary = nil
	return nil
}

func (f *FilePages) columnPath() columnPath {
	return columnPath(f.chunk.column.Path())
}

type putBufioReaderFunc func()

var (
	bufioReaderPoolLock sync.Mutex
	bufioReaderPool     = map[int]*memory.Pool[bufio.Reader]{}
)

func getBufioReader(r io.Reader, bufferSize int) (*bufio.Reader, *memory.Pool[bufio.Reader]) {
	pool := getBufioReaderPool(bufferSize)
	rbuf := pool.Get(
		func() *bufio.Reader { return bufio.NewReaderSize(r, bufferSize) },
		func(rbuf *bufio.Reader) { rbuf.Reset(r) },
	)
	return rbuf, pool
}

func putBufioReader(rbuf *bufio.Reader, pool *memory.Pool[bufio.Reader]) {
	if pool != nil {
		rbuf.Reset(nil)
		pool.Put(rbuf)
	}
}

func getBufioReaderPool(size int) *memory.Pool[bufio.Reader] {
	bufioReaderPoolLock.Lock()
	defer bufioReaderPoolLock.Unlock()

	if pool := bufioReaderPool[size]; pool != nil {
		return pool
	}

	pool := &memory.Pool[bufio.Reader]{}
	bufioReaderPool[size] = pool
	return pool
}

func (f *File) readAt(p []byte, off int64) (int, error) {
	return readAt(f.reader, p, off)
}

func readAt(r io.ReaderAt, p []byte, off int64) (n int, err error) {
	n, err = r.ReadAt(p, off)
	if n == len(p) {
		err = nil
		// p was fully read.There is no further need to check for errors. This
		// operation is a success in principle.
		return
	}
	return
}

type optimisticFileReaderAt struct {
	reader io.ReaderAt
	offset int64
	footer []byte
}

func (r *optimisticFileReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	length := r.offset + int64(len(r.footer))

	if off >= length {
		return 0, io.EOF
	}

	if off >= r.offset {
		n = copy(p, r.footer[off-r.offset:])
		p = p[n:]
		off += int64(n)
		if len(p) == 0 {
			return n, nil
		}
	}

	rn, err := r.reader.ReadAt(p, off)
	return n + rn, err
}
