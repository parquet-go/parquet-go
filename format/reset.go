package format

// The Reset methods in this file clear values in place while retaining
// allocated slice capacity, so that a value can be reused as the decode
// target of multiple thrift deserializations without allocating on each
// decode (see FooterDecoder).
//
// Because the thrift decoder only writes fields that are present in the
// input, Reset must recursively clear the elements of retained slices;
// otherwise fields decoded from a previous input would leak into the next
// decoded value. Union fields are the exception: the decoder itself clears
// unions absent from the input and zeroes reused members, so Reset retains
// their member allocations for the next decode to reuse.
//
// Byte slice and string fields decoded from footers alias the input buffer
// (they are replaced, never reused, by the decoder), so Reset sets them to
// nil or empty instead of truncating, releasing references to the previous
// input buffer.

// Reset clears m in place, retaining allocated capacity for reuse in
// subsequent thrift decodes.
//
// Note that after reuse, optional list fields that are absent from the
// decoded input are left empty but non-nil, whereas decoding into a fresh
// FileMetaData leaves them nil. Both states are semantically equivalent
// (zero-length), but programs distinguishing nil from empty slices, or
// re-encoding the value, can observe the difference.
func (m *FileMetaData) Reset() {
	m.Version = 0
	for i := range m.Schema {
		m.Schema[i].Reset()
	}
	m.Schema = m.Schema[:0]
	m.NumRows = 0
	for i := range m.RowGroups {
		m.RowGroups[i].Reset()
	}
	m.RowGroups = m.RowGroups[:0]
	clear(m.KeyValueMetadata)
	m.KeyValueMetadata = m.KeyValueMetadata[:0]
	m.CreatedBy = ""
	m.ColumnOrders = m.ColumnOrders[:0]
	// EncryptionAlgorithm is a union; its member allocation is retained
	// for reuse by the next decode (see SchemaElement.Reset). The members'
	// byte slices alias the decode input buffer, so their contents are
	// cleared like the CRS strings of SchemaElement.
	switch v := m.EncryptionAlgorithm.Value.(type) {
	case *AesGcmV1:
		*v = AesGcmV1{}
	case *AesGcmCtrV1:
		*v = AesGcmCtrV1{}
	}
	m.FooterSigningKeyMetadata = nil
}

// Reset clears s in place, retaining allocated capacity for reuse in
// subsequent thrift decodes.
func (s *SchemaElement) Reset() {
	s.Type.Reset()
	s.TypeLength.Reset()
	s.RepetitionType.Reset()
	s.Name = ""
	s.NumChildren.Reset()
	s.ConvertedType.Reset()
	s.Scale.Reset()
	s.Precision.Reset()
	s.FieldID = 0
	// The LogicalType union member is deliberately retained: when the
	// element is decoded again, the thrift decoder reuses the member
	// allocation if the input holds the same member type (zeroing its
	// contents first), and clears the union if the field is absent from
	// the input. Either way no stale state survives a decode.
	//
	// The Geometry and Geography members are the exception: their CRS
	// strings alias the decode input buffer, which the owner of this value
	// may overwrite before the next decode. Clear their contents so no
	// retained member holds a reference into a buffer with unrelated data.
	switch m := s.LogicalType.Value.(type) {
	case *GeometryType:
		*m = GeometryType{}
	case *GeographyType:
		*m = GeographyType{}
	}
}

// Reset clears c in place, retaining allocated capacity for reuse in
// subsequent thrift decodes.
func (c *ColumnChunk) Reset() {
	c.FilePath = ""
	c.FileOffset = 0
	c.MetaData.Reset()
	c.OffsetIndexOffset = 0
	c.OffsetIndexLength = 0
	c.ColumnIndexOffset = 0
	c.ColumnIndexLength = 0
	// CryptoMetadata is a union; its member allocation is retained for
	// reuse by the next decode (see SchemaElement.Reset). The column-key
	// member's path strings and key metadata alias the decode input
	// buffer, so its contents are cleared like the CRS strings of
	// SchemaElement.
	if v, ok := c.CryptoMetadata.Value.(*EncryptionWithColumnKey); ok {
		*v = EncryptionWithColumnKey{}
	}
	c.EncryptedColumnMetadata = nil
}

// Reset clears c in place, retaining allocated capacity for reuse in
// subsequent thrift decodes.
func (c *ColumnMetaData) Reset() {
	c.Type = 0
	c.Encoding = c.Encoding[:0]
	clear(c.PathInSchema)
	c.PathInSchema = c.PathInSchema[:0]
	c.Codec = 0
	c.NumValues = 0
	c.TotalUncompressedSize = 0
	c.TotalCompressedSize = 0
	clear(c.KeyValueMetadata)
	c.KeyValueMetadata = c.KeyValueMetadata[:0]
	c.DataPageOffset = 0
	c.IndexPageOffset = 0
	c.DictionaryPageOffset = 0
	c.Statistics.Reset()
	clear(c.EncodingStats)
	c.EncodingStats = c.EncodingStats[:0]
	c.BloomFilterOffset = 0
	c.BloomFilterLength = 0
	c.SizeStatistics.Reset()
	c.GeospatialStatistics.Reset()
}

// Reset clears s in place. The byte slice fields are set to nil rather than
// truncated because decoded values alias the input buffer and are always
// replaced, never appended to.
func (s *Statistics) Reset() {
	s.Max = nil
	s.Min = nil
	s.NullCount = 0
	s.DistinctCount = 0
	s.MaxValue = nil
	s.MinValue = nil
}

// Reset clears s in place, retaining allocated capacity for reuse in
// subsequent thrift decodes.
func (s *SizeStatistics) Reset() {
	s.UnencodedByteArrayDataBytes = 0
	s.RepetitionLevelHistogram = s.RepetitionLevelHistogram[:0]
	s.DefinitionLevelHistogram = s.DefinitionLevelHistogram[:0]
}

// Reset clears g in place, retaining allocated capacity for reuse in
// subsequent thrift decodes.
func (g *GeospatialStatistics) Reset() {
	g.BBox = BoundingBox{}
	g.GeoSpatialTypes = g.GeoSpatialTypes[:0]
}
