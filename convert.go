package parquet

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go/internal/memory"
	"golang.org/x/sys/cpu"

	"github.com/parquet-go/bitpack/unsafecast"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

// ConvertError is an error type returned by calls to Convert when the conversion
// of parquet schemas is impossible or the input row for the conversion is
// malformed.
type ConvertError struct {
	Path []string
	From Node
	To   Node
}

// Error satisfies the error interface.
func (e *ConvertError) Error() string {
	sourceType := e.From.Type()
	targetType := e.To.Type()

	sourceRepetition := fieldRepetitionTypeOf(e.From)
	targetRepetition := fieldRepetitionTypeOf(e.To)

	return fmt.Sprintf("cannot convert parquet column %q from %s %s to %s %s",
		columnPath(e.Path),
		sourceRepetition,
		sourceType,
		targetRepetition,
		targetType,
	)
}

// Conversion is an interface implemented by types that provide conversion of
// parquet rows from one schema to another.
//
// Conversion instances must be safe to use concurrently from multiple goroutines.
type Conversion interface {
	// Applies the conversion logic on the src row, returning the result
	// appended to dst.
	Convert(rows []Row) (int, error)
	// Converts the given column index in the target schema to the original
	// column index in the source schema of the conversion.
	Column(int) int
	// Returns the target schema of the conversion.
	Schema() *Schema
}

type conversion struct {
	columns []conversionColumn
	// Shredded variant columns of the source being reconstructed to
	// unshredded variant columns of the target (see convert_variant.go).
	variants []variantConversion
	schema   *Schema
	buffers  memory.Pool[conversionBuffer]
	// This field is used to size the column buffers held in the memory.Pool since
	// they are intended to store the source rows being converted from.
	numberOfSourceColumns int
}

type conversionBuffer struct {
	columns [][]Value
	// Per-variant-conversion scratch space, reused across rows (see
	// convert_variant.go).
	variants []variantScratch
	// scratch holds one missing column's projected values for the current row.
	scratch []Value
}

type conversionColumn struct {
	sourceIndex   int
	convertValues conversionFunc
	targetKind    Kind // Target column kind for creating proper null values
	isOptional    bool // Whether the target column is optional (for null handling)
	// Index in conversion.variants of the variant conversion producing this
	// column, or -1; variantOutput selects its metadata (0) or value (1)
	// output column.
	variantIndex  int
	variantOutput int
	// missing projects a target column that is absent from the source.
	// Nil when the column exists in the source.
	missing *missingProjection
}

type conversionFunc func([]Value) error

func convertToSelf(column []Value) error { return nil }

func convertToType(targetType, sourceType Type) conversionFunc {
	return func(column []Value) error {
		for i, v := range column {
			v, err := targetType.ConvertValue(v, sourceType)
			if err != nil {
				return err
			}
			column[i].ptr = v.ptr
			column[i].u64 = v.u64
			column[i].kind = v.kind
		}
		return nil
	}
}

func convertToLevels(repetitionLevels, definitionLevels []byte) conversionFunc {
	return func(column []Value) error {
		for i := range column {
			r := column[i].repetitionLevel
			d := column[i].definitionLevel
			// Source levels that exceed the lookup-table size can be produced
			// when maskMissingRowGroupColumns or sibling-fallback synthesizes
			// values for schemas where source and target disagree on optionality
			// or nesting depth. Without this guard, the convertedRows reader
			// panics with "index out of range" inside this hot loop. Treat the
			// value as null so downstream Reconstruct skips it instead of
			// dereferencing a placeholder's nil byte-array pointer.
			if int(r) >= len(repetitionLevels) || int(d) >= len(definitionLevels) {
				column[i] = Value{}
				continue
			}
			column[i].repetitionLevel = repetitionLevels[r]
			column[i].definitionLevel = definitionLevels[d]
		}
		return nil
	}
}

func multiConversionFunc(conversions []conversionFunc) conversionFunc {
	switch len(conversions) {
	case 0:
		return convertToSelf
	case 1:
		return conversions[0]
	default:
		return func(column []Value) error {
			for _, conv := range conversions {
				if err := conv(column); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func (c *conversion) getBuffer() *conversionBuffer {
	return c.buffers.Get(
		func() *conversionBuffer {
			b := &conversionBuffer{
				columns:  make([][]Value, c.numberOfSourceColumns),
				variants: make([]variantScratch, len(c.variants)),
			}
			values := make([]Value, c.numberOfSourceColumns)
			for i := range b.columns {
				b.columns[i] = values[i : i : i+1]
			}
			return b
		},
		func(b *conversionBuffer) {},
	)
}

func (c *conversion) putBuffer(b *conversionBuffer) {
	c.buffers.Put(b)
}

// Convert here satisfies the Conversion interface, and does the actual work
// to convert between the source and target Rows.
func (c *conversion) Convert(rows []Row) (int, error) {
	source := c.getBuffer()
	defer c.putBuffer(source)

	for n, row := range rows {
		for i, values := range source.columns {
			source.columns[i] = values[:0]
		}
		row.Range(func(columnIndex int, columnValues []Value) bool {
			source.columns[columnIndex] = append(source.columns[columnIndex], columnValues...)
			return true
		})
		row = row[:0]

		// Reconstruct shredded variant columns once per row; the column
		// loop below picks up the metadata and value outputs.
		for k := range c.variants {
			if err := c.variants[k].convert(source.columns, &source.variants[k]); err != nil {
				return n, err
			}
		}

		for columnIndex, conv := range c.columns {
			if conv.variantIndex >= 0 {
				// Values already carry final levels and column indexes.
				scratch := &source.variants[conv.variantIndex]
				if conv.variantOutput == 0 {
					row = append(row, scratch.metas...)
				} else {
					row = append(row, scratch.values...)
				}
				continue
			}

			columnOffset := len(row)

			if conv.missing != nil {
				if !conv.missing.useSource {
					row = append(row, conv.missing.value(0, 0))
					row[len(row)-1].columnIndex = ^uint16(columnIndex)
					continue
				}
				var src []Value
				if conv.sourceIndex >= 0 && conv.sourceIndex < len(source.columns) {
					src = source.columns[conv.sourceIndex]
				}
				source.scratch = conv.missing.apply(source.scratch, src)
				row = append(row, source.scratch...)
				for i := columnOffset; i < len(row); i++ {
					row[i].columnIndex = ^uint16(columnIndex)
				}
				continue
			}

			// Get source values if available
			var sourceValues []Value
			if conv.sourceIndex >= 0 {
				sourceValues = source.columns[conv.sourceIndex]
			}

			if len(sourceValues) > 0 {
				row = append(row, sourceValues...)
			} else {
				// No source values - add placeholder to maintain sequential column order.
				// This happens when the column doesn't exist in source or has no values for this row.
				if conv.isOptional {
					row = append(row, NullValue())
				} else {
					row = append(row, ZeroValue(conv.targetKind))
				}
			}
			columnValues := row[columnOffset:]

			if err := conv.convertValues(columnValues); err != nil {
				return n, err
			}

			// Since the column index may have changed between the source and
			// taget columns we ensure that the right value is always written
			// to the output row.
			for i := range columnValues {
				// Fix: If we have a zero Value{}, convert it to a properly typed value
				// For optional fields, keep as null (kind = 0)
				// For required fields, convert to typed zero value
				if columnValues[i].IsNull() && !conv.isOptional {
					columnValues[i] = ZeroValue(conv.targetKind)
				}

				columnValues[i].columnIndex = ^uint16(columnIndex)
			}
		}

		rows[n] = row
	}

	return len(rows), nil
}

func (c *conversion) Column(i int) int {
	return c.columns[i].sourceIndex
}

func (c *conversion) Schema() *Schema {
	return c.schema
}

type identity struct{ schema *Schema }

func (id identity) Convert(rows []Row) (int, error) { return len(rows), nil }
func (id identity) Column(i int) int                { return i }
func (id identity) Schema() *Schema                 { return id.schema }

// Convert constructs a conversion function from one parquet schema to another.
//
// The function supports converting between schemas where the source or target
// have extra columns; if there are more columns in the source, they will be
// stripped out of the rows. Extra columns in the target schema will be set to
// null or zero values.
//
// Variant columns that the target declares unshredded but the source stores
// shredded are reconstructed with the construct_variant algorithm of the
// Variant Shredding specification instead of being mapped column-by-column
// (see convert_variant.go).
//
// The returned function is intended to be used to append the converted source
// row to the destination buffer.
func Convert(to, from Node) (conv Conversion, err error) {
	schema, _ := to.(*Schema)
	if schema == nil {
		schema = NewSchema("", to)
	}

	if EqualNodes(to, from) {
		return identity{schema}, nil
	}

	// Variant columns that the target declares unshredded but the source
	// stores shredded are reconstructed, not mapped column-by-column (see
	// convert_variant.go).
	variants := findVariantConversions(to, from)
	variantColumns := make(map[int]conversionColumn, 2*len(variants))
	for k := range variants {
		vc := &variants[k]
		metaSource, valueSource := 0, 0
		if vc.group != nil {
			metaSource = vc.group.metadataCol
			// Shredded groups without a value column are legal; any column
			// of the group keeps the source chunk mapping meaningful.
			valueSource = max(vc.group.valueCol, 0)
		}
		variantColumns[vc.targetMetaCol] = conversionColumn{
			sourceIndex:   vc.sourceStart + metaSource,
			targetKind:    ByteArray,
			variantIndex:  k,
			variantOutput: 0,
		}
		variantColumns[vc.targetValueCol] = conversionColumn{
			sourceIndex:   vc.sourceStart + valueSource,
			targetKind:    ByteArray,
			variantIndex:  k,
			variantOutput: 1,
		}
	}

	targetMapping, targetColumns := columnMappingOf(to)
	sourceMapping, sourceColumns := columnMappingOf(from)
	columns := make([]conversionColumn, len(targetColumns))

	targetFieldCache := make(map[Node]map[string]Field)
	sourceFieldCache := make(map[Node]map[string]Field)
	// Legacy LIST shapes (PyArrow's "item", 2-level lists, ...) do not share
	// the "list"/"element" paths of a Go schema. Resolve them before the
	// missing-column path, which would otherwise copy the sibling levels and
	// replace the values with zeros.
	var normalized normalizedLists

	// Helper function to get or build field map for a node
	getFieldMap := func(cache map[Node]map[string]Field, node Node) map[string]Field {
		if fieldMap, ok := cache[node]; ok {
			return fieldMap
		}
		fields := node.Fields()
		fieldMap := make(map[string]Field, len(fields))
		for _, f := range fields {
			fieldMap[f.Name()] = f
		}
		cache[node] = fieldMap
		return fieldMap
	}

	for i, path := range targetColumns {
		if cc, ok := variantColumns[i]; ok {
			columns[i] = cc
			continue
		}

		targetColumn := targetMapping.lookup(path)
		sourceColumn := sourceMapping.lookup(path)
		sourceRoot := from
		if sourceColumn.node == nil {
			if col, root, found := normalized.lookup(from, path); found {
				sourceColumn = col
				sourceRoot = root
			}
		}

		// Track whether this column exists in source BEFORE any closestColumn lookup
		isMissingColumn := sourceColumn.node == nil

		conversions := []conversionFunc{}
		if sourceColumn.node != nil {
			targetType := targetColumn.node.Type()
			sourceType := sourceColumn.node.Type()
			if !EqualTypes(targetType, sourceType) {
				conversions = append(conversions, convertToType(targetType, sourceType))
			}

			repetitionLevels := make([]byte, len(path)+1)
			definitionLevels := make([]byte, len(path)+1)
			targetRepetitionLevel := byte(0)
			targetDefinitionLevel := byte(0)
			sourceRepetitionLevel := byte(0)
			sourceDefinitionLevel := byte(0)
			targetNode := to
			sourceNode := sourceRoot

			levelsOK := true
			for j := range path {
				targetField := getFieldMap(targetFieldCache, targetNode)[path[j]]
				sourceField := getFieldMap(sourceFieldCache, sourceNode)[path[j]]
				if targetField == nil || sourceField == nil {
					levelsOK = false
					break
				}
				targetNode = targetField
				sourceNode = sourceField

				targetRepetitionLevel, targetDefinitionLevel = applyFieldRepetitionType(
					fieldRepetitionTypeOf(targetNode),
					targetRepetitionLevel,
					targetDefinitionLevel,
				)
				sourceRepetitionLevel, sourceDefinitionLevel = applyFieldRepetitionType(
					fieldRepetitionTypeOf(sourceNode),
					sourceRepetitionLevel,
					sourceDefinitionLevel,
				)

				repetitionLevels[sourceRepetitionLevel] = targetRepetitionLevel
				definitionLevels[sourceDefinitionLevel] = targetDefinitionLevel
			}

			if !levelsOK {
				// The normalized path did not line up with the target. Treat
				// the column as missing rather than indexing a short level map.
				sourceColumn = leafColumn{columnIndex: math.MaxUint16}
				isMissingColumn = true
				conversions = nil
			} else {
				repetitionLevels = repetitionLevels[:sourceRepetitionLevel+1]
				definitionLevels = definitionLevels[:sourceDefinitionLevel+1]

				if !isDirectLevelMapping(repetitionLevels) || !isDirectLevelMapping(definitionLevels) {
					conversions = append(conversions, convertToLevels(repetitionLevels, definitionLevels))
				}
			}
		}
		var missing *missingProjection
		sourceIndex := -1
		if isMissingColumn {
			missing, sourceIndex = missingProjectionOf(to, from, path, targetColumn, sourceMapping)
		} else {
			sourceIndex = int(sourceColumn.columnIndex)
		}

		// Store target column type for creating proper null values
		targetType := targetColumn.node.Type()

		// Determine if target column is optional
		isOptional := targetColumn.maxDefinitionLevel > 0

		columns[i] = conversionColumn{
			sourceIndex:   sourceIndex,
			convertValues: multiConversionFunc(conversions),
			targetKind:    targetType.Kind(), // Store target kind for null value creation
			isOptional:    isOptional,
			variantIndex:  -1,
			missing:       missing,
		}
	}

	c := &conversion{
		columns:               columns,
		variants:              variants,
		schema:                schema,
		numberOfSourceColumns: len(sourceColumns),
	}
	return c, nil
}

func isDirectLevelMapping(levels []byte) bool {
	for i, level := range levels {
		if level != byte(i) {
			return false
		}
	}
	return true
}

// missingProjection synthesizes values for a target column that the source does
// not have. Each instance of the deepest source group enclosing the column
// produces one value. Definition levels are remapped onto the target schema
// and clamped to that group's definition level, so the added field is null or
// zero where the group is present and follows ancestor nulls where it is not.
type missingProjection struct {
	groupSourceRep byte
	sourceMaxRep   byte
	targetRep      byte
	targetDef      byte
	repLevels      []byte
	defLevels      []byte
	// useSource is set when the enclosing group's presence varies between rows
	// and a leaf under that group records it. Otherwise every row is one value
	// at repetition level 0.
	useSource bool
	// null selects a null value. Required leaves are typed zeros.
	null bool
	kind Kind
}

func missingProjectionOf(target, source Node, path columnPath, targetColumn leafColumn, sourceMapping columnMappingGroup) (*missingProjection, int) {
	repLevels := []byte{0}
	defLevels := []byte{0}
	var sourceRep, sourceDef, targetRep, targetDef byte
	targetNode := target
	sourceNode := source
	var matched columnPath
	if len(path) > 0 {
		for _, name := range path[:len(path)-1] {
			targetField := fieldByName(targetNode, name)
			sourceField := fieldByName(sourceNode, name)
			if targetField == nil || sourceField == nil || sourceField.Leaf() {
				break
			}
			targetNode = targetField
			sourceNode = sourceField
			targetRep, targetDef = applyFieldRepetitionType(fieldRepetitionTypeOf(targetNode), targetRep, targetDef)
			sourceRep, sourceDef = applyFieldRepetitionType(fieldRepetitionTypeOf(sourceNode), sourceRep, sourceDef)
			repLevels = setLevelMapping(repLevels, sourceRep, targetRep)
			defLevels = setLevelMapping(defLevels, sourceDef, targetDef)
			matched = append(matched, name)
		}
	}

	sourceIndex := -1
	// Groups that share a path but no field are different schemas that happen
	// to use the same names. Their levels do not describe the added field.
	group := sourceMapping.descend(matched)
	if len(matched) > 0 && (group == nil || (!group.hasDirectLeaf() && !nodesShareField(sourceNode, targetNode))) {
		repLevels = []byte{0}
		defLevels = []byte{0}
		sourceRep, sourceDef, targetRep, targetDef = 0, 0, 0, 0
		group = nil
	}

	proj := &missingProjection{
		groupSourceRep: sourceRep,
		targetRep:      targetRep,
		targetDef:      targetDef,
		repLevels:      repLevels,
		defLevels:      defLevels,
		null:           targetColumn.node.Optional(),
		kind:           targetColumn.node.Type().Kind(),
	}
	// A required path has fixed levels, so the remapped zero level is enough
	// and no leaf has to be read. Optional or repeated ancestors vary per row.
	if group != nil && (sourceRep > 0 || sourceDef > 0) {
		leaf := group.lookupAnyLeaf()
		if leaf.node != nil {
			proj.useSource = true
			proj.sourceMaxRep = leaf.maxRepetitionLevel
			sourceIndex = int(leaf.columnIndex)
		}
	}
	return proj, sourceIndex
}

// nodesShareField reports whether two groups have a child with the same name.
func nodesShareField(a, b Node) bool {
	if a == nil || b == nil || a.Leaf() || b.Leaf() {
		return false
	}
	names := make(map[string]struct{}, len(a.Fields()))
	for _, field := range a.Fields() {
		names[field.Name()] = struct{}{}
	}
	for _, field := range b.Fields() {
		if _, ok := names[field.Name()]; ok {
			return true
		}
	}
	return false
}

func (p *missingProjection) apply(dst, src []Value) []Value {
	dst = dst[:0]
	if p.useSource {
		for i := range src {
			if src[i].repetitionLevel > p.groupSourceRep {
				continue
			}
			dst = append(dst, p.value(src[i].repetitionLevel, src[i].definitionLevel))
		}
		if len(dst) > 0 {
			return dst
		}
	}
	return append(dst, p.value(0, 0))
}

func (p *missingProjection) value(rep, def byte) Value {
	var v Value
	if !p.null {
		v = ZeroValue(p.kind)
	}
	v.repetitionLevel = p.mapRep(rep)
	v.definitionLevel = p.mapDef(def)
	return v
}

func (p *missingProjection) mapRep(level byte) byte {
	if int(level) >= len(p.repLevels) {
		return p.targetRep
	}
	return p.repLevels[level]
}

func (p *missingProjection) mapDef(level byte) byte {
	if int(level) >= len(p.defLevels) {
		return p.targetDef
	}
	mapped := p.defLevels[level]
	if mapped > p.targetDef {
		return p.targetDef
	}
	return mapped
}

// missingColumnNeedsSibling reports whether a target column that is absent from
// the source still needs another column's repetition and definition levels.
//
// Repetition levels are zero when no node on the path is repeated. Definition
// levels are fixed by the schema when every ancestor that exists in the source
// is required: a missing optional ancestor is null on every row, and a required
// ancestor is present on every row. An optional or repeated ancestor that is
// present in the source is null on some rows and not on others, which is only
// recorded in a sibling column.
func missingColumnNeedsSibling(source Node, path columnPath, maxRepetitionLevel byte) bool {
	if maxRepetitionLevel > 0 {
		return true
	}
	if len(path) == 0 {
		return false
	}
	sourceNode := source
	for _, name := range path[:len(path)-1] {
		if sourceNode == nil {
			return false
		}
		field := fieldByName(sourceNode, name)
		if field == nil {
			return false
		}
		if field.Optional() || field.Repeated() {
			return true
		}
		sourceNode = field
	}
	return false
}

// leafLevelsAreFixedZero reports whether every value of leaf has repetition and
// definition levels of zero. Copying those levels onto a missing column is the
// same as synthesizing a zero level, so the leaf does not have to be read.
func leafLevelsAreFixedZero(leaf leafColumn) bool {
	return leaf.node != nil && leaf.maxRepetitionLevel == 0 && leaf.maxDefinitionLevel == 0
}

func sourceHasLeaf(leaves []leafColumn, path columnPath) bool {
	for _, leaf := range leaves {
		if leaf.node != nil && leaf.path.equal(path) {
			return true
		}
	}
	return false
}

// findAdjacentColumnChunk finds a sibling column at the same repetition depth
// Returns nil if no suitable adjacent column exists
func findAdjacentColumnChunk(schema *Schema, targetColumnIndex uint16, columns []ColumnChunk, sourceMapping columnMapping) ColumnChunk {
	var targetLeaf leafColumn
	targetFound := false

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		if leaf.columnIndex == targetColumnIndex {
			targetLeaf = leaf
			targetFound = true
		}
	})

	if !targetFound {
		return nil
	}

	// Find a sibling: same parent path and same max repetition level
	targetParentPath := targetLeaf.path
	if len(targetParentPath) > 0 {
		targetParentPath = targetParentPath[:len(targetParentPath)-1]
	}

	var adjacentChunk ColumnChunk
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		if leaf.columnIndex == targetColumnIndex {
			return // Skip self
		}

		// Check if this is a sibling
		if len(leaf.path) > 0 {
			leafParentPath := leaf.path[:len(leaf.path)-1]
			if targetParentPath.equal(leafParentPath) &&
				leaf.maxRepetitionLevel == targetLeaf.maxRepetitionLevel {
				// Check if this column exists in the converted row group
				if int(leaf.columnIndex) < len(columns) && columns[leaf.columnIndex] != nil {
					// Make sure it's not another missing column
					if _, ok := columns[leaf.columnIndex].(*missingColumnChunk); !ok {
						adjacentChunk = columns[leaf.columnIndex]
						return // Found a suitable adjacent column
					}
				}
			}
		}
	})

	return adjacentChunk
}

// ConvertRowGroup constructs a wrapper of the given row group which applies
// the given schema conversion to its rows.
func ConvertRowGroup(rowGroup RowGroup, conv Conversion) RowGroup {
	if EqualNodes(rowGroup.Schema(), conv.Schema()) {
		return rowGroup
	}
	schema := conv.Schema()
	numRows := rowGroup.NumRows()
	rowGroupColumns := rowGroup.ColumnChunks()
	sourceSchema := rowGroup.Schema()

	// Build a mapping to detect missing columns
	sourceMapping, _ := columnMappingOf(sourceSchema)

	columns := make([]ColumnChunk, numLeafColumnsOf(schema))

	// First pass: create all non-missing columns
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		i := leaf.columnIndex
		j := conv.Column(int(leaf.columnIndex))

		// Check if this column actually exists in the source schema
		sourceColumn := sourceMapping.lookup(leaf.path)
		isMissing := sourceColumn.node == nil

		if !isMissing {
			if i == uint16(j) {
				columns[i] = rowGroupColumns[j]
			} else {
				columns[i] = &convertedColumnChunk{
					chunk:             rowGroupColumns[j],
					targetColumnIndex: ^i,
				}
			}
		}
	})

	// Second pass: create missing columns with references to adjacent columns
	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		i := leaf.columnIndex

		// Check if this column actually exists in the source schema
		sourceColumn := sourceMapping.lookup(leaf.path)
		isMissing := sourceColumn.node == nil

		if isMissing {
			var adjacentChunk ColumnChunk
			mirrorSiblingLevels := false
			var project *missingProjection
			numValues := numRows
			if c, ok := conv.(*conversion); ok && int(i) < len(c.columns) {
				project = c.columns[i].missing
			}
			if project != nil && project.useSource {
				src := conv.Column(int(i))
				if src >= 0 && src < len(rowGroupColumns) {
					adjacentChunk = rowGroupColumns[src]
					// One output value per row can be sliced by seeking the leaf.
					mirrorSiblingLevels = project.groupSourceRep == 0
					if project.groupSourceRep > 0 {
						if n := adjacentChunk.NumValues(); project.sourceMaxRep <= project.groupSourceRep || n > numValues {
							numValues = n
						}
					}
				}
			} else if project == nil && missingColumnNeedsSibling(sourceSchema, leaf.path, leaf.maxRepetitionLevel) {
				adjacentChunk = findAdjacentColumnChunk(schema, i, columns, sourceMapping)
				mirrorSiblingLevels = leaf.maxRepetitionLevel == 0 && adjacentChunk != nil
			}

			columns[i] = &missingColumnChunk{
				typ:                 leaf.node.Type(),
				column:              i,
				numRows:             numRows,
				numValues:           numValues,
				numNulls:            numRows, // Depends on required vs optional
				maxRepetitionLevel:  leaf.maxRepetitionLevel,
				maxDefinitionLevel:  leaf.maxDefinitionLevel,
				adjacentChunk:       adjacentChunk,
				mirrorSiblingLevels: mirrorSiblingLevels,
				leafOptional:        leaf.node.Optional(),
				project:             project,
			}
		}
	})

	// Sorting columns must exist on the conversion schema in order to be
	// advertised on the converted row group otherwise the resulting rows
	// would not be in the right order.
	sorting := []SortingColumn{}
	for _, col := range rowGroup.SortingColumns() {
		if !hasColumnPath(schema, col.Path()) {
			break
		}
		sorting = append(sorting, col)
	}

	return &convertedRowGroup{
		// The pair of rowGroup+conv is retained to construct a converted row
		// reader by wrapping the underlying row reader of the row group because
		// it allows proper reconstruction of the repetition and definition
		// levels.
		//
		// Columns of the source row group which do not exist in the target are
		// masked to prevent loading unneeded pages when reading rows from the
		// converted row group.
		rowGroup: maskMissingRowGroupColumns(rowGroup, conv),
		columns:  columns,
		sorting:  sorting,
		conv:     conv,
	}
}

func maskMissingRowGroupColumns(r RowGroup, conv Conversion) RowGroup {
	rowGroupColumns := r.ColumnChunks()
	columns := make([]ColumnChunk, len(rowGroupColumns))
	missing := make([]missingColumnChunk, len(columns))
	numRows := r.NumRows()

	// Compute max levels for each column in the source schema
	sourceSchema := r.Schema()
	sourceLeaves := make([]leafColumn, len(rowGroupColumns))
	forEachLeafColumnOf(sourceSchema, func(leaf leafColumn) {
		i := leaf.columnIndex
		if int(i) >= len(missing) {
			return
		}
		sourceLeaves[i] = leaf
		missing[i] = missingColumnChunk{
			typ:                rowGroupColumns[i].Type(),
			column:             i,
			numRows:            numRows,
			numValues:          numRows,
			numNulls:           numRows,
			maxRepetitionLevel: leaf.maxRepetitionLevel,
			maxDefinitionLevel: leaf.maxDefinitionLevel,
		}
	})

	for i := range columns {
		columns[i] = &missing[i]
	}

	// Source columns are read only when a target column needs their values.
	// A missing flat column whose sibling levels are always zero does not:
	// Convert replaces the values, and the levels match a synthesized zero.
	// The sibling stays in place when it is the only column that can produce
	// the row count.
	targetSchema := conv.Schema()
	rowCountFallback := -1
	forEachLeafColumnOf(targetSchema, func(leaf leafColumn) {
		j := conv.Column(int(leaf.columnIndex))
		if j < 0 || j >= len(columns) {
			return
		}
		if sourceHasLeaf(sourceLeaves, leaf.path) {
			columns[j] = rowGroupColumns[j]
			return
		}
		if missingColumnNeedsSibling(sourceSchema, leaf.path, leaf.maxRepetitionLevel) ||
			j >= len(sourceLeaves) || !leafLevelsAreFixedZero(sourceLeaves[j]) {
			columns[j] = rowGroupColumns[j]
			return
		}
		if rowCountFallback < 0 {
			rowCountFallback = j
		}
	})
	if rowCountFallback >= 0 {
		onlyTemplates := true
		for _, col := range columns {
			if _, synth := col.(*missingColumnChunk); !synth {
				onlyTemplates = false
				break
			}
		}
		if onlyTemplates {
			columns[rowCountFallback] = rowGroupColumns[rowCountFallback]
		}
	}

	// Shredded variant reconstruction reads every leaf column of the
	// variant group, not just the ones mapped to target columns.
	if c, ok := conv.(*conversion); ok {
		for k := range c.variants {
			vc := &c.variants[k]
			for j := vc.sourceStart; j < vc.sourceStart+vc.numCols && j < len(columns); j++ {
				columns[j] = rowGroupColumns[j]
			}
		}
	}

	return &rowGroup{
		schema:  r.Schema(),
		numRows: numRows,
		columns: columns,
	}
}

type missingColumnChunk struct {
	typ                 Type
	column              uint16
	numRows             int64
	numValues           int64
	numNulls            int64
	maxRepetitionLevel  byte        // Maximum repetition level for this column
	maxDefinitionLevel  byte        // Maximum definition level for this column
	adjacentChunk       ColumnChunk // Adjacent column chunk to mirror levels from (nil if none)
	mirrorSiblingLevels bool        // Copy sibling levels the way Convert does (flat columns)
	leafOptional        bool        // The missing leaf itself is optional
	project             *missingProjection
}

func (c *missingColumnChunk) Type() Type  { return c.typ }
func (c *missingColumnChunk) Column() int { return int(c.column) }
func (c *missingColumnChunk) Pages() Pages {
	var adjacentPages Pages
	if c.adjacentChunk != nil {
		adjacentPages = c.adjacentChunk.Pages()
	}
	return onePage(missingPage{
		missingColumnChunk: c,
		adjacentPages:      adjacentPages,
	})
}
func (c *missingColumnChunk) ColumnIndex() (ColumnIndex, error) { return missingColumnIndex{c}, nil }
func (c *missingColumnChunk) OffsetIndex() (OffsetIndex, error) { return missingOffsetIndex{}, nil }
func (c *missingColumnChunk) BloomFilter() BloomFilter          { return missingBloomFilter{} }
func (c *missingColumnChunk) NumValues() int64                  { return c.numValues }

type missingColumnIndex struct{ *missingColumnChunk }

func (i missingColumnIndex) NumPages() int       { return 1 }
func (i missingColumnIndex) NullCount(int) int64 { return i.numNulls }
func (i missingColumnIndex) NullPage(int) bool   { return true }
func (i missingColumnIndex) MinValue(int) Value  { return Value{} }
func (i missingColumnIndex) MaxValue(int) Value  { return Value{} }
func (i missingColumnIndex) IsAscending() bool   { return true }
func (i missingColumnIndex) IsDescending() bool  { return false }

type missingOffsetIndex struct{}

func (missingOffsetIndex) NumPages() int                { return 1 }
func (missingOffsetIndex) Offset(int) int64             { return 0 }
func (missingOffsetIndex) CompressedPageSize(int) int64 { return 0 }
func (missingOffsetIndex) FirstRowIndex(int) int64      { return 0 }

type missingBloomFilter struct{}

func (missingBloomFilter) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (missingBloomFilter) Size() int64                       { return 0 }
func (missingBloomFilter) Check(Value) (bool, error)         { return false, nil }

type missingPage struct {
	*missingColumnChunk
	adjacentPages Pages // Pages from adjacent column for level mirroring
}

func (p missingPage) Column() int                       { return int(p.column) }
func (p missingPage) Dictionary() Dictionary            { return nil }
func (p missingPage) NumRows() int64                    { return p.numRows }
func (p missingPage) NumValues() int64                  { return p.numValues }
func (p missingPage) NumNulls() int64                   { return p.numNulls }
func (p missingPage) Bounds() (min, max Value, ok bool) { return }
func (p missingPage) Slice(i, j int64) Page {
	chunk := &missingColumnChunk{
		typ:                 p.typ,
		column:              p.column,
		numRows:             j - i,
		numValues:           j - i,
		numNulls:            j - i,
		maxRepetitionLevel:  p.maxRepetitionLevel,
		maxDefinitionLevel:  p.maxDefinitionLevel,
		adjacentChunk:       p.adjacentChunk,
		mirrorSiblingLevels: p.mirrorSiblingLevels,
		leafOptional:        p.leafOptional,
		project:             p.project,
	}
	adjacentPages := p.adjacentPages
	// Flat columns mirror per-row levels, so each slice needs its own sibling
	// reader positioned at the slice start. Seeking the page's reader would
	// move the original page and any other slice that shares it. The reader
	// opened here is owned by the slice and closed if the seek fails; a
	// successful one is closed by the slice value reader. Repeated columns
	// keep the historical slice, which shares the reader without seeking.
	if p.mirrorSiblingLevels && p.adjacentChunk != nil {
		opened := p.adjacentChunk.Pages()
		if err := opened.SeekToRow(i); err != nil {
			opened.Close()
			adjacentPages = &errorPages{err: err}
		} else {
			adjacentPages = opened
		}
	}
	return missingPage{
		missingColumnChunk: chunk,
		adjacentPages:      adjacentPages,
	}
}
func (p missingPage) Size() int64              { return 0 }
func (p missingPage) RepetitionLevels() []byte { return nil }
func (p missingPage) DefinitionLevels() []byte { return nil }
func (p missingPage) Data() encoding.Values    { return p.typ.NewValues(nil, nil) }
func (p missingPage) Values() ValueReader {
	r := &missingPageValues{
		page:          p,
		adjacentPages: p.adjacentPages,
	}
	// The buffer is only used to pull levels from a sibling column.
	if p.adjacentPages != nil {
		r.adjacentBuffer = make([]Value, 1024)
	}
	return r
}

type missingPageValues struct {
	page           missingPage
	read           int64
	adjacentPages  Pages       // Pages reader for adjacent column (to read multiple pages)
	adjacentReader ValueReader // Current page's reader for adjacent column to mirror levels
	adjacentBuffer []Value     // Buffer for reading adjacent values
	siblingBuf     []Value     // Unconsumed values from the last sibling read
	siblingOff     int
}

func (r *missingPageValues) ReadValues(values []Value) (int, error) {
	remain := r.page.numValues - r.read
	if remain == 0 {
		return 0, io.EOF
	}
	if int64(len(values)) > remain {
		values = values[:remain]
	}

	typ := r.page.typ
	columnIndex := ^r.page.column

	var n int
	var err error
	if r.page.project != nil {
		n, err = r.readProjected(values, columnIndex)
	} else if r.adjacentPages == nil {
		n, err = r.readWithoutAdjacent(values, typ, columnIndex)
	} else if r.page.mirrorSiblingLevels {
		n, err = r.readMirroringSibling(values, typ, columnIndex)
	} else {
		n, err = r.readWithAdjacent(values, typ, columnIndex)
	}

	return n, err
}

func (r *missingPageValues) readProjected(values []Value, columnIndex uint16) (int, error) {
	p := r.page.project
	if !p.useSource || r.adjacentPages == nil {
		for i := range values {
			values[i] = p.value(0, 0)
			values[i].columnIndex = columnIndex
		}
		r.read += int64(len(values))
		if r.read >= r.page.numValues {
			return len(values), io.EOF
		}
		return len(values), nil
	}

	total := 0
	for total < len(values) {
		v, err := r.nextSibling()
		if err != nil {
			r.read += int64(total)
			if err == io.EOF && total > 0 {
				return total, nil
			}
			return total, err
		}
		if v.repetitionLevel > p.groupSourceRep {
			continue
		}
		out := p.value(v.repetitionLevel, v.definitionLevel)
		out.columnIndex = columnIndex
		values[total] = out
		total++
	}
	r.read += int64(total)
	return total, nil
}

func (r *missingPageValues) nextSibling() (Value, error) {
	for {
		if r.siblingOff < len(r.siblingBuf) {
			v := r.siblingBuf[r.siblingOff]
			r.siblingOff++
			return v, nil
		}
		if r.adjacentReader == nil {
			page, err := r.adjacentPages.ReadPage()
			if err != nil {
				return Value{}, err
			}
			r.adjacentReader = page.Values()
		}
		buf := r.adjacentBuffer
		if cap(buf) < 64 {
			buf = make([]Value, 1024)
			r.adjacentBuffer = buf
		}
		buf = buf[:cap(buf)]
		n, err := r.adjacentReader.ReadValues(buf)
		if n > 0 {
			r.siblingBuf = buf[:n]
			r.siblingOff = 0
		}
		if err == io.EOF {
			r.adjacentReader = nil
			continue
		}
		if err != nil {
			return Value{}, err
		}
		if n == 0 {
			return Value{}, io.ErrUnexpectedEOF
		}
	}
}

func (r *missingPageValues) readWithoutAdjacent(values []Value, typ Type, columnIndex uint16) (int, error) {
	// For fields without siblings, assume one value per row
	// Definition level depends on whether field is required or optional
	isRequired := r.page.maxDefinitionLevel == 0

	if isRequired {
		// Required field: produce zero/default values
		for i := range values {
			values[i] = ZeroValue(typ.Kind())
			values[i].repetitionLevel = 0
			values[i].definitionLevel = r.page.maxDefinitionLevel // Present value
			values[i].columnIndex = columnIndex
		}
	} else {
		// Optional field: produce nulls
		definitionLevel := byte(0)
		if r.page.maxDefinitionLevel > 0 {
			definitionLevel = r.page.maxDefinitionLevel - 1
		}

		for i := range values {
			values[i] = Value{
				repetitionLevel: 0,
				definitionLevel: definitionLevel,
				columnIndex:     columnIndex,
			}
		}
	}

	r.read += int64(len(values))
	if r.read == r.page.numValues {
		return len(values), io.EOF
	}
	return len(values), nil
}

// adjacentReadBuf returns a prefix of the sibling buffer no longer than
// remaining. ReadValues may be given a destination larger than the buffer.
func (r *missingPageValues) adjacentReadBuf(remaining []Value) []Value {
	buf := r.adjacentBuffer
	if len(buf) == 0 {
		n := min(len(remaining), 1024)
		r.adjacentBuffer = make([]Value, n)
		buf = r.adjacentBuffer
	}
	if len(remaining) < len(buf) {
		return buf[:len(remaining)]
	}
	return buf
}

// readMirroringSibling copies repetition and definition levels from the sibling
// column using the same rules as Convert: required leaves keep the sibling
// levels and become zeros, optional leaves become null and step the definition
// level down when the sibling was present at the leaf.
func (r *missingPageValues) readMirroringSibling(values []Value, typ Type, columnIndex uint16) (int, error) {
	leafOptional := r.page.leafOptional
	maxDef := r.page.maxDefinitionLevel
	totalRead := 0

	for totalRead < len(values) {
		if r.adjacentReader == nil {
			adjacentPage, err := r.adjacentPages.ReadPage()
			if err == io.EOF {
				r.read += int64(totalRead)
				if totalRead > 0 {
					return totalRead, nil
				}
				return totalRead, io.EOF
			}
			if err != nil {
				return totalRead, err
			}
			r.adjacentReader = adjacentPage.Values()
		}

		remaining := values[totalRead:]
		buf := r.adjacentReadBuf(remaining)
		n, err := r.adjacentReader.ReadValues(buf)

		for i := range n {
			def := buf[i].definitionLevel
			rep := buf[i].repetitionLevel
			var value Value
			if leafOptional {
				if def == maxDef {
					def--
				}
			} else {
				value = ZeroValue(typ.Kind())
			}
			value.repetitionLevel = rep
			value.definitionLevel = def
			value.columnIndex = columnIndex
			remaining[i] = value
		}

		totalRead += n

		if err == io.EOF {
			r.adjacentReader = nil
			continue
		}
		if err != nil {
			return totalRead, err
		}
	}

	r.read += int64(totalRead)
	return totalRead, nil
}

func (r *missingPageValues) readWithAdjacent(values []Value, typ Type, columnIndex uint16) (int, error) {
	// Determine if this missing column is required or optional
	isRequired := r.page.maxDefinitionLevel == 0
	totalRead := 0

	for totalRead < len(values) {
		// Initialize or get next page from adjacent column if needed
		if r.adjacentReader == nil {
			adjacentPage, err := r.adjacentPages.ReadPage()
			if err == io.EOF {
				// No more adjacent pages - we're done
				if totalRead > 0 {
					r.read += int64(totalRead)
					return totalRead, nil
				}
				r.read += int64(totalRead)
				return totalRead, io.EOF
			}
			if err != nil {
				return totalRead, err
			}
			r.adjacentReader = adjacentPage.Values()
		}

		// Read values from adjacent column to get its levels.
		// The destination may be larger than the sibling buffer.
		remaining := values[totalRead:]
		buf := r.adjacentReadBuf(remaining)
		n, err := r.adjacentReader.ReadValues(buf)

		for i := range n {
			repLevel := buf[i].repetitionLevel
			adjacentDefLevel := buf[i].definitionLevel

			var defLevel byte
			var value Value

			if isRequired {
				// Required field: produce zero/default values
				value = ZeroValue(typ.Kind())
				// Mirror adjacent definition level structure
				defLevel = adjacentDefLevel
			} else {
				// Optional field: produce nulls
				// Definition level indicates null at appropriate nesting
				if adjacentDefLevel < r.page.maxDefinitionLevel {
					// Adjacent is null at some level, follow its definition
					defLevel = adjacentDefLevel
				} else {
					// Adjacent is present, but we are null at leaf level
					defLevel = r.page.maxDefinitionLevel - 1
				}
			}

			value.repetitionLevel = repLevel
			value.definitionLevel = defLevel
			value.columnIndex = columnIndex
			remaining[i] = value
		}

		totalRead += n

		if err == io.EOF {
			// Current adjacent page exhausted, try next page
			r.adjacentReader = nil
			continue
		}
		if err != nil {
			return totalRead, err
		}
	}

	r.read += int64(totalRead)
	return totalRead, nil
}

func (r *missingPageValues) Close() error {
	r.read = r.page.numValues
	if r.adjacentPages != nil {
		return r.adjacentPages.Close()
	}
	return nil
}

type convertedRowGroup struct {
	rowGroup RowGroup
	columns  []ColumnChunk
	sorting  []SortingColumn
	conv     Conversion
}

func (c *convertedRowGroup) NumRows() int64                  { return c.rowGroup.NumRows() }
func (c *convertedRowGroup) ColumnChunks() []ColumnChunk     { return c.columns }
func (c *convertedRowGroup) Schema() *Schema                 { return c.conv.Schema() }
func (c *convertedRowGroup) SortingColumns() []SortingColumn { return c.sorting }
func (c *convertedRowGroup) Rows() Rows {
	rows := c.rowGroup.Rows()
	return &convertedRows{
		Closer: rows,
		rows:   rows,
		conv:   c.conv,
	}
}

// ConvertRowReader constructs a wrapper of the given row reader which applies
// the given schema conversion to the rows.
func ConvertRowReader(rows RowReader, conv Conversion) RowReaderWithSchema {
	return &convertedRows{rows: &forwardRowSeeker{rows: rows}, conv: conv}
}

type convertedRows struct {
	io.Closer
	rows RowReadSeeker
	conv Conversion
}

func (c *convertedRows) ReadRows(rows []Row) (int, error) {
	n, err := c.rows.ReadRows(rows)
	if n > 0 {
		var convErr error
		n, convErr = c.conv.Convert(rows[:n])
		if convErr != nil {
			err = convErr
		}
	}
	return n, err
}

func (c *convertedRows) Schema() *Schema {
	return c.conv.Schema()
}

func (c *convertedRows) SeekToRow(rowIndex int64) error {
	return c.rows.SeekToRow(rowIndex)
}

// convertedColumnChunk wraps a ColumnChunk to fix the column index after reordering.
// When ConvertRowGroup reorders columns, the underlying chunk's Column() method
// returns the original position. This wrapper fixes both Column() and the
// columnIndex in values read from the chunk.
type convertedColumnChunk struct {
	chunk             ColumnChunk
	targetColumnIndex uint16 // XOR-encoded column index (^uint16(columnIndex))
}

func (c *convertedColumnChunk) Type() Type {
	return c.chunk.Type()
}

func (c *convertedColumnChunk) Column() int {
	return int(^c.targetColumnIndex)
}

func (c *convertedColumnChunk) NumValues() int64 {
	return c.chunk.NumValues()
}

func (c *convertedColumnChunk) Pages() Pages {
	return &convertedPages{
		pages:             c.chunk.Pages(),
		targetColumnIndex: c.targetColumnIndex,
	}
}

func (c *convertedColumnChunk) ColumnIndex() (ColumnIndex, error) {
	return c.chunk.ColumnIndex()
}

func (c *convertedColumnChunk) OffsetIndex() (OffsetIndex, error) {
	return c.chunk.OffsetIndex()
}

func (c *convertedColumnChunk) BloomFilter() BloomFilter {
	return c.chunk.BloomFilter()
}

// convertedPages wraps Pages to return convertedPage instances.
type convertedPages struct {
	pages             Pages
	targetColumnIndex uint16
}

func (p *convertedPages) ReadPage() (Page, error) {
	page, err := p.pages.ReadPage()
	if err != nil {
		return nil, err
	}
	return &convertedPage{
		page:              page,
		targetColumnIndex: p.targetColumnIndex,
	}, nil
}

func (p *convertedPages) SeekToRow(rowIndex int64) error {
	return p.pages.SeekToRow(rowIndex)
}

func (p *convertedPages) Close() error {
	return p.pages.Close()
}

// convertedPage wraps a Page to return a convertedValueReader.
type convertedPage struct {
	page              Page
	targetColumnIndex uint16
}

func (p *convertedPage) Type() Type {
	return p.page.Type()
}

func (p *convertedPage) Column() int {
	return int(^p.targetColumnIndex)
}

func (p *convertedPage) Dictionary() Dictionary {
	return p.page.Dictionary()
}

func (p *convertedPage) NumRows() int64 {
	return p.page.NumRows()
}

func (p *convertedPage) NumValues() int64 {
	return p.page.NumValues()
}

func (p *convertedPage) NumNulls() int64 {
	return p.page.NumNulls()
}

func (p *convertedPage) Bounds() (min, max Value, ok bool) {
	return p.page.Bounds()
}

func (p *convertedPage) Size() int64 {
	return p.page.Size()
}

func (p *convertedPage) RepetitionLevels() []byte {
	return p.page.RepetitionLevels()
}

func (p *convertedPage) DefinitionLevels() []byte {
	return p.page.DefinitionLevels()
}

func (p *convertedPage) Data() encoding.Values {
	return p.page.Data()
}

func (p *convertedPage) Values() ValueReader {
	return &convertedValueReader{
		reader:            p.page.Values(),
		targetColumnIndex: p.targetColumnIndex,
	}
}

func (p *convertedPage) Slice(i, j int64) Page {
	return &convertedPage{
		page:              p.page.Slice(i, j),
		targetColumnIndex: p.targetColumnIndex,
	}
}

func (p *convertedPage) Retain() {
	Retain(p.page)
}

func (p *convertedPage) Release() {
	Release(p.page)
}

func (p *convertedPage) ReleaseAndDetachValues() {
	releaseAndDetachValues(p.page)
}

var (
	_ retainable = (*convertedPage)(nil)
	_ releasable = (*convertedPage)(nil)
	_ detachable = (*convertedPage)(nil)
)

// convertedValueReader wraps a ValueReader to rewrite columnIndex in values.
type convertedValueReader struct {
	reader            ValueReader
	targetColumnIndex uint16
}

func (r *convertedValueReader) ReadValues(values []Value) (int, error) {
	n, err := r.reader.ReadValues(values)
	// Rewrite columnIndex for all values to match the target column position
	for i := range n {
		values[i].columnIndex = r.targetColumnIndex
	}
	return n, err
}

var (
	trueBytes  = []byte(`true`)
	falseBytes = []byte(`false`)
	unixEpoch  = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
)

func convertBooleanToInt32(v Value) (Value, error) {
	return v.convertToInt32(int32(v.byte())), nil
}

func convertBooleanToInt64(v Value) (Value, error) {
	return v.convertToInt64(int64(v.byte())), nil
}

func convertBooleanToInt96(v Value) (Value, error) {
	return v.convertToInt96(deprecated.Int96{0: uint32(v.byte())}), nil
}

func convertBooleanToFloat(v Value) (Value, error) {
	return v.convertToFloat(float32(v.byte())), nil
}

func convertBooleanToDouble(v Value) (Value, error) {
	return v.convertToDouble(float64(v.byte())), nil
}

func convertBooleanToByteArray(v Value) (Value, error) {
	return v.convertToByteArray([]byte{v.byte()}), nil
}

func convertBooleanToFixedLenByteArray(v Value, size int) (Value, error) {
	b := []byte{v.byte()}
	c := make([]byte, size)
	copy(c, b)
	return v.convertToFixedLenByteArray(c), nil
}

func convertBooleanToString(v Value) (Value, error) {
	b := ([]byte)(nil)
	if v.boolean() {
		b = trueBytes
	} else {
		b = falseBytes
	}
	return v.convertToByteArray(b), nil
}

func convertInt32ToBoolean(v Value) (Value, error) {
	return v.convertToBoolean(v.int32() != 0), nil
}

func convertInt32ToInt64(v Value) (Value, error) {
	return v.convertToInt64(int64(v.int32())), nil
}

func convertInt32ToInt96(v Value) (Value, error) {
	return v.convertToInt96(deprecated.Int32ToInt96(v.int32())), nil
}

func convertInt32ToFloat(v Value) (Value, error) {
	return v.convertToFloat(float32(v.int32())), nil
}

func convertInt32ToDouble(v Value) (Value, error) {
	return v.convertToDouble(float64(v.int32())), nil
}

func convertInt32ToByteArray(v Value) (Value, error) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v.uint32())
	return v.convertToByteArray(b), nil
}

func convertInt32ToFixedLenByteArray(v Value, size int) (Value, error) {
	b := make([]byte, 4)
	c := make([]byte, size)
	binary.LittleEndian.PutUint32(b, v.uint32())
	copy(c, b)
	return v.convertToFixedLenByteArray(c), nil
}

func convertInt32ToString(v Value) (Value, error) {
	return v.convertToByteArray(strconv.AppendInt(nil, int64(v.int32()), 10)), nil
}

func convertInt64ToBoolean(v Value) (Value, error) {
	return v.convertToBoolean(v.int64() != 0), nil
}

func convertInt64ToInt32(v Value) (Value, error) {
	return v.convertToInt32(int32(v.int64())), nil
}

func convertInt64ToInt96(v Value) (Value, error) {
	return v.convertToInt96(deprecated.Int64ToInt96(v.int64())), nil
}

func convertInt64ToFloat(v Value) (Value, error) {
	return v.convertToFloat(float32(v.int64())), nil
}

func convertInt64ToDouble(v Value) (Value, error) {
	return v.convertToDouble(float64(v.int64())), nil
}

func convertInt64ToByteArray(v Value) (Value, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v.uint64())
	return v.convertToByteArray(b), nil
}

func convertInt64ToFixedLenByteArray(v Value, size int) (Value, error) {
	b := make([]byte, 8)
	c := make([]byte, size)
	binary.LittleEndian.PutUint64(b, v.uint64())
	copy(c, b)
	return v.convertToFixedLenByteArray(c), nil
}

func convertInt64ToString(v Value) (Value, error) {
	return v.convertToByteArray(strconv.AppendInt(nil, v.int64(), 10)), nil
}

func convertInt96ToBoolean(v Value) (Value, error) {
	return v.convertToBoolean(!v.int96().IsZero()), nil
}

func convertInt96ToInt32(v Value) (Value, error) {
	return v.convertToInt32(v.int96().Int32()), nil
}

func convertInt96ToInt64(v Value) (Value, error) {
	return v.convertToInt64(v.int96().Int64()), nil
}

func convertInt96ToFloat(v Value) (Value, error) {
	return v, invalidConversion(v, "INT96", "FLOAT")
}

func convertInt96ToDouble(v Value) (Value, error) {
	return v, invalidConversion(v, "INT96", "DOUBLE")
}

func convertInt96ToByteArray(v Value) (Value, error) {
	return v.convertToByteArray(v.byteArray()), nil
}

func convertInt96ToFixedLenByteArray(v Value, size int) (Value, error) {
	b := v.byteArray()
	if len(b) < size {
		c := make([]byte, size)
		copy(c, b)
		b = c
	} else {
		b = b[:size]
	}
	return v.convertToFixedLenByteArray(b), nil
}

func convertInt96ToString(v Value) (Value, error) {
	return v.convertToByteArray([]byte(v.String())), nil
}

func convertFloatToBoolean(v Value) (Value, error) {
	return v.convertToBoolean(v.float() != 0), nil
}

func convertFloatToInt32(v Value) (Value, error) {
	return v.convertToInt32(int32(v.float())), nil
}

func convertFloatToInt64(v Value) (Value, error) {
	return v.convertToInt64(int64(v.float())), nil
}

func convertFloatToInt96(v Value) (Value, error) {
	return v, invalidConversion(v, "FLOAT", "INT96")
}

func convertFloatToDouble(v Value) (Value, error) {
	return v.convertToDouble(float64(v.float())), nil
}

func convertFloatToByteArray(v Value) (Value, error) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v.uint32())
	return v.convertToByteArray(b), nil
}

func convertFloatToFixedLenByteArray(v Value, size int) (Value, error) {
	b := make([]byte, 4)
	c := make([]byte, size)
	binary.LittleEndian.PutUint32(b, v.uint32())
	copy(c, b)
	return v.convertToFixedLenByteArray(c), nil
}

func convertFloatToString(v Value) (Value, error) {
	return v.convertToByteArray(strconv.AppendFloat(nil, float64(v.float()), 'g', -1, 32)), nil
}

func convertDoubleToBoolean(v Value) (Value, error) {
	return v.convertToBoolean(v.double() != 0), nil
}

func convertDoubleToInt32(v Value) (Value, error) {
	return v.convertToInt32(int32(v.double())), nil
}

func convertDoubleToInt64(v Value) (Value, error) {
	return v.convertToInt64(int64(v.double())), nil
}

func convertDoubleToInt96(v Value) (Value, error) {
	return v, invalidConversion(v, "FLOAT", "INT96")
}

func convertDoubleToFloat(v Value) (Value, error) {
	return v.convertToFloat(float32(v.double())), nil
}

func convertDoubleToByteArray(v Value) (Value, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v.uint64())
	return v.convertToByteArray(b), nil
}

func convertDoubleToFixedLenByteArray(v Value, size int) (Value, error) {
	b := make([]byte, 8)
	c := make([]byte, size)
	binary.LittleEndian.PutUint64(b, v.uint64())
	copy(c, b)
	return v.convertToFixedLenByteArray(c), nil
}

func convertDoubleToString(v Value) (Value, error) {
	return v.convertToByteArray(strconv.AppendFloat(nil, v.double(), 'g', -1, 64)), nil
}

func convertByteArrayToBoolean(v Value) (Value, error) {
	return v.convertToBoolean(!isZero(v.byteArray())), nil
}

func convertByteArrayToInt32(v Value) (Value, error) {
	b := make([]byte, 4)
	copy(b, v.byteArray())
	return v.convertToInt32(int32(binary.LittleEndian.Uint32(b))), nil
}

func convertByteArrayToInt64(v Value) (Value, error) {
	b := make([]byte, 8)
	copy(b, v.byteArray())
	return v.convertToInt64(int64(binary.LittleEndian.Uint64(b))), nil
}

func convertByteArrayToInt96(v Value) (Value, error) {
	b := make([]byte, 12)
	copy(b, v.byteArray())
	return v.convertToInt96(deprecated.Int96{
		0: binary.LittleEndian.Uint32(b[0:4]),
		1: binary.LittleEndian.Uint32(b[4:8]),
		2: binary.LittleEndian.Uint32(b[8:12]),
	}), nil
}

func convertByteArrayToFloat(v Value) (Value, error) {
	b := make([]byte, 4)
	copy(b, v.byteArray())
	return v.convertToFloat(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil
}

func convertByteArrayToDouble(v Value) (Value, error) {
	b := make([]byte, 8)
	copy(b, v.byteArray())
	return v.convertToDouble(math.Float64frombits(binary.LittleEndian.Uint64(b))), nil
}

func convertByteArrayToFixedLenByteArray(v Value, size int) (Value, error) {
	b := v.byteArray()
	if len(b) < size {
		c := make([]byte, size)
		copy(c, b)
		b = c
	} else {
		b = b[:size]
	}
	return v.convertToFixedLenByteArray(b), nil
}

func convertFixedLenByteArrayToString(v Value) (Value, error) {
	b := v.byteArray()
	c := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(c, b)
	return v.convertToByteArray(c), nil
}

func convertStringToBoolean(v Value) (Value, error) {
	b, err := strconv.ParseBool(v.string())
	if err != nil {
		return v, conversionError(v, "STRING", "BOOLEAN", err)
	}
	return v.convertToBoolean(b), nil
}

func convertStringToInt32(v Value) (Value, error) {
	i, err := strconv.ParseInt(v.string(), 10, 32)
	if err != nil {
		return v, conversionError(v, "STRING", "INT32", err)
	}
	return v.convertToInt32(int32(i)), nil
}

func convertStringToInt64(v Value) (Value, error) {
	i, err := strconv.ParseInt(v.string(), 10, 64)
	if err != nil {
		return v, conversionError(v, "STRING", "INT64", err)
	}
	return v.convertToInt64(i), nil
}

func convertStringToInt96(v Value) (Value, error) {
	i, ok := new(big.Int).SetString(v.string(), 10)
	if !ok {
		return v, conversionError(v, "STRING", "INT96", strconv.ErrSyntax)
	}
	b := i.Bytes()
	c := make([]byte, 12)
	copy(c, b)
	if cpu.IsBigEndian {
		bufLen := len(c)
		for idx := 0; idx < bufLen; idx = idx + 4 {
			for m, n := (idx + 0), (idx + 3); m < n; m, n = m+1, n-1 {
				c[m], c[n] = c[n], c[m]
			}
		}
	}
	i96 := unsafecast.Slice[deprecated.Int96](c)
	return v.convertToInt96(i96[0]), nil
}

func convertStringToFloat(v Value) (Value, error) {
	f, err := strconv.ParseFloat(v.string(), 32)
	if err != nil {
		return v, conversionError(v, "STRING", "FLOAT", err)
	}
	return v.convertToFloat(float32(f)), nil
}

func convertStringToDouble(v Value) (Value, error) {
	f, err := strconv.ParseFloat(v.string(), 64)
	if err != nil {
		return v, conversionError(v, "STRING", "DOUBLE", err)
	}
	return v.convertToDouble(f), nil
}

func convertStringToFixedLenByteArray(v Value, size int) (Value, error) {
	b := v.byteArray()
	c := make([]byte, size)
	_, err := hex.Decode(c, b)
	if err != nil {
		return v, conversionError(v, "STRING", "BYTE_ARRAY", err)
	}
	return v.convertToFixedLenByteArray(c), nil
}

func convertStringToDate(v Value, tz *time.Location) (Value, error) {
	t, err := time.ParseInLocation("2006-01-02", v.string(), tz)
	if err != nil {
		return v, conversionError(v, "STRING", "DATE", err)
	}
	d := daysSinceUnixEpoch(t)
	return v.convertToInt32(int32(d)), nil
}

func convertStringToTimeMillis(v Value, tz *time.Location) (Value, error) {
	t, err := time.ParseInLocation("15:04:05.999", v.string(), tz)
	if err != nil {
		return v, conversionError(v, "STRING", "TIME", err)
	}
	m := nearestMidnightLessThan(t)
	milliseconds := t.Sub(m).Milliseconds()
	return v.convertToInt32(int32(milliseconds)), nil
}

func convertStringToTimeMicros(v Value, tz *time.Location) (Value, error) {
	t, err := time.ParseInLocation("15:04:05.999999", v.string(), tz)
	if err != nil {
		return v, conversionError(v, "STRING", "TIME", err)
	}
	m := nearestMidnightLessThan(t)
	microseconds := t.Sub(m).Microseconds()
	return v.convertToInt64(microseconds), nil
}

func convertStringToTimeNanos(v Value, tz *time.Location) (Value, error) {
	t, err := time.ParseInLocation("15:04:05.999999999", v.string(), tz)
	if err != nil {
		return v, conversionError(v, "STRING", "TIME", err)
	}
	m := nearestMidnightLessThan(t)
	nanoseconds := t.Sub(m).Nanoseconds()
	return v.convertToInt64(nanoseconds), nil
}

func convertDateToTimestamp(v Value, u format.TimeUnit, tz *time.Location) (Value, error) {
	days := int(v.int32())
	t := unixEpoch.AddDate(0, 0, days)
	unit := timeUnitDuration(u)
	// A date within about 292 years of the epoch still fits in a Duration.
	// Dividing that duration keeps the previous result, including its
	// truncation. Farther dates are an exact number of days in UTC.
	instant := t.In(tz)
	delta := instant.Sub(unixEpoch)
	if unixEpoch.Add(delta).Equal(instant) {
		return v.convertToInt64(int64(delta / unit)), nil
	}
	perDay := int64((24 * time.Hour) / unit)
	scaled, ok := scaleInt64(int64(days), perDay)
	if !ok {
		return v, invalidUnitConversion(v, "DATE", "TIMESTAMP")
	}
	return v.convertToInt64(scaled), nil
}

func convertDateToString(v Value) (Value, error) {
	t := unixEpoch.AddDate(0, 0, int(v.int32()))
	b := t.AppendFormat(make([]byte, 0, 10), "2006-01-02")
	return v.convertToByteArray(b), nil
}

func convertTimeMillisToString(v Value, tz *time.Location) (Value, error) {
	t := time.UnixMilli(int64(v.int32())).In(tz)
	b := t.AppendFormat(make([]byte, 0, 12), "15:04:05.999")
	return v.convertToByteArray(b), nil
}

func convertTimeMicrosToString(v Value, tz *time.Location) (Value, error) {
	t := time.UnixMicro(v.int64()).In(tz)
	b := t.AppendFormat(make([]byte, 0, 15), "15:04:05.999999")
	return v.convertToByteArray(b), nil
}

func convertTimeNanosToString(v Value, tz *time.Location) (Value, error) {
	t := time.Unix(0, v.int64()).In(tz)
	b := t.AppendFormat(make([]byte, 0, 18), "15:04:05.999999999")
	return v.convertToByteArray(b), nil
}

func convertTimestampToDate(v Value, u format.TimeUnit, tz *time.Location) (Value, error) {
	day, ok := epochDay(timestamp(v, u, tz))
	if !ok {
		return v, invalidUnitConversion(v, "TIMESTAMP", "DATE")
	}
	return v.convertToInt32(int32(day)), nil
}

func convertTimestampToTimeMillis(v Value, u format.TimeUnit, sourceZone, targetZone *time.Location) (Value, error) {
	t := timestamp(v, u, sourceZone)
	m := nearestMidnightLessThan(t)
	milliseconds := t.In(targetZone).Sub(m).Milliseconds()
	return v.convertToInt32(int32(milliseconds)), nil
}

func convertTimestampToTimeMicros(v Value, u format.TimeUnit, sourceZone, targetZone *time.Location) (Value, error) {
	t := timestamp(v, u, sourceZone)
	m := nearestMidnightLessThan(t)
	microseconds := t.In(targetZone).Sub(m).Microseconds()
	return v.convertToInt64(int64(microseconds)), nil
}

func convertTimestampToTimeNanos(v Value, u format.TimeUnit, sourceZone, targetZone *time.Location) (Value, error) {
	t := timestamp(v, u, sourceZone)
	m := nearestMidnightLessThan(t)
	nanoseconds := t.In(targetZone).Sub(m).Nanoseconds()
	return v.convertToInt64(nanoseconds), nil
}

func convertTimestampToTimestamp(v Value, sourceUnit, targetUnit format.TimeUnit) (Value, error) {
	target, ok := convertTimestampUnit(v.int64(), timeUnitDuration(sourceUnit), timeUnitDuration(targetUnit))
	if !ok {
		return v, invalidUnitConversion(v, "TIMESTAMP", "TIMESTAMP")
	}
	return v.convertToInt64(target), nil
}

// invalidUnitConversion formats an overflow. It stays out of line so the
// success path of a unit conversion can still be inlined.
//
//go:noinline
func invalidUnitConversion(v Value, from, to string) error {
	return invalidConversion(v, from, to)
}

const nanosecondsPerDay = 24 * 60 * 60 * 1e9

const secondsPerDay = 24 * 60 * 60

// epochDay reports days from the Unix epoch to t.
//
// Instants within a time.Duration of the epoch keep the historical count,
// int(duration.Hours())/24. That count truncates toward zero, so some
// instants before the epoch do not land on their calendar day; those results
// already ship and stay as they are. Instants past the Duration limits use
// the calendar day when it fits in an int32. ok is false otherwise.
func epochDay(t time.Time) (day int64, ok bool) {
	sec := t.Unix()
	// One second inside the largest Duration, so the subtraction below cannot
	// saturate and the historical count is the one callers already observe.
	const limit = int64(math.MaxInt64/int64(time.Second)) - 1
	if sec > -limit && sec < limit {
		return int64(int(t.Sub(unixEpoch).Hours()) / 24), true
	}
	d := t.Sub(unixEpoch)
	if unixEpoch.Add(d).Equal(t) {
		return int64(int(d.Hours()) / 24), true
	}
	day = floorDivInt64(sec, secondsPerDay)
	if day > math.MaxInt32 || day < math.MinInt32 {
		return 0, false
	}
	return day, true
}

func daysSinceUnixEpoch(t time.Time) int {
	if day, ok := epochDay(t); ok {
		return int(day)
	}
	// DATE cannot represent the day. Writes have no error to return, so keep
	// the saturated Duration count they produced before.
	return int(t.Sub(unixEpoch).Hours()) / 24
}

func floorDivInt64(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func nearestMidnightLessThan(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

func timestamp(v Value, u format.TimeUnit, tz *time.Location) time.Time {
	n := v.int64()
	var t time.Time
	switch u.Value.(type) {
	case *format.MilliSeconds:
		t = time.UnixMilli(n)
	case *format.MicroSeconds:
		t = time.UnixMicro(n)
	default:
		t = time.Unix(0, n)
	}
	return t.In(tz)
}

// convertTimestampUnit scales v from one timestamp unit to another.
// Fractions left by a coarser target are truncated toward zero.
// ok is false when the units are empty or the scaled value does not fit in an int64.
func convertTimestampUnit(v int64, from, to time.Duration) (int64, bool) {
	fromScale := int64(from)
	toScale := int64(to)
	switch {
	case fromScale <= 0 || toScale <= 0:
		return 0, false
	case fromScale == toScale:
		return v, true
	case fromScale > toScale && fromScale%toScale == 0:
		return scaleInt64(v, fromScale/toScale)
	case toScale%fromScale == 0:
		return v / (toScale / fromScale), true
	default:
		return 0, false
	}
}

// scaleInt64 returns v*factor. ok is false when factor is not positive or the
// product does not fit in an int64. The common timestamp ratios are spelled
// out so the overflow limits are constant comparisons.
func scaleInt64(v, factor int64) (int64, bool) {
	switch factor {
	case 1:
		return v, true
	case 1_000:
		if v > math.MaxInt64/1_000 || v < math.MinInt64/1_000 {
			return 0, false
		}
		return v * 1_000, true
	case 1_000_000:
		if v > math.MaxInt64/1_000_000 || v < math.MinInt64/1_000_000 {
			return 0, false
		}
		return v * 1_000_000, true
	default:
		if factor <= 0 || v > math.MaxInt64/factor || v < math.MinInt64/factor {
			return 0, false
		}
		return v * factor, true
	}
}

// timeUnitDuration returns the precision of unit, defaulting to nanoseconds
// when the unit is unset (an unknown unit written by a newer implementation).
func timeUnitDuration(unit format.TimeUnit) time.Duration {
	if unit.Value == nil {
		return time.Nanosecond
	}
	return unit.Value.Duration()
}

func invalidConversion(value Value, from, to string) error {
	return fmt.Errorf("%s to %s: %s: %w", from, to, value, ErrInvalidConversion)
}

func conversionError(value Value, from, to string, err error) error {
	return fmt.Errorf("%s to %s: %q: %s: %w", from, to, value.string(), err, ErrInvalidConversion)
}
