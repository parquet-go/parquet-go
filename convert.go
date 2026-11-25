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
	// missing synthesizes values when the target column is absent from the
	// source. Nil for columns that exist in both schemas.
	missing *missingField
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

// convertToTypeKeepingNulls rewrites column values from sourceType to
// targetType, leaving nulls null when their mapped definition level still
// denotes null.
//
// ConvertValue reads the zero bits of a null and returns a typed zero, which
// would clear IsNull while the definition level stayed put. definitionLevels
// maps a source definition level to the target level. A required column has
// no null level, so it uses convertToType and a null still becomes the zero
// of that field.
//
// kind is read directly. (*Value).isNull takes a pointer, which forces every
// value in this loop onto the stack.
func convertToTypeKeepingNulls(targetType, sourceType Type, maxDefinitionLevel byte, definitionLevels []byte) conversionFunc {
	return func(column []Value) error {
		for i, v := range column {
			if v.kind == 0 {
				d := v.definitionLevel
				if int(d) >= len(definitionLevels) || definitionLevels[d] < maxDefinitionLevel {
					continue
				}
			}
			converted, err := targetType.ConvertValue(v, sourceType)
			if err != nil {
				return err
			}
			column[i].ptr = converted.ptr
			column[i].u64 = converted.u64
			column[i].kind = converted.kind
		}
		return nil
	}
}

// missingField synthesizes a target column that the source schema does not
// have. Each value corresponds to one instance of the deepest matching source
// group. Definition levels are remapped onto the target schema and clamped to
// that group's definition level, so the new leaf is null (or the zero value,
// when the leaf is required) for exactly those instances.
type missingField struct {
	needsValues      bool
	groupSourceRep   byte
	groupSourceDef   byte
	groupTargetDef   byte
	repetitionLevels []byte
	definitionLevels []byte
	null             bool
	kind             Kind
}

func (m *missingField) append(dst []Value, src []Value, columnIndex int) []Value {
	col := ^uint16(columnIndex)
	if !m.needsValues {
		return append(dst, m.value(m.repetitionLevels[0], m.clampDef(m.definitionLevels[0]), col))
	}
	start := len(dst)
	for i := range src {
		rep := src[i].repetitionLevel
		if rep > m.groupSourceRep {
			continue
		}
		def := min(src[i].definitionLevel, m.groupSourceDef)
		dst = append(dst, m.value(m.mapLevel(m.repetitionLevels, rep), m.clampDef(m.mapLevel(m.definitionLevels, def)), col))
	}
	if len(dst) == start {
		dst = append(dst, m.value(m.repetitionLevels[0], m.clampDef(m.definitionLevels[0]), col))
	}
	return dst
}

func (m *missingField) mapLevel(levels []byte, level byte) byte {
	if int(level) >= len(levels) {
		return 0
	}
	return levels[level]
}

func (m *missingField) clampDef(def byte) byte {
	if def > m.groupTargetDef {
		return m.groupTargetDef
	}
	return def
}

func (m *missingField) value(rep, def byte, columnIndex uint16) Value {
	var v Value
	if !m.null {
		v = ZeroValue(m.kind)
	}
	v.repetitionLevel = rep
	v.definitionLevel = def
	v.columnIndex = columnIndex
	return v
}

// enclosingGroupPath is the longest prefix of path that addresses a group in
// source. The final element of path is the missing leaf, so it is not part of
// the prefix. Ancestors that the source does not have, or that are leaves,
// end the walk.
func enclosingGroupPath(source Node, path columnPath) columnPath {
	node := source
	n := 0
	for n < len(path)-1 {
		field := fieldByName(node, path[n])
		if field == nil || field.Leaf() {
			break
		}
		node = field
		n++
	}
	return path[:n]
}

// groupLevelMapping maps source repetition and definition levels along
// groupPath onto the target schema. The returned slices are indexed by the
// source level. groupTargetDef is the target definition level of the group
// itself, which is the highest definition level a missing leaf inside it can
// take.
func groupLevelMapping(target, source Node, groupPath columnPath) (rep, def []byte, groupSourceRep, groupSourceDef, groupTargetDef byte) {
	rep = make([]byte, len(groupPath)+1)
	def = make([]byte, len(groupPath)+1)
	targetNode := target
	sourceNode := source
	var targetRep, targetDef byte
	for _, name := range groupPath {
		targetField := fieldByName(targetNode, name)
		sourceField := fieldByName(sourceNode, name)
		if targetField == nil || sourceField == nil {
			break
		}
		targetNode = targetField
		sourceNode = sourceField
		targetRep, targetDef = applyFieldRepetitionType(fieldRepetitionTypeOf(targetNode), targetRep, targetDef)
		groupSourceRep, groupSourceDef = applyFieldRepetitionType(fieldRepetitionTypeOf(sourceNode), groupSourceRep, groupSourceDef)
		rep[groupSourceRep] = targetRep
		def[groupSourceDef] = targetDef
	}
	groupTargetDef = targetDef
	return rep[:groupSourceRep+1], def[:groupSourceDef+1], groupSourceRep, groupSourceDef, groupTargetDef
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
			if conv.missing != nil {
				var src []Value
				if conv.missing.needsValues && conv.sourceIndex >= 0 {
					src = source.columns[conv.sourceIndex]
				}
				row = conv.missing.append(row, src, columnIndex)
				continue
			}

			columnOffset := len(row)

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
	var sourceTemplates *columnTemplate

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

				if !EqualTypes(targetType, sourceType) {
					if targetColumn.maxDefinitionLevel == 0 {
						conversions = append(conversions, convertToType(targetType, sourceType))
					} else {
						conversions = append(conversions, convertToTypeKeepingNulls(
							targetType,
							sourceType,
							targetColumn.maxDefinitionLevel,
							definitionLevels,
						))
					}
				}
				if !isDirectLevelMapping(repetitionLevels) || !isDirectLevelMapping(definitionLevels) {
					conversions = append(conversions, convertToLevels(repetitionLevels, definitionLevels))
				}
			}
		}
		if isMissingColumn {
			// The column is absent from the source. Synthesize one value per
			// instance of the deepest source group that contains it, instead of
			// copying a sibling leaf's levels. A sibling nested deeper than that
			// group repeats more often than the new field, and a sibling that
			// does not exist at all used to collapse the field to a root null.
			if sourceTemplates == nil {
				sourceTemplates = sourceMapping.templates()
			}
			groupPath := enclosingGroupPath(from, path)
			repLevels, defLevels, groupSourceRep, groupSourceDef, groupTargetDef := groupLevelMapping(to, from, groupPath)
			needsValues := groupSourceRep > 0 || groupSourceDef > 0
			tmpl := sourceTemplates.at(groupPath)
			direct := tmpl != nil && tmpl.has && tmpl.direct && tmpl.leaf.node != nil
			// A required group has no level of its own. Without a direct leaf,
			// there is nothing to remap, and a single placeholder at level 0
			// is what readers already observe for a missing nested column
			// (a nil proto message read back as an optional group, for example).
			// Remapping would mark that group present.
			if !needsValues && !direct {
				repLevels = []byte{0}
				defLevels = []byte{0}
				groupSourceRep, groupSourceDef, groupTargetDef = 0, 0, 0
			}
			sourceIndex := -1
			if tmpl != nil && tmpl.has && tmpl.leaf.node != nil && (needsValues || direct) {
				sourceIndex = int(tmpl.leaf.columnIndex)
			}
			columns[i] = conversionColumn{
				sourceIndex:   sourceIndex,
				convertValues: convertToSelf,
				targetKind:    targetColumn.node.Type().Kind(),
				isOptional:    targetColumn.maxDefinitionLevel > 0,
				variantIndex:  -1,
				missing: &missingField{
					needsValues:      needsValues,
					groupSourceRep:   groupSourceRep,
					groupSourceDef:   groupSourceDef,
					groupTargetDef:   groupTargetDef,
					repetitionLevels: repLevels,
					definitionLevels: defLevels,
					null:             targetColumn.node.Optional() || targetColumn.node.Repeated(),
					kind:             targetColumn.node.Type().Kind(),
				},
			}
			continue
		}

		// Store target column type for creating proper null values
		targetType := targetColumn.node.Type()

		// Determine sourceIndex: use the source column index (which may be a
		// sibling's index if the column is missing but has a sibling).
		// For missing columns without siblings, sourceColumn.columnIndex will
		// be math.MaxUint16 (from the initial lookup returning an empty leafColumn).
		sourceIndex := int(sourceColumn.columnIndex)
		// Only set to -1 if truly missing AND no sibling was found
		if isMissingColumn && sourceColumn.node == nil {
			sourceIndex = -1
		}

		// Determine if target column is optional
		isOptional := targetColumn.maxDefinitionLevel > 0

		columns[i] = conversionColumn{
			sourceIndex:   sourceIndex,
			convertValues: multiConversionFunc(conversions),
			targetKind:    targetType.Kind(), // Store target kind for null value creation
			isOptional:    isOptional,
			variantIndex:  -1,
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
			chunk := &missingColumnChunk{
				typ:                leaf.node.Type(),
				column:             i,
				numRows:            numRows,
				numValues:          numRows, // May be adjusted when reading
				numNulls:           numRows, // Depends on required vs optional
				maxRepetitionLevel: leaf.maxRepetitionLevel,
				maxDefinitionLevel: leaf.maxDefinitionLevel,
				leafOptional:       leaf.node.Optional(),
			}
			// Use the same synthesis as Convert. The template is a source
			// column, read from the original row group so its levels have
			// not already been remapped.
			if c, ok := conv.(*conversion); ok && int(i) < len(c.columns) {
				col := &c.columns[i]
				chunk.synth = col.missing
				if col.missing != nil && col.missing.needsValues && col.sourceIndex >= 0 && col.sourceIndex < len(rowGroupColumns) {
					chunk.adjacentChunk = rowGroupColumns[col.sourceIndex]
					// One synthesized value per row, so a slice can seek by row.
					chunk.mirrorSiblingLevels = col.missing.groupSourceRep == 0
				}
			}
			if chunk.synth == nil && missingColumnNeedsSibling(sourceSchema, leaf.path, leaf.maxRepetitionLevel) {
				chunk.adjacentChunk = findAdjacentColumnChunk(schema, i, columns, sourceMapping)
				chunk.mirrorSiblingLevels = leaf.maxRepetitionLevel == 0 && chunk.adjacentChunk != nil
			}
			columns[i] = chunk
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
	synth               *missingField
	counted             bool
}

func (c *missingColumnChunk) ensureCount() {
	if c.counted || c.synth == nil || !c.synth.needsValues || c.synth.groupSourceRep == 0 || c.adjacentChunk == nil {
		c.counted = true
		return
	}
	c.counted = true
	if n := countKeptValues(c.adjacentChunk, c.synth.groupSourceRep); n > 0 {
		c.numValues = n
		c.numNulls = n
	}
}

// countKeptValues counts template values whose repetition level is at most
// maxRep. That is the number of enclosing-group instances.
func countKeptValues(chunk ColumnChunk, maxRep byte) int64 {
	pages := chunk.Pages()
	defer pages.Close()
	buf := make([]Value, 1024)
	var n int64
	var reader ValueReader
	for {
		if reader == nil {
			page, err := pages.ReadPage()
			if err != nil {
				return n
			}
			reader = page.Values()
		}
		k, err := reader.ReadValues(buf)
		for i := range k {
			if buf[i].repetitionLevel <= maxRep {
				n++
			}
		}
		if err == io.EOF {
			reader = nil
			continue
		}
		if err != nil {
			return n
		}
	}
}

func (c *missingColumnChunk) Type() Type  { return c.typ }
func (c *missingColumnChunk) Column() int { return int(c.column) }
func (c *missingColumnChunk) Pages() Pages {
	c.ensureCount()
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
func (c *missingColumnChunk) NumValues() int64 {
	c.ensureCount()
	return c.numValues
}

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
		synth:               p.synth,
		counted:             true,
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
	pending        []Value     // Template values read but not yet synthesized
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
	if r.page.synth != nil {
		n, err = r.readSynthesized(values, columnIndex)
	} else if r.adjacentPages == nil {
		n, err = r.readWithoutAdjacent(values, typ, columnIndex)
	} else if r.page.mirrorSiblingLevels {
		n, err = r.readMirroringSibling(values, typ, columnIndex)
	} else {
		n, err = r.readWithAdjacent(values, typ, columnIndex)
	}

	return n, err
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

// readSynthesized emits the same values Convert does for a missing column.
// A fixed group is one placeholder per row. Otherwise each template value at
// or below the enclosing group's repetition level becomes one output value.
func (r *missingPageValues) readSynthesized(values []Value, columnIndex uint16) (int, error) {
	synth := r.page.synth
	if !synth.needsValues || r.adjacentPages == nil {
		v := synth.value(synth.repetitionLevels[0], synth.clampDef(synth.definitionLevels[0]), columnIndex)
		for i := range values {
			values[i] = v
		}
		r.read += int64(len(values))
		if r.read == r.page.numValues {
			return len(values), io.EOF
		}
		return len(values), nil
	}

	total := 0
	for total < len(values) {
		if len(r.pending) == 0 {
			if r.adjacentReader == nil {
				adjacentPage, err := r.adjacentPages.ReadPage()
				if err == io.EOF {
					r.read += int64(total)
					if total > 0 {
						return total, nil
					}
					return total, io.EOF
				}
				if err != nil {
					r.read += int64(total)
					return total, err
				}
				r.adjacentReader = adjacentPage.Values()
			}
			buf := r.adjacentReadBuf(values)
			n, err := r.adjacentReader.ReadValues(buf)
			if n > 0 {
				r.pending = append(r.pending[:0], buf[:n]...)
			}
			if err == io.EOF {
				r.adjacentReader = nil
			} else if err != nil {
				r.read += int64(total)
				return total, err
			} else if n == 0 {
				r.adjacentReader = nil
				continue
			}
		}
		for len(r.pending) > 0 && total < len(values) {
			src := r.pending[0]
			r.pending = r.pending[1:]
			rep := src.repetitionLevel
			if rep > synth.groupSourceRep {
				continue
			}
			def := min(src.definitionLevel, synth.groupSourceDef)
			values[total] = synth.value(synth.mapLevel(synth.repetitionLevels, rep), synth.clampDef(synth.mapLevel(synth.definitionLevels, def)), columnIndex)
			total++
		}
	}
	r.read += int64(total)
	return total, nil
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
