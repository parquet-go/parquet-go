package parquet

import (
	"fmt"

	"github.com/parquet-go/parquet-go/variant"
)

// This file teaches schema conversion to re-shred variant columns. When the
// target and source schemas declare the same variant column with different
// shredding (including unshredded on either side), the conversion
// reconstructs each value from the source columns with the
// construct_variant algorithm (see variant_shredded_read.go) and re-shreds
// it into the target columns (see variant_shredded_write.go). Without this,
// the generic column mapping would silently drop the typed_value columns of
// the source, losing every shredded value.
//
// Because every read path funnels through Convert when the reader and file
// schemas differ, this single integration point makes shredded files
// readable by readers declaring any other variant shape, and makes
// MergeRowGroups correct when the inputs disagree on shredding.

// variantConversion re-shreds one variant column during row conversion.
type variantConversion struct {
	// A source group that cannot be reconstructed (e.g. an unsupported
	// shredded type) fails when rows are read, not when the conversion is
	// constructed, matching how readers report other malformed files.
	err error

	source *shreddedVariantGroup
	// sourceValueRequired marks plain unshredded sources (required binary
	// value), whose value column does not contribute a definition level.
	sourceValueRequired bool
	numCols             int // leaf columns of the source variant group
	sourceStart         int // first source leaf column of the variant group

	target *shreddedVariantGroup
	// targetValueRequired marks plain unshredded targets.
	targetValueRequired bool
	targetStart         int  // first target leaf column of the variant group
	targetNumCols       int  // leaf columns of the target variant group
	targetDef           byte // definition level of the target group when present
	targetRep           byte // repetition depth of the target group

	sourceGroupDef int // definition level at which the source group is present
	sourceGroupRep int // repetition level of the source group

	// Source-to-target level mappings for the path down to the variant
	// group, used for occurrences where the group is null because of a
	// null enclosing level.
	defLevels []byte
	repLevels []byte
}

// variantNodesEquivalent reports whether two variant group nodes have the
// same shredding, ignoring the repetition of the group node itself and
// field order. Equivalent groups convert through the generic column
// mapping; anything else needs re-shredding.
func variantNodesEquivalent(a, b Node) bool {
	return SameNodes(Required(a), Required(b))
}

// findVariantConversions walks the target and source schemas in parallel
// and returns a conversion for every variant column whose shredding differs
// between the two.
func findVariantConversions(to, from Node) []variantConversion {
	var conversions []variantConversion
	walkVariantConversions(to, from, 0, 0, 0, 0, 0, 0, []byte{0}, []byte{0}, &conversions)
	return conversions
}

func walkVariantConversions(t, s Node, tCol, sCol int, tRep, tDef, sRep, sDef byte, repLevels, defLevels []byte, conversions *[]variantConversion) {
	if isVariant(t) && isVariant(s) {
		if variantNodesEquivalent(t, s) {
			return // the generic column mapping handles it
		}
		if vc := makeVariantConversion(t, s, tCol, sCol, tDef, tRep, sDef, sRep, repLevels, defLevels); vc != nil {
			*conversions = append(*conversions, *vc)
		}
		return
	}
	if t.Leaf() || s.Leaf() {
		return
	}
	sColOf := make(map[string]int, len(s.Fields()))
	sFieldOf := make(map[string]Field, len(s.Fields()))
	col := sCol
	for _, sf := range s.Fields() {
		sColOf[sf.Name()] = col
		sFieldOf[sf.Name()] = sf
		col += int(numLeafColumnsOf(sf))
	}
	for _, tf := range t.Fields() {
		if sf, ok := sFieldOf[tf.Name()]; ok {
			tr, td := applyFieldRepetitionType(fieldRepetitionTypeOf(tf), tRep, tDef)
			sr, sd := applyFieldRepetitionType(fieldRepetitionTypeOf(sf), sRep, sDef)
			walkVariantConversions(
				tf, sf,
				tCol, sColOf[tf.Name()],
				tr, td, sr, sd,
				setLevelMapping(repLevels, sr, tr),
				setLevelMapping(defLevels, sd, td),
				conversions,
			)
		}
		tCol += int(numLeafColumnsOf(tf))
	}
}

// makeVariantConversion builds the conversion for one variant column. The
// level arguments are those of the variant group nodes themselves. A nil
// result means the target group does not have a recognizable variant shape
// and the generic column mapping applies.
func makeVariantConversion(t, s Node, tCol, sCol int, tDef, tRep, sDef, sRep byte, repLevels, defLevels []byte) *variantConversion {
	target, targetValueRequired, err := variantGroupOf(t)
	if err != nil {
		return nil
	}
	vc := &variantConversion{
		target:              target,
		targetValueRequired: targetValueRequired,
		targetStart:         tCol,
		targetNumCols:       target.numCols,
		targetDef:           tDef,
		targetRep:           tRep,
		numCols:             int(numLeafColumnsOf(s)),
		sourceStart:         sCol,
		sourceGroupDef:      int(sDef),
		sourceGroupRep:      int(sRep),
		defLevels:           defLevels,
		repLevels:           repLevels,
	}
	vc.source, vc.sourceValueRequired, vc.err = variantGroupOf(s)
	return vc
}

// sourceColumnFor returns the source leaf column backing the given relative
// target column, used to keep the conversion's chunk mapping meaningful.
// The metadata column maps to the source metadata; every other target
// column maps to the source value column (or metadata when there is none).
func (vc *variantConversion) sourceColumnFor(rel int) int {
	if vc.source == nil {
		return vc.sourceStart
	}
	if rel == vc.target.metadataCol {
		return vc.sourceStart + vc.source.metadataCol
	}
	if vc.source.valueCol >= 0 {
		return vc.sourceStart + vc.source.valueCol
	}
	return vc.sourceStart + vc.source.metadataCol
}

// setLevelMapping returns a copy of the source-to-target level mapping with
// the entry for the source level set, extending the table when the source
// level is new (each schema step adds at most one level).
func setLevelMapping(levels []byte, source, target byte) []byte {
	mapping := make([]byte, max(len(levels), int(source)+1))
	copy(mapping, levels)
	mapping[source] = target
	return mapping
}

// variantScratch is the per-row state of one variant conversion, pooled in
// conversionBuffer so converting a row does not allocate it anew. cols
// holds the output values of every leaf column of the target variant group.
type variantScratch struct {
	pos  []int
	cols [][]Value
	meta variant.MetadataBuilder
}

// convert re-shreds every occurrence of the variant column in the current
// row from the source columns into the target columns, leaving the output
// values in the scratch space.
func (vc *variantConversion) convert(source [][]Value, scratch *variantScratch) error {
	if vc.err != nil {
		return vc.err
	}
	columns := source[vc.sourceStart : vc.sourceStart+vc.numCols]
	if cap(scratch.pos) < len(columns) {
		scratch.pos = make([]int, len(columns))
	}
	scratch.pos = scratch.pos[:len(columns)]
	clear(scratch.pos)
	if cap(scratch.cols) < vc.targetNumCols {
		scratch.cols = make([][]Value, vc.targetNumCols)
	}
	scratch.cols = scratch.cols[:vc.targetNumCols]
	for i := range scratch.cols {
		scratch.cols[i] = scratch.cols[i][:0]
	}
	reader := variantColumnReader{columns: columns, pos: scratch.pos}

	for {
		metaVal, ok := reader.peek(vc.source.metadataCol)
		if !ok {
			break
		}

		// The level mapping tables cover the levels the schema can
		// produce; corrupt files can carry level bytes beyond them, which
		// must surface as a decode error rather than a panic.
		if int(metaVal.repetitionLevel) >= len(vc.repLevels) || int(metaVal.definitionLevel) >= len(vc.defLevels) {
			return fmt.Errorf("variant: metadata column has levels (r=%d, d=%d) outside the schema's levels",
				metaVal.repetitionLevel, metaVal.definitionLevel)
		}
		rep := vc.repLevels[metaVal.repetitionLevel]

		if int(metaVal.definitionLevel) < vc.sourceGroupDef {
			// The variant group is null at this occurrence; every leaf
			// column of the group carries exactly one value at the
			// enclosing level to consume.
			def := vc.defLevels[metaVal.definitionLevel]
			for col := range scratch.cols {
				scratch.cols[col] = append(scratch.cols[col], Value{repetitionLevel: rep, definitionLevel: def})
			}
			for col := range columns {
				reader.next(col)
			}
			continue
		}

		reader.next(vc.source.metadataCol)

		var v variant.Value
		if vc.sourceValueRequired {
			// Plain unshredded source: the required value column holds the
			// variant binary directly.
			val, ok := reader.next(vc.source.valueCol)
			if !ok {
				return fmt.Errorf("variant: value column has fewer values than metadata")
			}
			if b := val.ByteArray(); len(b) == 0 {
				// The plain unshredded write path stores a zero variant
				// value as empty metadata and value bytes; read it as
				// variant null like the row-based reader.
				v = variant.Null()
			} else {
				m, err := variant.DecodeMetadata(metaVal.ByteArray())
				if err != nil {
					return fmt.Errorf("variant metadata: %w", err)
				}
				if v, err = variant.Decode(m, b); err != nil {
					return fmt.Errorf("variant: decoding value: %w", err)
				}
			}
		} else {
			m, err := variant.DecodeMetadata(metaVal.ByteArray())
			if err != nil {
				return fmt.Errorf("variant metadata: %w", err)
			}
			var present bool
			v, present, err = vc.source.read(&reader, m, vc.sourceGroupDef, vc.sourceGroupRep)
			if err != nil {
				return err
			}
			if !present {
				// value and typed_value both null at the top level is
				// invalid per the spec; read it as variant null (matching
				// parquet-java).
				v = variant.Null()
			}
		}

		vc.emit(v, rep, scratch)
	}
	if len(scratch.cols[0]) == 0 {
		// No values in the source columns for this row; emit a null per
		// target column to maintain the one-value-per-column invariant.
		for col := range scratch.cols {
			scratch.cols[col] = append(scratch.cols[col], Value{})
		}
	}
	for col := range scratch.cols {
		idx := ^uint16(vc.targetStart + col)
		values := scratch.cols[col]
		for i := range values {
			values[i].columnIndex = idx
		}
	}
	return nil
}

// emit re-shreds one occurrence of the variant value into the scratch
// columns of the target group, mirroring the row-based shredded write path.
func (vc *variantConversion) emit(v variant.Value, rep byte, scratch *variantScratch) {
	b := &scratch.meta
	b.Reset()
	if vc.targetValueRequired {
		// Plain unshredded target: encode the whole value into the
		// required value column.
		encoded := variant.Encode(b, v)
		appendShredded(scratch.cols, 0, vc.target.valueCol, ByteArrayValue(encoded), vc.targetDef, rep)
	} else {
		// The row's metadata dictionary contains every object field name
		// of the value, shredded or not (VariantShredding.md).
		addVariantFieldNames(b, v)
		vc.target.write(scratch.cols, 0, b, v, true, shredLevels{
			baseDef: int(vc.targetDef),
			baseRep: int(vc.targetRep),
			rep:     rep,
		})
	}
	// The metadata column is written last so the dictionary includes the
	// field IDs assigned while encoding residual values.
	_, metadata := b.Build()
	appendShredded(scratch.cols, 0, vc.target.metadataCol, ByteArrayValue(metadata), vc.targetDef, rep)
}
