package parquet

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go/variant"
	"maps"
)

// variantMarshalOrNull marshals a reflect.Value to variant binary. If the
// value is nil/invalid/null, it returns the variant encoding of null.
func variantMarshalOrNull(value reflect.Value) (metadata, val []byte) {
	var goVal any
	if value.IsValid() && !isNullValue(value) {
		v := value
		for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
			if v.IsNil() {
				break
			}
			v = v.Elem()
		}
		if v.IsValid() && v.Kind() != reflect.Ptr && v.Kind() != reflect.Interface {
			goVal = v.Interface()
		}
	}
	metadata, val, err := variant.Marshal(goVal)
	if err != nil {
		panic(fmt.Sprintf("variant marshal: %v", err))
	}
	return metadata, val
}

// isUnshreddedVariant returns true if the variant node has only metadata and value
// (no typed_value field), indicating an unshredded variant.
func isUnshreddedVariant(node Node) bool {
	return fieldByName(node, "typed_value") == nil
}

// deconstructFuncOfVariant handles deconstruction of Go values into variant
// columns. Dispatches to unshredded or shredded variant handling.
//
//go:noinline
func deconstructFuncOfVariant(columnIndex int16, node Node) (int16, deconstructFunc) {
	if isUnshreddedVariant(node) {
		return deconstructFuncOfUnshreddedVariant(columnIndex, node)
	}
	return deconstructFuncOfShreddedVariant(columnIndex, node)
}

// deconstructFuncOfUnshreddedVariant handles the simple 2-column variant:
// metadata (required ByteArray) and value (required ByteArray).
//
//go:noinline
func deconstructFuncOfUnshreddedVariant(columnIndex int16, node Node) (int16, deconstructFunc) {
	metadataColumnIndex := columnIndex
	valueColumnIndex := columnIndex + 1
	nextColumnIndex := columnIndex + 2

	return nextColumnIndex, func(columns [][]Value, levels columnLevels, value reflect.Value) {
		metaIdx := ^metadataColumnIndex
		valIdx := ^valueColumnIndex

		metadata, val := variantMarshalOrNull(value)

		metaValue := makeValueByteArray(ByteArray, unsafe.SliceData(metadata), len(metadata))
		metaValue.repetitionLevel = levels.repetitionLevel
		metaValue.definitionLevel = levels.definitionLevel
		metaValue.columnIndex = metaIdx

		valValue := makeValueByteArray(ByteArray, unsafe.SliceData(val), len(val))
		valValue.repetitionLevel = levels.repetitionLevel
		valValue.definitionLevel = levels.definitionLevel
		valValue.columnIndex = valIdx

		columns[metadataColumnIndex] = append(columns[metadataColumnIndex], metaValue)
		columns[valueColumnIndex] = append(columns[valueColumnIndex], valValue)
	}
}

// deconstructFuncOfShreddedVariant handles the 3-field shredded variant:
// metadata (required), value (optional), typed_value (optional).
//
// When the Go value matches the typed_value schema, the value is written to the
// typed_value columns and the value column is set to null. Otherwise, the variant-
// encoded bytes are written to the value column and typed_value columns are null.
//
//go:noinline
func deconstructFuncOfShreddedVariant(columnIndex int16, node Node) (int16, deconstructFunc) {
	// Fields are sorted alphabetically: metadata, typed_value, value
	var metadataColumnIndex, valueColumnIndex, typedValueStartColumn int16
	var typedValueLeafCount int16
	var typedValueNode Node

	col := columnIndex
	for _, f := range node.Fields() {
		switch f.Name() {
		case "metadata":
			metadataColumnIndex = col
			col += numLeafColumnsOf(f)
		case "typed_value":
			typedValueStartColumn = col
			typedValueNode = f
			typedValueLeafCount = numLeafColumnsOf(f)
			col += typedValueLeafCount
		case "value":
			valueColumnIndex = col
			col += numLeafColumnsOf(f)
		}
	}
	nextColumnIndex := col

	// Determine the shredded type structure
	typedInner := Required(typedValueNode)
	matcher := buildShreddedMatcher(typedInner)

	return nextColumnIndex, func(columns [][]Value, levels columnLevels, value reflect.Value) {
		metaIdx := ^metadataColumnIndex
		valIdx := ^valueColumnIndex

		metadata, val := variantMarshalOrNull(value)

		// Always write metadata
		metaValue := makeValueByteArray(ByteArray, unsafe.SliceData(metadata), len(metadata))
		metaValue.repetitionLevel = levels.repetitionLevel
		metaValue.definitionLevel = levels.definitionLevel
		metaValue.columnIndex = metaIdx
		columns[metadataColumnIndex] = append(columns[metadataColumnIndex], metaValue)

		// Try to shred the value
		goVal := extractGoValue(value)
		if goVal != nil && matcher.canShred(goVal) {
			// Write null to value column (optional, def level not incremented)
			columns[valueColumnIndex] = append(columns[valueColumnIndex], Value{
				repetitionLevel: levels.repetitionLevel,
				definitionLevel: levels.definitionLevel,
				columnIndex:     valIdx,
			})
			// Write typed value (increment def level for optional typed_value wrapper)
			typedLevels := levels
			typedLevels.definitionLevel++
			matcher.shred(columns, typedLevels, typedValueStartColumn, goVal)
		} else {
			// Write variant bytes to value column (increment def level for optional wrapper)
			valueLevels := levels
			valueLevels.definitionLevel++
			valValue := makeValueByteArray(ByteArray, unsafe.SliceData(val), len(val))
			valValue.repetitionLevel = valueLevels.repetitionLevel
			valValue.definitionLevel = valueLevels.definitionLevel
			valValue.columnIndex = valIdx
			columns[valueColumnIndex] = append(columns[valueColumnIndex], valValue)

			// Write null to all typed_value columns
			writeNullLeaves(columns, typedValueStartColumn, typedValueStartColumn+typedValueLeafCount, levels)
		}
	}
}

// extractGoValue extracts the underlying Go value from a reflect.Value,
// dereferencing pointers and interfaces.
func extractGoValue(value reflect.Value) any {
	if !value.IsValid() || isNullValue(value) {
		return nil
	}
	v := value
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	return v.Interface()
}

// writeNullLeaves writes null values to all leaf columns in the range [start, end).
func writeNullLeaves(columns [][]Value, start, end int16, levels columnLevels) {
	for col := start; col < end; col++ {
		columns[col] = append(columns[col], Value{
			repetitionLevel: levels.repetitionLevel,
			definitionLevel: levels.definitionLevel,
			columnIndex:     ^col,
		})
	}
}

// shreddedMatcher determines if a Go value can be shredded into a typed_value
// column and performs the shredding.
type shreddedMatcher struct {
	// For primitive typed_value (leaf node)
	isPrimitive bool
	kind        Kind
	startColumn int16

	// For group typed_value (object node)
	isGroup bool
	fields  []shreddedFieldMatcher
}

type shreddedFieldMatcher struct {
	name    string
	matcher shreddedMatcher
	// Column range for this field's (value, typed_value) pair
	valueColumn     int16
	typedValueStart int16
	typedValueEnd   int16
}

func buildShreddedMatcher(node Node) shreddedMatcher {
	if node.Leaf() {
		return shreddedMatcher{
			isPrimitive: true,
			kind:        node.Type().Kind(),
		}
	}

	// Check if this is a group with (value, typed_value) pairs for each field
	fields := node.Fields()
	matchers := make([]shreddedFieldMatcher, 0, len(fields))
	for _, f := range fields {
		subFields := f.Fields()
		if len(subFields) == 2 {
			// This is a (value, typed_value) pair for a named field
			valueNode := fieldByName(f, "value")
			typedNode := fieldByName(f, "typed_value")
			if valueNode != nil && typedNode != nil {
				matchers = append(matchers, shreddedFieldMatcher{
					name:    f.Name(),
					matcher: buildShreddedMatcher(Required(typedNode)),
				})
			}
		}
	}

	if len(matchers) > 0 {
		return shreddedMatcher{
			isGroup: true,
			fields:  matchers,
		}
	}

	// Unsupported typed_value structure (e.g., LIST). Return a matcher
	// that never shreds so values always go to the value column.
	return shreddedMatcher{}
}

func (m *shreddedMatcher) canShred(v any) bool {
	if v == nil {
		return false
	}
	if m.isPrimitive {
		return canShredPrimitive(v, m.kind)
	}
	if m.isGroup {
		return canShredObject(v, m.fields)
	}
	return false
}

func canShredPrimitive(v any, kind Kind) bool {
	switch kind {
	case ByteArray:
		switch v.(type) {
		case string, []byte:
			return true
		}
	case Int32:
		switch v.(type) {
		case int8, int16, int32, uint8, uint16:
			return true
		}
	case Int64:
		switch v.(type) {
		case int8, int16, int32, int64, int, uint8, uint16, uint32:
			return true
		}
	case Float:
		switch v.(type) {
		case float32:
			return true
		}
	case Double:
		switch v.(type) {
		case float32, float64:
			return true
		}
	case Boolean:
		switch v.(type) {
		case bool:
			return true
		}
	case FixedLenByteArray:
		switch v.(type) {
		case [16]byte:
			return true
		}
	}
	return false
}

func canShredObject(v any, fields []shreddedFieldMatcher) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	// An object can be shredded if ALL its fields exist in the shredded schema
	for key := range m {
		found := false
		for _, f := range fields {
			if f.name == key {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (m *shreddedMatcher) shred(columns [][]Value, levels columnLevels, startColumn int16, v any) {
	if m.isPrimitive {
		col := startColumn
		pv := goValueToParquetValue(v, m.kind)
		pv.repetitionLevel = levels.repetitionLevel
		pv.definitionLevel = levels.definitionLevel
		pv.columnIndex = ^col
		columns[col] = append(columns[col], pv)
		return
	}
	if m.isGroup {
		m.shredObject(columns, levels, startColumn, v)
	}
}

func (m *shreddedMatcher) shredObject(columns [][]Value, levels columnLevels, startColumn int16, v any) {
	obj, ok := v.(map[string]any)
	if !ok {
		// Should not happen if canShred returned true
		return
	}

	col := startColumn
	for _, f := range m.fields {
		// Within each field's sub-group, fields are sorted alphabetically:
		// typed_value comes before value.
		typedValueCol := col
		typedCount := int16(1)
		if f.matcher.isGroup {
			typedCount = countMatcherColumns(&f.matcher)
		}
		valueCol := col + typedCount
		nextFieldCol := valueCol + 1

		fieldVal, exists := obj[f.name]
		if exists && fieldVal != nil && f.matcher.canShred(fieldVal) {
			// Write null to field's value column
			columns[valueCol] = append(columns[valueCol], Value{
				repetitionLevel: levels.repetitionLevel,
				definitionLevel: levels.definitionLevel,
				columnIndex:     ^valueCol,
			})
			// Write typed value (increment def for optional typed_value)
			typedLevels := levels
			typedLevels.definitionLevel++
			f.matcher.shred(columns, typedLevels, typedValueCol, fieldVal)
		} else if exists && fieldVal != nil {
			// Value doesn't match typed schema: write variant to field's value
			valueLevels := levels
			valueLevels.definitionLevel++
			metadata, val, _ := variant.Marshal(fieldVal)
			_ = metadata // Field-level metadata is embedded in value
			valValue := makeValueByteArray(ByteArray, unsafe.SliceData(val), len(val))
			valValue.repetitionLevel = valueLevels.repetitionLevel
			valValue.definitionLevel = valueLevels.definitionLevel
			valValue.columnIndex = ^valueCol
			columns[valueCol] = append(columns[valueCol], valValue)
			// Write null to typed_value columns
			writeNullLeaves(columns, typedValueCol, typedValueCol+typedCount, levels)
		} else {
			// Field not present or null: write null to both
			writeNullLeaves(columns, typedValueCol, nextFieldCol, levels)
		}

		col = nextFieldCol
	}
}

func countMatcherColumns(m *shreddedMatcher) int16 {
	if m.isPrimitive {
		return 1
	}
	if m.isGroup {
		var count int16
		for _, f := range m.fields {
			count += 1 + countMatcherColumns(&f.matcher) // value + typed_value
		}
		return count
	}
	return 1
}

func goValueToParquetValue(v any, kind Kind) Value {
	switch kind {
	case Boolean:
		return BooleanValue(v.(bool))
	case Int32:
		switch val := v.(type) {
		case int8:
			return Int32Value(int32(val))
		case int16:
			return Int32Value(int32(val))
		case int32:
			return Int32Value(val)
		case uint8:
			return Int32Value(int32(val))
		case uint16:
			return Int32Value(int32(val))
		default:
			return Int32Value(0)
		}
	case Int64:
		switch val := v.(type) {
		case int8:
			return Int64Value(int64(val))
		case int16:
			return Int64Value(int64(val))
		case int32:
			return Int64Value(int64(val))
		case int64:
			return Int64Value(val)
		case int:
			return Int64Value(int64(val))
		case uint8:
			return Int64Value(int64(val))
		case uint16:
			return Int64Value(int64(val))
		case uint32:
			return Int64Value(int64(val))
		default:
			return Int64Value(0)
		}
	case Float:
		return FloatValue(v.(float32))
	case Double:
		switch val := v.(type) {
		case float32:
			return DoubleValue(float64(val))
		case float64:
			return DoubleValue(val)
		default:
			return DoubleValue(0)
		}
	case ByteArray:
		switch val := v.(type) {
		case string:
			return ByteArrayValue([]byte(val))
		case []byte:
			return ByteArrayValue(val)
		default:
			return ByteArrayValue(nil)
		}
	case FixedLenByteArray:
		switch val := v.(type) {
		case [16]byte:
			return FixedLenByteArrayValue(val[:])
		default:
			return FixedLenByteArrayValue(nil)
		}
	default:
		return Value{}
	}
}

// reconstructFuncOfVariant handles reconstruction of variant columns back into
// Go values. Dispatches to unshredded or shredded variant handling.
//
//go:noinline
func reconstructFuncOfVariant(columnIndex int16, node Node) (int16, reconstructFunc) {
	if isUnshreddedVariant(node) {
		return reconstructFuncOfUnshreddedVariant(columnIndex, node)
	}
	return reconstructFuncOfShreddedVariant(columnIndex, node)
}

// reconstructFuncOfUnshreddedVariant handles the simple 2-column variant.
//
//go:noinline
func reconstructFuncOfUnshreddedVariant(columnIndex int16, node Node) (int16, reconstructFunc) {
	nextColumnIndex := columnIndex + 2

	return nextColumnIndex, func(value reflect.Value, levels columnLevels, columns [][]Value) error {
		if len(columns) < 2 {
			return fmt.Errorf("variant reconstruction: expected 2 columns, got %d", len(columns))
		}

		metaColumn := columns[0]
		valColumn := columns[1]

		if len(metaColumn) == 0 || len(valColumn) == 0 {
			return fmt.Errorf("variant reconstruction: no values in columns for column %d", columnIndex)
		}

		metaVal := metaColumn[0]
		valVal := valColumn[0]

		if metaVal.IsNull() || valVal.IsNull() {
			value.Set(reflect.Zero(value.Type()))
			return nil
		}

		metaBytes := metaVal.ByteArray()
		valBytes := valVal.ByteArray()

		goVal, err := variant.Unmarshal(metaBytes, valBytes)
		if err != nil {
			return fmt.Errorf("variant unmarshal: %w", err)
		}

		return setVariantGoValue(value, goVal)
	}
}

// reconstructFuncOfShreddedVariant handles the 3-field shredded variant.
//
//go:noinline
func reconstructFuncOfShreddedVariant(columnIndex int16, node Node) (int16, reconstructFunc) {
	// Fields are sorted alphabetically: metadata, typed_value, value
	var metadataOffset, valueOffset, typedValueOffset int16
	var typedValueLeafCount int16
	var typedValueNode Node

	col := int16(0)
	for _, f := range node.Fields() {
		switch f.Name() {
		case "metadata":
			metadataOffset = col
			col += numLeafColumnsOf(f)
		case "typed_value":
			typedValueOffset = col
			typedValueNode = f
			typedValueLeafCount = numLeafColumnsOf(f)
			col += typedValueLeafCount
		case "value":
			valueOffset = col
			col += numLeafColumnsOf(f)
		}
	}
	totalCols := int(col)
	nextColumnIndex := columnIndex + col

	typedInner := Required(typedValueNode)
	extractor := buildShreddedExtractor(typedInner)

	return nextColumnIndex, func(value reflect.Value, levels columnLevels, columns [][]Value) error {
		if len(columns) < totalCols {
			return fmt.Errorf("shredded variant reconstruction: expected %d columns, got %d", totalCols, len(columns))
		}

		metaCol := columns[metadataOffset]
		valCol := columns[valueOffset]
		typedColumns := columns[typedValueOffset : typedValueOffset+typedValueLeafCount]

		if len(metaCol) == 0 {
			return fmt.Errorf("variant reconstruction: no values in metadata column %d", columnIndex)
		}

		metaVal := metaCol[0]
		hasValue := len(valCol) > 0 && !valCol[0].IsNull()
		hasTypedValue := hasNonNullInColumns(typedColumns)

		if metaVal.IsNull() && !hasValue && !hasTypedValue {
			// All null: missing/null
			value.Set(reflect.Zero(value.Type()))
			return nil
		}

		if hasValue && !hasTypedValue {
			// Unshredded value
			valBytes := valCol[0].ByteArray()
			metaBytes := metaVal.ByteArray()
			goVal, err := variant.Unmarshal(metaBytes, valBytes)
			if err != nil {
				return fmt.Errorf("variant unmarshal: %w", err)
			}
			return setVariantGoValue(value, goVal)
		}

		if !hasValue && hasTypedValue {
			// Shredded value: extract from typed columns
			goVal := extractor.extract(typedColumns)
			return setVariantGoValue(value, goVal)
		}

		if hasValue && hasTypedValue {
			// Partially shredded: typed_value has some fields, value has the rest
			typedVal := extractor.extract(typedColumns)
			valBytes := valCol[0].ByteArray()
			metaBytes := metaVal.ByteArray()
			unshreddedVal, err := variant.Unmarshal(metaBytes, valBytes)
			if err != nil {
				return fmt.Errorf("variant unmarshal: %w", err)
			}
			merged := mergeVariantObjects(typedVal, unshreddedVal)
			return setVariantGoValue(value, merged)
		}

		value.Set(reflect.Zero(value.Type()))
		return nil
	}
}

func hasNonNullInColumns(columns [][]Value) bool {
	for _, col := range columns {
		if len(col) > 0 && !col[0].IsNull() {
			return true
		}
	}
	return false
}

func setVariantGoValue(value reflect.Value, goVal any) error {
	if goVal == nil {
		value.Set(reflect.Zero(value.Type()))
		return nil
	}
	goValue := reflect.ValueOf(goVal)
	if value.Kind() == reflect.Interface {
		value.Set(goValue)
	} else if goValue.Type().AssignableTo(value.Type()) {
		value.Set(goValue)
	} else if goValue.Type().ConvertibleTo(value.Type()) {
		value.Set(goValue.Convert(value.Type()))
	} else {
		value.Set(goValue)
	}
	return nil
}

// shreddedExtractor reconstructs Go values from typed_value columns.
type shreddedExtractor struct {
	isPrimitive bool
	kind        Kind

	isGroup bool
	fields  []shreddedFieldExtractor
}

type shreddedFieldExtractor struct {
	name      string
	extractor shreddedExtractor
}

func buildShreddedExtractor(node Node) shreddedExtractor {
	if node.Leaf() {
		return shreddedExtractor{
			isPrimitive: true,
			kind:        node.Type().Kind(),
		}
	}

	fields := node.Fields()
	extractors := make([]shreddedFieldExtractor, 0, len(fields))
	for _, f := range fields {
		subFields := f.Fields()
		if len(subFields) == 2 {
			typedNode := fieldByName(f, "typed_value")
			if typedNode != nil {
				extractors = append(extractors, shreddedFieldExtractor{
					name:      f.Name(),
					extractor: buildShreddedExtractor(Required(typedNode)),
				})
			}
		}
	}

	if len(extractors) > 0 {
		return shreddedExtractor{
			isGroup: true,
			fields:  extractors,
		}
	}

	// Unsupported typed_value structure (e.g., LIST). Return an extractor
	// that always returns nil so values are read from the value column.
	return shreddedExtractor{}
}

func (e *shreddedExtractor) extract(columns [][]Value) any {
	if e.isPrimitive {
		if len(columns) == 0 || len(columns[0]) == 0 || columns[0][0].IsNull() {
			return nil
		}
		return parquetValueToGo(columns[0][0], e.kind)
	}
	if e.isGroup {
		return e.extractObject(columns)
	}
	return nil
}

func (e *shreddedExtractor) extractObject(columns [][]Value) any {
	result := make(map[string]any)
	col := 0
	for _, f := range e.fields {
		// Within each field's sub-group, fields are sorted alphabetically:
		// typed_value comes before value.
		typedStart := col
		typedCount := 1
		if f.extractor.isGroup {
			typedCount = int(countExtractorColumns(&f.extractor))
		}
		valueCol := col + typedCount
		nextFieldCol := valueCol + 1

		hasValue := valueCol < len(columns) && len(columns[valueCol]) > 0 && !columns[valueCol][0].IsNull()
		hasTyped := hasNonNullInRange(columns, typedStart, typedStart+typedCount)

		if hasTyped {
			val := f.extractor.extract(columns[typedStart : typedStart+typedCount])
			if val != nil {
				result[f.name] = val
			}
		} else if hasValue {
			// Field stored as unshredded variant
			valBytes := columns[valueCol][0].ByteArray()
			// For field-level values, the value is variant-encoded without separate metadata
			// We need to decode with an empty metadata (no field names needed for leaf values)
			m := variant.Metadata{}
			v, err := variant.Decode(m, valBytes)
			if err == nil {
				result[f.name] = v.GoValue()
			}
		}

		col = nextFieldCol
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func countExtractorColumns(e *shreddedExtractor) int16 {
	if e.isPrimitive {
		return 1
	}
	if e.isGroup {
		var count int16
		for _, f := range e.fields {
			count += 1 + countExtractorColumns(&f.extractor)
		}
		return count
	}
	return 1
}

func hasNonNullInRange(columns [][]Value, start, end int) bool {
	for i := start; i < end && i < len(columns); i++ {
		if len(columns[i]) > 0 && !columns[i][0].IsNull() {
			return true
		}
	}
	return false
}

func parquetValueToGo(v Value, kind Kind) any {
	switch kind {
	case Boolean:
		return v.Boolean()
	case Int32:
		return v.Int32()
	case Int64:
		return v.Int64()
	case Float:
		return v.Float()
	case Double:
		return v.Double()
	case ByteArray:
		return string(v.ByteArray())
	case FixedLenByteArray:
		b := v.ByteArray()
		if len(b) == 16 {
			var u [16]byte
			copy(u[:], b)
			return u
		}
		return b
	default:
		return nil
	}
}

// mergeVariantObjects merges two values, preferring typed (shredded) values
// over unshredded ones. Used for partially shredded objects.
func mergeVariantObjects(typed, unshredded any) any {
	typedMap, tOk := typed.(map[string]any)
	unshreddedMap, uOk := unshredded.(map[string]any)
	if tOk && uOk {
		result := make(map[string]any, len(typedMap)+len(unshreddedMap))
		maps.Copy(result, unshreddedMap)
		maps.Copy(result, typedMap)
		return result
	}
	if tOk {
		return typed
	}
	return unshredded
}
