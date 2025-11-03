package parquet

import (
	"cmp"
	"math/bits"
	"reflect"
	"sort"
	"unsafe"

	"github.com/parquet-go/parquet-go/sparse"
)

type anymap interface {
	entries() (keys, values sparse.Array)
}

type gomap[K cmp.Ordered] struct {
	keys []K
	vals reflect.Value // slice
	swap func(int, int)
	size uintptr
}

func (m *gomap[K]) Len() int { return len(m.keys) }

func (m *gomap[K]) Less(i, j int) bool { return cmp.Compare(m.keys[i], m.keys[j]) < 0 }

func (m *gomap[K]) Swap(i, j int) {
	m.keys[i], m.keys[j] = m.keys[j], m.keys[i]
	m.swap(i, j)
}

func (m *gomap[K]) entries() (keys, values sparse.Array) {
	return makeArrayFromSlice(m.keys), makeArray(m.vals.UnsafePointer(), m.Len(), m.size)
}

type reflectMap struct {
	keys    reflect.Value // slice
	vals    reflect.Value // slice
	numKeys int
	keySize uintptr
	valSize uintptr
}

func (m *reflectMap) entries() (keys, values sparse.Array) {
	return makeArray(m.keys.UnsafePointer(), m.numKeys, m.keySize), makeArray(m.vals.UnsafePointer(), m.numKeys, m.valSize)
}

func makeMapFuncOf(mapType reflect.Type) func(reflect.Value) anymap {
	switch mapType.Key().Kind() {
	case reflect.Int:
		return makeMapFunc[int](mapType)
	case reflect.Int8:
		return makeMapFunc[int8](mapType)
	case reflect.Int16:
		return makeMapFunc[int16](mapType)
	case reflect.Int32:
		return makeMapFunc[int32](mapType)
	case reflect.Int64:
		return makeMapFunc[int64](mapType)
	case reflect.Uint:
		return makeMapFunc[uint](mapType)
	case reflect.Uint8:
		return makeMapFunc[uint8](mapType)
	case reflect.Uint16:
		return makeMapFunc[uint16](mapType)
	case reflect.Uint32:
		return makeMapFunc[uint32](mapType)
	case reflect.Uint64:
		return makeMapFunc[uint64](mapType)
	case reflect.Uintptr:
		return makeMapFunc[uintptr](mapType)
	case reflect.Float32:
		return makeMapFunc[float32](mapType)
	case reflect.Float64:
		return makeMapFunc[float64](mapType)
	case reflect.String:
		return makeMapFunc[string](mapType)
	}

	keyType := mapType.Key()
	valType := mapType.Elem()

	mapBuffer := &reflectMap{
		keySize: keyType.Size(),
		valSize: valType.Size(),
	}

	keySliceType := reflect.SliceOf(keyType)
	valSliceType := reflect.SliceOf(valType)
	return func(mapValue reflect.Value) anymap {
		length := mapValue.Len()

		if !mapBuffer.keys.IsValid() || mapBuffer.keys.Len() < length {
			capacity := 1 << bits.Len(uint(length))
			mapBuffer.keys = reflect.MakeSlice(keySliceType, capacity, capacity)
			mapBuffer.vals = reflect.MakeSlice(valSliceType, capacity, capacity)
		}

		mapBuffer.numKeys = length
		for i, mapIter := 0, mapValue.MapRange(); mapIter.Next(); i++ {
			mapBuffer.keys.Index(i).SetIterKey(mapIter)
			mapBuffer.vals.Index(i).SetIterValue(mapIter)
		}

		return mapBuffer
	}
}

func makeMapFunc[K cmp.Ordered](mapType reflect.Type) func(reflect.Value) anymap {
	keyType := mapType.Key()
	valType := mapType.Elem()
	valSliceType := reflect.SliceOf(valType)
	mapBuffer := &gomap[K]{size: valType.Size()}
	return func(mapValue reflect.Value) anymap {
		length := mapValue.Len()

		if cap(mapBuffer.keys) < length {
			capacity := 1 << bits.Len(uint(length))
			mapBuffer.keys = make([]K, capacity)
			mapBuffer.vals = reflect.MakeSlice(valSliceType, capacity, capacity)
			mapBuffer.swap = reflect.Swapper(mapBuffer.vals.Interface())
		}

		mapBuffer.keys = mapBuffer.keys[:length]
		for i, mapIter := 0, mapValue.MapRange(); mapIter.Next(); i++ {
			reflect.NewAt(keyType, unsafe.Pointer(&mapBuffer.keys[i])).Elem().SetIterKey(mapIter)
			mapBuffer.vals.Index(i).SetIterValue(mapIter)
		}

		sort.Sort(mapBuffer)
		return mapBuffer
	}
}

// writeValueFunc is a function that writes a single reflect.Value to a set of column buffers.
// Panics if the value cannot be written (similar to reflect package behavior).
type writeValueFunc func([]ColumnBuffer, columnLevels, reflect.Value)

// writeValueFuncOf constructs a function that writes reflect.Values to column buffers.
// It follows the deconstructFuncOf pattern, recursively building functions for the schema tree.
// Returns (nextColumnIndex, writeFunc).
func writeValueFuncOf(columnIndex int16, node Node) (int16, writeValueFunc) {
	switch {
	case node.Optional():
		return writeValueFuncOfOptional(columnIndex, node)
	case node.Repeated():
		return writeValueFuncOfRepeated(columnIndex, node)
	case isList(node):
		return writeValueFuncOfList(columnIndex, node)
	case isMap(node):
		return writeValueFuncOfMap(columnIndex, node)
	default:
		return writeValueFuncOfRequired(columnIndex, node)
	}
}

func writeValueFuncOfOptional(columnIndex int16, node Node) (int16, writeValueFunc) {
	nextColumnIndex, writeValue := writeValueFuncOf(columnIndex, Required(node))
	return nextColumnIndex, func(columns []ColumnBuffer, levels columnLevels, value reflect.Value) {
		// Handle invalid values
		if !value.IsValid() {
			writeValue(columns, levels, value)
			return
		}

		// Check for nil pointers/interfaces
		switch value.Kind() {
		case reflect.Pointer, reflect.Interface:
			if value.IsNil() {
				// Nil pointer/interface - write as null
				writeValue(columns, levels, value)
				return
			}
			// Non-nil - unwrap and increment definition level
			value = value.Elem()
			levels.definitionLevel++
			writeValue(columns, levels, value)
			return
		}

		// For other types, check if zero value
		if value.IsZero() {
			// Zero value - write as null
			writeValue(columns, levels, value)
			return
		}

		// Non-zero value - increment definition level
		levels.definitionLevel++
		writeValue(columns, levels, value)
	}
}

func writeValueFuncOfRepeated(columnIndex int16, node Node) (int16, writeValueFunc) {
	nextColumnIndex, writeValue := writeValueFuncOf(columnIndex, Required(node))
	return nextColumnIndex, func(columns []ColumnBuffer, levels columnLevels, value reflect.Value) {
		if value.Kind() == reflect.Interface {
			value = value.Elem()
		}

		if !value.IsValid() || value.Len() == 0 {
			writeValue(columns, levels, value)
			return
		}

		levels.repetitionDepth++
		levels.definitionLevel++
		writeValue(columns, levels, value.Index(0))

		if n := value.Len(); n > 1 {
			levels.repetitionLevel = levels.repetitionDepth
			for i := 1; i < n; i++ {
				writeValue(columns, levels, value.Index(i))
			}
		}
	}
}

func writeValueFuncOfRequired(columnIndex int16, node Node) (int16, writeValueFunc) {
	switch {
	case node.Leaf():
		return writeValueFuncOfLeaf(columnIndex, node)
	default:
		return writeValueFuncOfGroup(columnIndex, node)
	}
}

func writeValueFuncOfList(columnIndex int16, node Node) (int16, writeValueFunc) {
	return writeValueFuncOf(columnIndex, Repeated(listElementOf(node)))
}

func writeValueFuncOfMap(columnIndex int16, node Node) (int16, writeValueFunc) {
	keyValue := mapKeyValueOf(node)
	keyValueType := keyValue.GoType()
	keyValueElem := keyValueType.Elem()
	keyType := keyValueElem.Field(0).Type
	valueType := keyValueElem.Field(1).Type
	nextColumnIndex, writeValue := writeValueFuncOf(columnIndex, schemaOf(keyValueElem))

	return nextColumnIndex, func(columns []ColumnBuffer, levels columnLevels, mapValue reflect.Value) {
		if mapValue.Len() == 0 {
			writeValue(columns, levels, reflect.Zero(keyValueElem))
			return
		}

		levels.repetitionDepth++
		levels.definitionLevel++

		elem := reflect.New(keyValueElem).Elem()
		k := elem.Field(0)
		v := elem.Field(1)

		for _, key := range mapValue.MapKeys() {
			k.Set(key.Convert(keyType))
			v.Set(mapValue.MapIndex(key).Convert(valueType))
			writeValue(columns, levels, elem)
			levels.repetitionLevel = levels.repetitionDepth
		}
	}
}

func writeValueFuncOfGroup(columnIndex int16, node Node) (int16, writeValueFunc) {
	type fieldWriter struct {
		fieldName  string
		writeValue writeValueFunc
	}

	fields := node.Fields()
	writers := make([]fieldWriter, len(fields))
	for i, field := range fields {
		writers[i].fieldName = field.Name()
		columnIndex, writers[i].writeValue = writeValueFuncOf(columnIndex, field)
	}

	// Pre-compute type information for common map types
	mapStringStringType := reflect.TypeOf((map[string]string)(nil))
	mapStringAnyType := reflect.TypeOf((map[string]any)(nil))

	return columnIndex, func(columns []ColumnBuffer, levels columnLevels, value reflect.Value) {
		for {
			if !value.IsValid() {
				for i := range writers {
					w := &writers[i]
					w.writeValue(columns, levels, reflect.Value{})
				}
				return
			}

			switch t := value.Type(); t.Kind() {
			case reflect.Map:
				switch {
				case t.ConvertibleTo(mapStringStringType):
					m := value.Convert(mapStringStringType).Interface().(map[string]string)
					v := new(string)
					for i := range writers {
						w := &writers[i]
						*v = m[w.fieldName]
						w.writeValue(columns, levels, reflect.ValueOf(v).Elem())
					}

				case t.ConvertibleTo(mapStringAnyType):
					m := value.Convert(mapStringAnyType).Interface().(map[string]any)
					for i := range writers {
						w := &writers[i]
						v := m[w.fieldName]
						w.writeValue(columns, levels, reflect.ValueOf(v))
					}

				default:
					for i := range writers {
						w := &writers[i]
						fieldName := reflect.ValueOf(&w.fieldName).Elem()
						fieldValue := value.MapIndex(fieldName)
						w.writeValue(columns, levels, fieldValue)
					}
				}
				return

			case reflect.Struct:
				for i := range writers {
					w := &writers[i]
					fieldValue := value.FieldByName(w.fieldName)
					w.writeValue(columns, levels, fieldValue)
				}
				return

			case reflect.Pointer, reflect.Interface:
				if value.IsNil() {
					value = reflect.Value{}
				} else {
					value = value.Elem()
				}

			default:
				value = reflect.Value{}
			}
		}
	}
}

func writeValueFuncOfLeaf(columnIndex int16, node Node) (int16, writeValueFunc) {
	if columnIndex > MaxColumnIndex {
		panic("row cannot be written because it has more than 127 columns")
	}

	return columnIndex + 1, func(columns []ColumnBuffer, levels columnLevels, value reflect.Value) {
		// Unwrap interface{} values before passing to writeReflectValue
		if value.IsValid() && value.Kind() == reflect.Interface {
			value = value.Elem()
		}
		columns[columnIndex].writeReflectValue(levels, value)
	}
}
