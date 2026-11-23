package thrift

import (
	"reflect"
	"slices"
)

// Slice[T] is a slice type that optimizes thrift decoding allocations.
// Unlike bare slices, Slice[T] implements the Value interface to use
// typed allocation instead of reflect.MakeSlice, avoiding one heap
// allocation per slice decode.
//
// Slice[T] is designed as a drop-in replacement for slice fields in
// thrift structs. It works transparently with standard slice operations.
type Slice[T any] []T

// Type returns the thrift type LIST.
func (l Slice[T]) Type() Type {
	return LIST
}

// EncodeFunc returns an encode function that encodes the slice.
func (l Slice[T]) EncodeFunc(cache EncodeFuncCache) EncodeFunc {
	elemEnc := EncodeFuncFor[T](cache)
	elemType := TypeOf(reflect.TypeFor[T]())

	return func(w Writer, v reflect.Value, flags Flags) error {
		n := v.Len()

		if err := w.WriteList(List{Size: int32(n), Type: elemType}); err != nil {
			return err
		}

		for i := range n {
			if err := elemEnc(w, v.Index(i), flags); err != nil {
				return err
			}
		}
		return nil
	}
}

// NullFunc returns a function that checks if the slice is nil.
// A nil slice is considered unset; an empty slice is a valid empty list.
func (l Slice[T]) NullFunc() NullFunc {
	return func(v reflect.Value) bool {
		return v.IsNil()
	}
}

// DecodeFunc returns a decode function that decodes into the slice
// using typed allocation to avoid reflect.MakeSlice overhead.
func (l Slice[T]) DecodeFunc(cache DecodeFuncCache) DecodeFunc {
	elemDec := DecodeFuncFor[T](cache)
	elemType := TypeOf(reflect.TypeFor[T]())

	return func(r Reader, v reflect.Value, flags Flags) error {
		listHeader, err := r.ReadList()
		if err != nil {
			return err
		}

		// Handle TRUE -> BOOL type conversion
		if listHeader.Type == TRUE {
			listHeader.Type = BOOL
		}

		size := int(listHeader.Size)
		if err := checkCount(r, "list size", size, 1); err != nil {
			return err
		}

		if elemType != listHeader.Type {
			if flags.Have(Strict) {
				return &TypeMismatch{item: "list item", Expect: elemType, Found: listHeader.Type}
			}
			return skipListItems(r, listHeader)
		}

		// Zero-alloc typed allocation via interface-to-pointer conversion.
		// Sizes within the prealloc cap (or already backed by a large enough
		// slice) are allocated once. Larger lists grow as elements are decoded
		// so the declared count is not itself an allocation.
		list := v.Addr().Interface().(*Slice[T])
		flags = flags.Only(decodeFlags)
		if size <= maxContainerPrealloc || cap(*list) >= size {
			if *list == nil {
				*list = make(Slice[T], size)
			} else if cap(*list) >= size {
				*list = (*list)[:size]
			} else {
				*list = slices.Grow((*list)[:0], size)[:size]
			}
			for i := range size {
				if err := elemDec(r, v.Index(i), flags); err != nil {
					return with(dontExpectEOF(err), &decodeErrorList{cause: listHeader, index: i})
				}
			}
			return nil
		}

		hint := maxContainerPrealloc
		if cap(*list) > hint {
			hint = cap(*list)
		}
		if *list == nil {
			*list = make(Slice[T], 0, hint)
		} else {
			*list = slices.Grow((*list)[:0], hint)[:0]
		}
		for i := 0; i < size; i++ {
			if len(*list) == cap(*list) {
				extra := cap(*list)
				if extra < 1 {
					extra = 1
				}
				*list = slices.Grow(*list, extra)
			}
			*list = (*list)[:len(*list)+1]
			if err := elemDec(r, v.Index(i), flags); err != nil {
				return with(dontExpectEOF(err), &decodeErrorList{cause: listHeader, index: i})
			}
		}
		return nil
	}
}
