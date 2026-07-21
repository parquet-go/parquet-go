package parquet

import (
	"fmt"
	"io"
	"slices"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go/format"
	"github.com/parquet-go/parquet-go/internal/unsafecast"
	"github.com/parquet-go/parquet-go/variant"
)

// This file implements columnar copying of VARIANT columns: CopyVariantRows
// bridges a VariantReader (variant_column_reader.go) to a
// VariantColumnWriter (variant_column_writer.go), re-shredding rows from the
// source file's shredding schema to the destination's. It is the core of a
// compaction pipeline: merging many small files into one, changing the
// shredding schema of existing data, or shredding previously unshredded
// files, all without materializing rows. Where the two schemas agree at a
// position, typed values flow between the column buffers without ever being
// decoded to variant binary or boxed into a variant.Value; where they
// disagree, the destination writer re-shreds per its own case tables.
// Writer.WriteRowGroup routes whole row groups through CopyVariantRows
// automatically when shredding differs; that integration lives in
// writer_variant.go.

// variantCopyWindow is the number of rows CopyVariantRows reads from the
// source per window. It bounds the memory buffered by the reader while
// keeping per-window overhead (level walks, buffer resets) amortized.
const variantCopyWindow = 256

// CopyVariantRows copies every remaining row of the variant column read by r
// into w and returns the number of rows copied (including null rows). The
// source and destination shredding schemas are independent: values are
// re-shredded to w's schema as they are copied.
//
// CopyVariantRows drives r's row window itself; cursors it creates project
// every shredded column of the source. Rows are appended to w one at a time,
// so the caller must keep any other columns of the destination file in step
// (or give the destination a schema whose only columns are the variant's).
// On error, rows already copied remain written and w must be abandoned along
// with its parent writer's current row group, as documented on
// VariantColumnWriter.
//
// The variant column should not sit below optional or repeated fields on
// either side: the reader reports a null enclosing level and a null variant
// column both as missing rows, and the writer writes missing rows as null
// variant columns, so enclosing nulls would not round-trip.
func CopyVariantRows(w *VariantColumnWriter, r *VariantReader) (int64, error) {
	cp, err := newVariantCopier(r.Root())
	if err != nil {
		return 0, err
	}
	var n int64
	for {
		k, err := r.Next(variantCopyWindow)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		cp.beginWindow()
		locs := cp.cur.Locs()
		for e := range k {
			if locs[e] == variant.LocMissing {
				err = w.WriteNullRow()
			} else if err = w.BeginRow(); err == nil {
				if err = cp.copyEntry(w, e); err == nil {
					err = w.EndRow()
				}
			}
			if err != nil {
				return n, err
			}
			n++
		}
	}
}

// variantCopier mirrors the source's shredded schema tree with one node per
// cursor, caching what the per-row copy loop needs: the scalar emitter of
// leaf positions and the dense-vector position of the current window.
type variantCopier struct {
	cur        *VariantCursor
	emit       func(w *VariantColumnWriter, c *VariantCursor, d int) error
	fieldNames []string
	fields     []*variantCopier
	elems      *variantCopier

	// skipShredded reports whether a residual field name is covered by the
	// shredded schema, for filtering partial-object leftovers. Built once
	// so the per-row replay does not allocate a closure.
	skipShredded func(name string) bool

	// dense is the index of the next typed value of the current window.
	// Entries are visited in entry order and every variant.LocTyped entry is
	// visited exactly once (typed entries exist only below typed ancestors,
	// which the copy always descends into), so a running counter matches
	// the dense vectors without materializing TypedRows' inverse.
	dense int
}

func newVariantCopier(c *VariantCursor) (*variantCopier, error) {
	cp := &variantCopier{cur: c}
	switch c.Kind() {
	case VariantCursorLeaf:
		emit, err := variantTypedEmitter(c.LeafType())
		if err != nil {
			return nil, err
		}
		cp.emit = emit
	case VariantCursorObject:
		cp.fieldNames = c.Fields()
		cp.fields = make([]*variantCopier, len(cp.fieldNames))
		for i, name := range cp.fieldNames {
			ch, err := newVariantCopier(c.Field(name))
			if err != nil {
				return nil, err
			}
			cp.fields[i] = ch
		}
		cp.skipShredded = func(name string) bool {
			return slices.Contains(cp.fieldNames, name)
		}
	case VariantCursorList:
		ch, err := newVariantCopier(c.Elements())
		if err != nil {
			return nil, err
		}
		cp.elems = ch
	}
	return cp, nil
}

func (cp *variantCopier) beginWindow() {
	cp.dense = 0
	for _, f := range cp.fields {
		f.beginWindow()
	}
	if cp.elems != nil {
		cp.elems.beginWindow()
	}
}

// copyEntry replays the value at entry e of cp's cursor as events on w.
// Callers skip variant.LocMissing entries (missing object fields emit
// nothing, missing rows become WriteNullRow); a Missing entry reaching here
// means the source file is corrupt, and degrades to a variant null.
func (cp *variantCopier) copyEntry(w *VariantColumnWriter, e int) error {
	switch cp.cur.Locs()[e] {
	case variant.LocTyped:
		d := cp.dense
		cp.dense++
		return cp.emit(w, cp.cur, d)

	case variant.LocTypedObject:
		w.BeginObject()
		for i, name := range cp.fieldNames {
			ch := cp.fields[i]
			if ch.cur.Locs()[e] == variant.LocMissing {
				continue
			}
			w.Field(name)
			if err := ch.copyEntry(w, e); err != nil {
				return err
			}
		}
		// Leftover fields of a partially shredded object. Fields whose name
		// is in the shredded schema are ignored like the row-based reader
		// does: the shredded field wins over a non-compliant residual copy.
		// ReplayObjectFields replays nothing if the bytes are not an object
		// (the spec requires an object here).
		if r := cp.cur.residualAt(e); r != nil {
			if r.decoded {
				// Already decoded by the reader for cursor navigation.
				if v := r.val; v.Basic() == variant.BasicObject {
					for _, f := range v.ObjectValue().Fields {
						if cp.skipShredded(f.Name) {
							continue
						}
						w.Field(f.Name)
						f.Value.Write(w)
					}
				}
			} else if r.bytes != nil {
				m, err := cp.cur.reader.metadataFor(cp.cur.rowOf(e))
				if err != nil {
					return err
				}
				if err := variant.ReplayObjectFields(w, m, r.bytes, cp.skipShredded); err != nil {
					return err
				}
			}
		}
		w.EndObject()

	case variant.LocTypedList:
		offsets := cp.cur.ListOffsets()
		w.BeginArray()
		for i := offsets[e]; i < offsets[e+1]; i++ {
			if err := cp.elems.copyEntry(w, int(i)); err != nil {
				return err
			}
		}
		w.EndArray()

	case variant.LocResidual:
		r := cp.cur.residualAt(e)
		switch {
		case r == nil || (!r.decoded && r.bytes == nil):
			// Corruption backstop: entries the reader tagged itself
			// always carry residual state.
			w.Null()
		case r.decoded:
			// Already decoded by the reader for cursor navigation
			// (navigated entries and residuals below cursors with
			// children).
			r.val.Write(w)
		default:
			// Replay the residual bytes as events without materializing a
			// variant.Value; this is the only per-row work of the copy
			// that would otherwise allocate.
			m, err := cp.cur.reader.metadataFor(cp.cur.rowOf(e))
			if err != nil {
				return err
			}
			if err := variant.Replay(w, m, r.bytes); err != nil {
				return err
			}
		}

	default: // variant.LocNull, variant.LocMissing
		w.Null()
	}
	return nil
}

// variantTypedEmitter returns a function that emits the d-th dense typed
// value of a leaf cursor as the matching variant scalar event, per the
// shredded types table (the event-space image of parquetToVariantValue).
func variantTypedEmitter(typ Type) (func(w *VariantColumnWriter, c *VariantCursor, d int) error, error) {
	lt := typ.LogicalType()
	var ltValue format.LogicalTypeValue
	if lt != nil {
		ltValue = lt.Value
	}
	switch lt := ltValue.(type) {
	case nil:
		switch typ.Kind() {
		case Boolean:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Bool(c.Booleans()[d])
				return nil
			}, nil
		case Int32:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int32(c.Int32s()[d])
				return nil
			}, nil
		case Int64:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int64(c.Int64s()[d])
				return nil
			}, nil
		case Float:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Float(c.Floats()[d])
				return nil
			}, nil
		case Double:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Double(c.Doubles()[d])
				return nil
			}, nil
		case ByteArray:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				slab, offsets := c.ByteArrays()
				w.Binary(slab[offsets[d]:offsets[d+1]])
				return nil
			}, nil
		}

	case *format.StringType:
		return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
			slab, offsets := c.ByteArrays()
			// The writer copies the string bytes before the slab is
			// invalidated by the next window, so an unsafe view is fine.
			w.String(unsafecast.String(slab[offsets[d]:offsets[d+1]]))
			return nil
		}, nil

	case *format.IntType:
		switch lt.BitWidth {
		case 8:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int8(int8(c.Int32s()[d]))
				return nil
			}, nil
		case 16:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int16(int16(c.Int32s()[d]))
				return nil
			}, nil
		case 32:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int32(c.Int32s()[d])
				return nil
			}, nil
		case 64:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Int64(c.Int64s()[d])
				return nil
			}, nil
		}

	case *format.DateType:
		return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
			w.Date(c.Int32s()[d])
			return nil
		}, nil

	case *format.TimeType:
		if _, ok := lt.Unit.Value.(*format.MicroSeconds); ok {
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Time(c.Int64s()[d])
				return nil
			}, nil
		}

	case *format.TimestampType:
		utc := lt.IsAdjustedToUTC
		switch lt.Unit.Value.(type) {
		case *format.MicroSeconds:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				if utc {
					w.Timestamp(c.Int64s()[d])
				} else {
					w.TimestampNTZ(c.Int64s()[d])
				}
				return nil
			}, nil
		case *format.NanoSeconds:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				if utc {
					w.TimestampNanos(c.Int64s()[d])
				} else {
					w.TimestampNTZNanos(c.Int64s()[d])
				}
				return nil
			}, nil
		}

	case *format.DecimalType:
		scale := byte(lt.Scale)
		switch typ.Kind() {
		case Int32:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Decimal4(c.Int32s()[d], scale)
				return nil
			}, nil
		case Int64:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				w.Decimal8(c.Int64s()[d], scale)
				return nil
			}, nil
		case FixedLenByteArray:
			if typ.Length() > 16 {
				break // variant decimals are at most 16 bytes
			}
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				slab, size := c.FixedLenByteArrays()
				w.Decimal16(bigEndianToLittleEndian16(slab[d*size:(d+1)*size]), scale)
				return nil
			}, nil
		case ByteArray:
			return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
				slab, offsets := c.ByteArrays()
				b := slab[offsets[d]:offsets[d+1]]
				if len(b) > 16 {
					return fmt.Errorf("variant: decimal value wider than 16 bytes")
				}
				w.Decimal16(bigEndianToLittleEndian16(b), scale)
				return nil
			}, nil
		}

	case *format.UUIDType:
		if typ.Kind() != FixedLenByteArray || typ.Length() != 16 {
			break // UUIDs are FIXED_LEN_BYTE_ARRAY(16)
		}
		return func(w *VariantColumnWriter, c *VariantCursor, d int) error {
			slab, size := c.FixedLenByteArrays()
			w.UUID(uuid.UUID(slab[d*size : (d+1)*size]))
			return nil
		}, nil
	}
	return nil, fmt.Errorf("variant: unsupported shredded leaf type %s", typ)
}
