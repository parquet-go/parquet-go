package parquet_test

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestConvertTimestampUnits(t *testing.T) {
	units := []parquet.TimeUnit{parquet.Millisecond, parquet.Microsecond, parquet.Nanosecond}
	for _, fromUnit := range units {
		for _, toUnit := range units {
			from := parquet.Timestamp(fromUnit).Type()
			to := parquet.Timestamp(toUnit).Type()
			fromScale := int64(fromUnit.Duration())
			toScale := int64(toUnit.Duration())
			t.Run(from.String()+" to "+to.String(), func(t *testing.T) {
				factor := int64(1)
				if fromScale > toScale {
					factor = fromScale / toScale
				}
				for _, v := range timestampUnitValues(factor) {
					got, err := to.ConvertValue(parquet.Int64Value(v).Level(1, 2, 3), from)
					want, ok := scaledInt64(v, fromScale, toScale)
					if !ok {
						if !errors.Is(err, parquet.ErrInvalidConversion) {
							t.Fatalf("value %d: error = %v, want ErrInvalidConversion", v, err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("value %d: %v", v, err)
					}
					wantValue := parquet.Int64Value(want).Level(1, 2, 3)
					if !parquet.DeepEqual(wantValue, got) {
						t.Fatalf("value %d: got %#v, want %#v", v, got, wantValue)
					}
				}
			})
		}
	}
}

func TestConvertTimestampToDateAndTime(t *testing.T) {
	from := parquet.Timestamp(parquet.Millisecond).Type()
	cases := []time.Time{
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2500, 6, 15, 12, 34, 56, 789_000_000, time.UTC),
	}
	for _, when := range cases {
		ms := when.UnixMilli()
		t.Run(when.Format(time.RFC3339Nano), func(t *testing.T) {
			instant := time.UnixMilli(ms).UTC()
			day, ok := wantEpochDay(instant)
			got, err := parquet.Date().Type().ConvertValue(parquet.Int64Value(ms), from)
			if !ok {
				if !errors.Is(err, parquet.ErrInvalidConversion) {
					t.Fatalf("date error = %v, want ErrInvalidConversion", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if got.Int32() != day {
					t.Fatalf("date = %d, want %d (%s)", got.Int32(), day, instant)
				}
			}

			midnight := time.Date(instant.Year(), instant.Month(), instant.Day(), 0, 0, 0, 0, time.UTC)
			for _, unit := range []parquet.TimeUnit{parquet.Millisecond, parquet.Microsecond, parquet.Nanosecond} {
				scale := int64(unit.Duration())
				want := instant.Sub(midnight).Nanoseconds() / scale
				got, err := parquet.Time(unit).Type().ConvertValue(parquet.Int64Value(ms), from)
				if err != nil {
					t.Fatalf("%s: %v", unit.Duration(), err)
				}
				if got.Int64() != want {
					t.Fatalf("%s time of day = %d, want %d", unit.Duration(), got.Int64(), want)
				}
			}
		})
	}
}

func TestConvertTimestampToDateExtremes(t *testing.T) {
	for _, unit := range []parquet.TimeUnit{parquet.Millisecond, parquet.Microsecond, parquet.Nanosecond} {
		from := parquet.Timestamp(unit).Type()
		t.Run(from.String(), func(t *testing.T) {
			for _, v := range []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64} {
				instant := timestampInstant(unit, v)
				day, ok := wantEpochDay(instant)
				got, err := parquet.Date().Type().ConvertValue(parquet.Int64Value(v), from)
				if !ok {
					if !errors.Is(err, parquet.ErrInvalidConversion) {
						t.Fatalf("value %d: error = %v, want ErrInvalidConversion", v, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("value %d: %v", v, err)
				}
				if got.Int32() != day {
					t.Fatalf("value %d: date = %d, want %d", v, got.Int32(), day)
				}
			}
		})
	}
}

func TestConvertTimestampToDateBeforeEpoch(t *testing.T) {
	// Noon on the day before the epoch is inside a time.Duration. The
	// historical day count truncates to 0 and stays there.
	from := parquet.Timestamp(parquet.Millisecond).Type()
	const v = int64(-12 * time.Hour / time.Millisecond)
	got, err := parquet.Date().Type().ConvertValue(parquet.Int64Value(v), from)
	if err != nil {
		t.Fatal(err)
	}
	if got.Int32() != 0 {
		t.Fatalf("date = %d, want 0", got.Int32())
	}
}

func TestConvertTimestampToLocalTime(t *testing.T) {
	from := parquet.TimestampAdjusted(parquet.Millisecond, false).Type()
	to := parquet.TimeAdjusted(parquet.Nanosecond, false).Type()
	for _, when := range []time.Time{
		time.Date(2020, 7, 1, 1, 2, 3, 4_000_000, time.UTC),
		time.Date(2500, 1, 1, 0, 30, 0, 0, time.UTC),
	} {
		ms := when.UnixMilli()
		instant := time.UnixMilli(ms).In(time.Local)
		midnight := time.Date(instant.Year(), instant.Month(), instant.Day(), 0, 0, 0, 0, instant.Location())
		want := instant.In(time.Local).Sub(midnight).Nanoseconds()
		got, err := to.ConvertValue(parquet.Int64Value(ms), from)
		if err != nil {
			t.Fatal(err)
		}
		if got.Int64() != want {
			t.Fatalf("%s: time of day = %d, want %d", when, got.Int64(), want)
		}
	}
}

func TestConvertDateToTimestampRange(t *testing.T) {
	from := parquet.Date().Type()
	units := []parquet.TimeUnit{parquet.Millisecond, parquet.Microsecond, parquet.Nanosecond}
	days := []int32{math.MinInt32, -719162, -1, 0, 1, 19338}
	year2500 := int32(time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	days = append(days, year2500, math.MaxInt32)

	for _, unit := range units {
		perDay := int64((24 * time.Hour) / unit.Duration())
		limit := min(int64(math.MaxInt64)/perDay, math.MaxInt32)
		floor := max(int64(math.MinInt64)/perDay, math.MinInt32)
		days := append(append([]int32{}, days...), int32(limit), int32(floor))
		if limit < math.MaxInt32 {
			days = append(days, int32(limit)+1)
		}
		if floor > math.MinInt32 {
			days = append(days, int32(floor)-1)
		}

		t.Run(unit.Duration().String(), func(t *testing.T) {
			targets := []parquet.Type{
				parquet.Timestamp(unit).Type(),
				parquet.TimestampAdjusted(unit, false).Type(),
			}
			for _, day := range days {
				want, ok := scaledInt64(int64(day), perDay, 1)
				for _, to := range targets {
					got, err := to.ConvertValue(parquet.Int32Value(day), from)
					if !ok {
						if !errors.Is(err, parquet.ErrInvalidConversion) {
							t.Fatalf("day %d: error = %v, want ErrInvalidConversion", day, err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("day %d: %v", day, err)
					}
					if got.Int64() != want {
						t.Fatalf("day %d: timestamp = %d, want %d", day, got.Int64(), want)
					}
				}
			}
		})
	}
}

func TestConvertStringToDateRange(t *testing.T) {
	from := parquet.String().Type()
	to := parquet.Date().Type()
	for _, date := range []string{"0001-01-01", "1500-01-01", "1969-12-31", "1970-01-01", "2500-01-01", "9999-01-01"} {
		parsed, err := time.ParseInLocation("2006-01-02", date, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		day, ok := wantEpochDay(parsed)
		if !ok {
			t.Fatalf("%s does not fit in DATE", date)
		}
		got, err := to.ConvertValue(parquet.ByteArrayValue([]byte(date)), from)
		if err != nil {
			t.Fatalf("%s: %v", date, err)
		}
		if got.Int32() != day {
			t.Fatalf("%s: date = %d, want %d", date, got.Int32(), day)
		}
	}
}

func TestDateRoundTripFarFromEpoch(t *testing.T) {
	type row struct {
		D time.Time `parquet:"d,date"`
	}
	for _, year := range []int{1, 1500, 1969, 1970, 2024, 2500, 9999} {
		when := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		var buf bytes.Buffer
		if err := parquet.Write(&buf, []row{{when}}); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[row](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || !got[0].D.Equal(when) {
			t.Fatalf("year %d: got %v, want %s", year, got, when.Format(time.RFC3339Nano))
		}
	}
}

func TestReadMillisTimestampIntoOtherUnits(t *testing.T) {
	type source struct {
		T time.Time `parquet:"t,timestamp(millisecond)"`
	}
	when := time.Date(2500, 1, 1, 12, 34, 56, 789_000_000, time.UTC)
	var buf bytes.Buffer
	if err := parquet.Write(&buf, []source{{when}}); err != nil {
		t.Fatal(err)
	}
	payload := buf.Bytes()

	t.Run("microsecond", func(t *testing.T) {
		type target struct {
			T time.Time `parquet:"t,timestamp(microsecond)"`
		}
		got, err := parquet.Read[target](bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		want := when.Truncate(time.Millisecond)
		if len(got) != 1 || !got[0].T.Equal(want) {
			t.Fatalf("got %v, want %s", got, want.Format(time.RFC3339Nano))
		}
	})

	t.Run("nanosecond", func(t *testing.T) {
		type target struct {
			T time.Time `parquet:"t,timestamp(nanosecond)"`
		}
		_, err := parquet.Read[target](bytes.NewReader(payload), int64(len(payload)))
		if !errors.Is(err, parquet.ErrInvalidConversion) {
			t.Fatalf("error = %v, want ErrInvalidConversion", err)
		}
	})

	t.Run("untagged", func(t *testing.T) {
		type target struct {
			T time.Time `parquet:"t"`
		}
		_, err := parquet.Read[target](bytes.NewReader(payload), int64(len(payload)))
		if !errors.Is(err, parquet.ErrInvalidConversion) {
			t.Fatalf("error = %v, want ErrInvalidConversion", err)
		}
	})

	t.Run("date", func(t *testing.T) {
		type target struct {
			D int32 `parquet:"t,date"`
		}
		got, err := parquet.Read[target](bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		want := int32(time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
		if len(got) != 1 || got[0].D != want {
			t.Fatalf("date = %v, want %d", got, want)
		}
	})

	t.Run("time", func(t *testing.T) {
		type target struct {
			N int64 `parquet:"t,time(nanosecond)"`
		}
		got, err := parquet.Read[target](bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		stored := when.Truncate(time.Millisecond)
		midnight := time.Date(stored.Year(), stored.Month(), stored.Day(), 0, 0, 0, 0, time.UTC)
		want := stored.Sub(midnight).Nanoseconds()
		if len(got) != 1 || got[0].N != want {
			t.Fatalf("time of day = %v, want %d", got, want)
		}
	})
}

func TestReadExtremeTimestampUnits(t *testing.T) {
	t.Run("max micros to millis", func(t *testing.T) {
		type source struct {
			T int64 `parquet:"t,timestamp(microsecond)"`
		}
		type target struct {
			T int64 `parquet:"t,timestamp(millisecond)"`
		}
		var buf bytes.Buffer
		if err := parquet.Write(&buf, []source{{math.MaxInt64}}); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[target](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].T != math.MaxInt64/1000 {
			t.Fatalf("got %v, want %d", got, int64(math.MaxInt64/1000))
		}
	})

	t.Run("min micros to millis", func(t *testing.T) {
		type source struct {
			T int64 `parquet:"t,timestamp(microsecond)"`
		}
		type target struct {
			T int64 `parquet:"t,timestamp(millisecond)"`
		}
		var buf bytes.Buffer
		if err := parquet.Write(&buf, []source{{math.MinInt64}}); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[target](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].T != math.MinInt64/1000 {
			t.Fatalf("got %v, want %d", got, int64(math.MinInt64/1000))
		}
	})

	t.Run("max millis to nanos", func(t *testing.T) {
		type source struct {
			T int64 `parquet:"t,timestamp(millisecond)"`
		}
		type target struct {
			T time.Time `parquet:"t,timestamp(nanosecond)"`
		}
		var buf bytes.Buffer
		if err := parquet.Write(&buf, []source{{math.MaxInt64}}); err != nil {
			t.Fatal(err)
		}
		_, err := parquet.Read[target](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if !errors.Is(err, parquet.ErrInvalidConversion) {
			t.Fatalf("error = %v, want ErrInvalidConversion", err)
		}
	})
}

func TestTimeDurationAssignRange(t *testing.T) {
	for _, unit := range []parquet.TimeUnit{parquet.Millisecond, parquet.Microsecond, parquet.Nanosecond} {
		typ := parquet.Time(unit).Type()
		scale := int64(unit.Duration())
		t.Run(typ.String(), func(t *testing.T) {
			for _, v := range timestampUnitValues(scale) {
				var got time.Duration
				err := typ.AssignValue(reflect.ValueOf(&got).Elem(), parquet.Int64Value(v))
				want, ok := scaledInt64(v, scale, 1)
				if !ok {
					if !errors.Is(err, parquet.ErrInvalidConversion) {
						t.Fatalf("value %d: error = %v, want ErrInvalidConversion", v, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("value %d: %v", v, err)
				}
				if int64(got) != want {
					t.Fatalf("value %d: duration = %d, want %d", v, got, want)
				}
			}
		})
	}
}

func timestampUnitValues(factor int64) []int64 {
	values := []int64{
		math.MinInt64, math.MinInt64 + 1,
		-1_000_000_000_000, -1, 0, 1,
		1_670_888_613_123,
		math.MaxInt64 - 1, math.MaxInt64,
	}
	when := time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
	values = append(values, when.UnixMilli(), when.UnixMicro())
	recent := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	values = append(values, recent.UnixNano())
	if factor > 1 {
		values = append(values,
			math.MinInt64/factor-1, math.MinInt64/factor, math.MinInt64/factor+1,
			math.MaxInt64/factor-1, math.MaxInt64/factor, math.MaxInt64/factor+1,
		)
	}
	return values
}

func timestampInstant(unit parquet.TimeUnit, v int64) time.Time {
	switch unit {
	case parquet.Millisecond:
		return time.UnixMilli(v).UTC()
	case parquet.Microsecond:
		return time.UnixMicro(v).UTC()
	default:
		return time.Unix(0, v).UTC()
	}
}

func wantEpochDay(t time.Time) (int32, bool) {
	epoch := time.Unix(0, 0)
	d := t.Sub(epoch)
	if epoch.Add(d).Equal(t) {
		return int32(int(d.Hours()) / 24), true
	}
	day := floorDiv(t.Unix(), 86400)
	if day > math.MaxInt32 || day < math.MinInt32 {
		return 0, false
	}
	return int32(day), true
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func scaledInt64(v, numer, denom int64) (int64, bool) {
	if denom == 0 {
		return 0, false
	}
	prod := new(big.Int).Mul(big.NewInt(v), big.NewInt(numer))
	q := new(big.Int).Quo(prod, big.NewInt(denom))
	if !q.IsInt64() {
		return 0, false
	}
	return q.Int64(), true
}
