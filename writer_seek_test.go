package parquet

import (
	"strconv"
	"testing"
)

func TestSeek(t *testing.T) {
	singularVals := []Value{
		ByteArrayValue([]byte("foo")).Level(0, 0, 1),
		ByteArrayValue([]byte("bar")).Level(0, 1, 1),
		ByteArrayValue([]byte("baz")).Level(0, 2, 1),
	}
	repeatedVals := []Value{
		Int32Value(0).Level(0, 0, 0),
		Int32Value(0).Level(1, 0, 0),
		Int32Value(0).Level(2, 0, 0),
		Int32Value(0).Level(2, 0, 0),

		Int32Value(1).Level(0, 0, 0),
		Int32Value(1).Level(1, 0, 0),
		Int32Value(1).Level(2, 0, 0),
		Int32Value(1).Level(2, 0, 0),

		Int32Value(2).Level(0, 0, 0),
		Int32Value(2).Level(1, 0, 0),
		Int32Value(2).Level(2, 0, 0),
		Int32Value(2).Level(2, 0, 0),

		Int32Value(3).Level(0, 0, 0),
		Int32Value(3).Level(1, 0, 0),
		Int32Value(3).Level(2, 0, 0),
		Int32Value(3).Level(2, 0, 0),

		Int32Value(4).Level(0, 0, 0),
		Int32Value(4).Level(1, 0, 0),
		Int32Value(4).Level(2, 0, 0),
		Int32Value(4).Level(2, 0, 0),
	}
	testCases := []struct {
		name       string
		input      []Value
		valsPerRow int
		numRows    int
	}{
		{
			name:       "singular",
			input:      singularVals,
			valsPerRow: 1,
			numRows:    3,
		},
		{
			name:       "repeated",
			input:      repeatedVals,
			valsPerRow: 4,
			numRows:    5,
		},
	}
	for _, testCase := range testCases {
		for i := -1; i <= testCase.numRows+1; i++ {
			t.Run(testCase.name+"_"+strconv.Itoa(i), func(t *testing.T) {
				var result int
				var panicVal any
				func() {
					defer func() {
						panicVal = recover()
						if panicVal != nil {
							t.Logf("panic: %v", panicVal)
						}
					}()
					result = seek(testCase.input, i)
				}()
				if i < 0 || i > testCase.numRows {
					if panicVal == nil {
						t.Error("expected out-of-range panic")
					}
				} else if result != testCase.valsPerRow*i {
					t.Errorf("expected %d but got %d", testCase.valsPerRow*i, result)
				}
			})
		}
	}
}
