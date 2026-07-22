package parquet

import (
	"io"
	"sync"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/bytealg"
)

// fixedRepeatedPage is a Page implementation for pages of repeated columns
// which were detected (from their encoded level streams) to contain only
// complete lists of the same length with no null values.
//
// Compared to repeatedPage, it does not hold materialized repetition and
// definition level arrays: row boundaries are computed arithmetically from
// the list length, making NumRows, NumNulls and Slice O(1). The level slices
// are only generated if a consumer explicitly requests them through the
// RepetitionLevels or DefinitionLevels methods.
//
// fixedRepeatedPage is only used for columns with maxRepetitionLevel == 1.
type fixedRepeatedPage struct {
	base               Page
	listLength         int
	maxDefinitionLevel byte

	levelsOnce       sync.Once
	repetitionLevels []byte
	definitionLevels []byte
}

func newFixedRepeatedPage(base Page, listLength int, maxDefinitionLevel byte) *fixedRepeatedPage {
	return &fixedRepeatedPage{
		base:               base,
		listLength:         listLength,
		maxDefinitionLevel: maxDefinitionLevel,
	}
}

// FixedListLength returns the common length of the lists in the page.
func (page *fixedRepeatedPage) FixedListLength() int { return page.listLength }

func (page *fixedRepeatedPage) Type() Type { return page.base.Type() }

func (page *fixedRepeatedPage) Column() int { return page.base.Column() }

func (page *fixedRepeatedPage) Dictionary() Dictionary { return page.base.Dictionary() }

func (page *fixedRepeatedPage) NumRows() int64 {
	return page.base.NumValues() / int64(page.listLength)
}

func (page *fixedRepeatedPage) NumValues() int64 { return page.base.NumValues() }

func (page *fixedRepeatedPage) NumNulls() int64 { return 0 }

func (page *fixedRepeatedPage) Bounds() (min, max Value, ok bool) { return page.base.Bounds() }

func (page *fixedRepeatedPage) Size() int64 { return page.base.Size() }

func (page *fixedRepeatedPage) RepetitionLevels() []byte {
	page.materializeLevels()
	return page.repetitionLevels
}

func (page *fixedRepeatedPage) DefinitionLevels() []byte {
	page.materializeLevels()
	return page.definitionLevels
}

func (page *fixedRepeatedPage) materializeLevels() {
	page.levelsOnce.Do(func() {
		numValues := int(page.base.NumValues())
		levels := make([]byte, 2*numValues)

		repetitionLevels := levels[:numValues:numValues]
		bytealg.Broadcast(repetitionLevels, 1)
		for i := 0; i < numValues; i += page.listLength {
			repetitionLevels[i] = 0
		}
		page.repetitionLevels = repetitionLevels

		definitionLevels := levels[numValues:]
		bytealg.Broadcast(definitionLevels, page.maxDefinitionLevel)
		page.definitionLevels = definitionLevels
	})
}

func (page *fixedRepeatedPage) Data() encoding.Values { return page.base.Data() }

func (page *fixedRepeatedPage) Values() ValueReader {
	return &fixedRepeatedPageValues{
		page:   page,
		values: page.base.Values(),
	}
}

func (page *fixedRepeatedPage) Slice(i, j int64) Page {
	numRows := page.NumRows()
	if i < 0 || i > numRows || j < 0 || j > numRows || i > j {
		panic(errPageBoundsOutOfRange(i, j, numRows))
	}
	n := int64(page.listLength)
	return newFixedRepeatedPage(
		page.base.Slice(i*n, j*n),
		page.listLength,
		page.maxDefinitionLevel,
	)
}

type fixedRepeatedPageValues struct {
	page   *fixedRepeatedPage
	values ValueReader
	offset int
}

func (r *fixedRepeatedPageValues) ReadValues(values []Value) (n int, err error) {
	numValues := int(r.page.base.NumValues())
	if r.offset >= numValues {
		return 0, io.EOF
	}
	if remain := numValues - r.offset; len(values) > remain {
		values = values[:remain]
	}

	n, err = r.values.ReadValues(values)

	listLength := r.page.listLength
	maxDefinitionLevel := r.page.maxDefinitionLevel
	for i := range values[:n] {
		values[i].repetitionLevel = 1
		values[i].definitionLevel = maxDefinitionLevel
	}
	// Zero the repetition level at each record boundary.
	i := 0
	if k := r.offset % listLength; k != 0 {
		i = listLength - k
	}
	for ; i < n; i += listLength {
		values[i].repetitionLevel = 0
	}

	r.offset += n
	if err == nil && r.offset == numValues {
		err = io.EOF
	}
	return n, err
}
