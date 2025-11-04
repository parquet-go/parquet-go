package parquet

import (
	"fmt"
	"sync"
	"testing"
)

func assertCompare(t *testing.T, a, b Value, cmp func(Value, Value) int, want int) {
	if got := cmp(a, b); got != want {
		t.Errorf("compare(%v, %v): got=%d want=%d", a, b, got, want)
	}
}

func TestCompareNullsFirst(t *testing.T) {
	cmp := CompareNullsFirst(Int32Type.Compare)
	assertCompare(t, Value{}, Value{}, cmp, 0)
	assertCompare(t, Value{}, ValueOf(int32(0)), cmp, -1)
	assertCompare(t, ValueOf(int32(0)), Value{}, cmp, +1)
	assertCompare(t, ValueOf(int32(0)), ValueOf(int32(1)), cmp, -1)
}

func TestCompareNullsLast(t *testing.T) {
	cmp := CompareNullsLast(Int32Type.Compare)
	assertCompare(t, Value{}, Value{}, cmp, 0)
	assertCompare(t, Value{}, ValueOf(int32(0)), cmp, +1)
	assertCompare(t, ValueOf(int32(0)), Value{}, cmp, -1)
	assertCompare(t, ValueOf(int32(0)), ValueOf(int32(1)), cmp, -1)
}

// testColumnPool is a mock pool that tracks usage for testing
type testColumnPool struct {
	mu          sync.Mutex
	getCalls    int
	putCalls    int
	getCapacity []int
	pool        []*[][2]int32
}

func newTestColumnPool() *testColumnPool {
	return &testColumnPool{
		pool: make([]*[][2]int32, 0),
	}
}

func (p *testColumnPool) Get(maxCapacity int) [][2]int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.getCalls++
	p.getCapacity = append(p.getCapacity, maxCapacity)

	if len(p.pool) > 0 {
		buf := p.pool[len(p.pool)-1]
		p.pool = p.pool[:len(p.pool)-1]
		if cap(*buf) >= maxCapacity {
			*buf = (*buf)[:0]
			return *buf
		}
	}

	buf := make([][2]int32, 0, maxCapacity)
	return buf
}

func (p *testColumnPool) Put(buf [][2]int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.putCalls++
	buf = buf[:0]
	p.pool = append(p.pool, &buf)
}

func (p *testColumnPool) stats() (getCalls, putCalls int, getCapacity []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	capacity := make([]int, len(p.getCapacity))
	copy(capacity, p.getCapacity)
	return p.getCalls, p.putCalls, capacity
}

func TestComparatorWithCustomPool(t *testing.T) {
	schema := NewSchema("test", Group{"col": Optional(Leaf(Int32Type))})
	pool := newTestColumnPool()

	comparator := schema.Comparator(WithColumnPool(pool), Ascending("col"))

	row1 := Row{ValueOf(int32(10)).Level(0, 1, 0)}
	row2 := Row{ValueOf(int32(20)).Level(0, 1, 0)}

	result := comparator(row1, row2)
	if result >= 0 {
		t.Errorf("expected row1 < row2, got %d", result)
	}

	// Verify pool Get/Put are balanced when used
	getCalls, putCalls, capacities := pool.stats()
	if getCalls > 0 {
		if putCalls != getCalls {
			t.Errorf("pool calls mismatch: Get=%d Put=%d", getCalls, putCalls)
		}
		for _, cap := range capacities {
			if cap <= 0 {
				t.Errorf("invalid capacity hint: %d", cap)
			}
		}
	}
}

func TestComparatorWithNilPool(t *testing.T) {
	schema := NewSchema("test", Group{"col": Optional(Leaf(Int32Type))})

	// Create comparator with nil config (should use default pool)
	comparator := schema.Comparator(nil, Ascending("col"))

	row1 := Row{ValueOf(int32(10)).Level(0, 1, 0)}
	row2 := Row{ValueOf(int32(20)).Level(0, 1, 0)}

	result := comparator(row1, row2)
	if result >= 0 {
		t.Errorf("expected row1 < row2, got %d", result)
	}

	// Should work without errors
	result = comparator(row2, row1)
	if result <= 0 {
		t.Errorf("expected row2 > row1, got %d", result)
	}
}

func TestComparatorWithLargeColumnIndex(t *testing.T) {
	// Create schema with 40 optional columns to force pool usage (stack alloc threshold is 32)
	fields := make(map[string]Node, 40)
	for i := 0; i < 40; i++ {
		fields[fmt.Sprintf("col%d", i)] = Optional(Leaf(Int32Type))
	}
	schema := NewSchema("test", Group(fields))

	pool := newTestColumnPool()
	comparator := schema.Comparator(WithColumnPool(pool), Ascending("col39"))

	builder := NewRowBuilder(schema)
	builder.Add(39, ValueOf(int32(10)))
	row1 := builder.Row()
	builder.Reset()
	builder.Add(39, ValueOf(int32(20)))
	row2 := builder.Row()

	comparator(row1, row2)

	// Verify pool was used for large column indices (> 32)
	getCalls, putCalls, capacities := pool.stats()
	if getCalls == 0 {
		t.Error("expected pool to be used for large column indices")
	}
	if putCalls != getCalls {
		t.Errorf("pool calls mismatch: Get=%d Put=%d", getCalls, putCalls)
	}
	for _, cap := range capacities {
		if cap <= 32 {
			t.Errorf("expected capacity > 32, got %d", cap)
		}
	}
}

func TestComparatorPoolReuse(t *testing.T) {
	schema := NewSchema("test", Group{"col": Optional(Leaf(Int32Type))})
	pool := newTestColumnPool()

	comparator := schema.Comparator(WithColumnPool(pool), Ascending("col"))

	row1 := Row{ValueOf(int32(10)).Level(0, 1, 0)}
	row2 := Row{ValueOf(int32(20)).Level(0, 1, 0)}

	for i := 0; i < 10; i++ {
		comparator(row1, row2)
		comparator(row2, row1)
	}

	// Verify pool Get/Put are balanced
	getCalls, putCalls, _ := pool.stats()
	if getCalls > 0 && putCalls != getCalls {
		t.Errorf("pool calls mismatch: Get=%d Put=%d", getCalls, putCalls)
	}
}

func BenchmarkCompareBE128(b *testing.B) {
	v1 := [16]byte{}
	v2 := [16]byte{}

	for b.Loop() {
		compareBE128(&v1, &v2)
	}
}

func BenchmarkLessBE128(b *testing.B) {
	v1 := [16]byte{}
	v2 := [16]byte{}

	for b.Loop() {
		lessBE128(&v1, &v2)
	}
}
