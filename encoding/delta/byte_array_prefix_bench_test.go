package delta

import (
	"fmt"
	"math/rand"
	"testing"
)

// generatePrefixHeavyByteArrays produces values resembling sorted URLs/paths,
// where DELTA_BYTE_ARRAY's prefix compression actually kicks in (the generic
// benchmarks use random bytes, which have no shared prefixes).
func generatePrefixHeavyByteArrays(n int, r *rand.Rand) ([]byte, []uint32) {
	prefixes := []string{
		"https://example.com/api/v1/users/",
		"https://example.com/api/v1/orders/",
		"https://example.com/api/v2/products/electronics/",
		"s3://data-lake-production/tables/events/date=2026-08-",
		"/var/log/containers/app-",
	}
	values := make([]byte, 0, n*64)
	offsets := make([]uint32, 0, n+1)
	for range n {
		offsets = append(offsets, uint32(len(values)))
		values = append(values, prefixes[r.Intn(len(prefixes))]...)
		values = fmt.Appendf(values, "%08d", r.Intn(1000000))
	}
	offsets = append(offsets, uint32(len(values)))
	return values, offsets
}

func BenchmarkEncodeByteArrayPrefixHeavy(b *testing.B) {
	e := &ByteArrayEncoding{}
	values, offsets := generatePrefixHeavyByteArrays(10000, rand.New(rand.NewSource(1)))
	buffer := make([]byte, 0, 2*len(values))
	b.SetBytes(int64(len(values)))
	for b.Loop() {
		buffer, _ = e.EncodeByteArray(buffer, values, offsets)
	}
	b.ReportMetric(float64(len(buffer))/float64(len(values)), "ratio")
}

func BenchmarkDecodeByteArrayPrefixHeavy(b *testing.B) {
	e := &ByteArrayEncoding{}
	values, offsets := generatePrefixHeavyByteArrays(10000, rand.New(rand.NewSource(1)))
	encoded, err := e.EncodeByteArray(nil, values, offsets)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, len(values))
	dstOffsets := make([]uint32, 0, len(offsets))
	b.SetBytes(int64(len(values)))
	for b.Loop() {
		dst, dstOffsets, _ = e.DecodeByteArray(dst, encoded, dstOffsets)
	}
}
