module github.com/parquet-go/parquet-go

go 1.26.0

tool golang.org/x/tools/go/analysis/passes/modernize/cmd/modernize

require (
	github.com/andybalholm/brotli v1.3.0
	github.com/google/uuid v1.6.0
	github.com/hexops/gotextdiff v1.0.3
	github.com/klauspost/compress v1.20.4
	github.com/parquet-go/bitpack v0.0.0-20251026130316-7709569977d0
	github.com/parquet-go/jsonlite v0.1.0
	github.com/pierrec/lz4/v4 v4.1.33
	github.com/twpayne/go-geom v1.6.1
	golang.org/x/sys v0.50.0
	google.golang.org/protobuf v1.36.12
)

require golang.org/x/tools v0.53.0 // indirect

require (
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
)
