module github.com/nextpkg/courier/transport/kafka

go 1.26.0

require (
	github.com/nextpkg/courier v0.1.0
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kmsg v1.15.0
)

require (
	github.com/apache/thrift v0.25.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
)

replace github.com/nextpkg/courier => ../..
