module github.com/nextpkg/courier/transport/kafka

go 1.26.0

require (
	github.com/nextpkg/courier v1.0.0
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kmsg v1.15.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/apache/thrift v0.25.0
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/stretchr/testify v1.11.1
)

replace github.com/nextpkg/courier => ../..
