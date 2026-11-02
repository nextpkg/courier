module github.com/nextpkg/courier/examples

go 1.26.0

require (
	github.com/nextpkg/courier v0.1.0
	github.com/nextpkg/courier/transport/kafka v0.1.0
	github.com/nextpkg/courier/transport/memory v0.1.0
	github.com/nextpkg/courier/transport/nats v0.1.0
	github.com/apache/thrift v0.25.0
	github.com/nats-io/nats.go v1.55.0
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kmsg v1.15.0
)

require (
	github.com/ThreeDotsLabs/watermill v1.5.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/lithammer/shortuuid/v3 v3.0.7 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/nextpkg/courier => ..

replace github.com/nextpkg/courier/transport/memory => ../transport/memory

replace github.com/nextpkg/courier/transport/nats => ../transport/nats

replace github.com/nextpkg/courier/transport/kafka => ../transport/kafka
