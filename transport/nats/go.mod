module github.com/nextpkg/courier/transport/nats

go 1.26.0

require (
	github.com/nextpkg/courier v1.0.0
	github.com/nats-io/nats.go v1.55.0
)

require (
	github.com/apache/thrift v0.25.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/nextpkg/courier => ../..
