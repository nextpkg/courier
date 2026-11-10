package main

import (
	"os"
	"testing"
)

func BenchmarkGenerate(b *testing.B) {
	source, err := os.ReadFile("../../examples/gen/demo/demo.go")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := generate(source, "github.com/nextpkg/courier"); err != nil {
			b.Fatal(err)
		}
	}
}
