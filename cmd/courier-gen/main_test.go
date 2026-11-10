package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestGeneratedParameterNamesCompile(t *testing.T) {
	src, err := os.ReadFile("../../examples/gen/shapes/shapes.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"f", "err", "zero", "_courierClient"} {
		t.Run(name, func(t *testing.T) {
			// Rename parameters in the pinned official fixture for both void and
			// value-returning methods; compile the wrapper, not just its syntax.
			input := regexp.MustCompile(`\b(data|fail)\b`).ReplaceAll(src, []byte(name))
			output, err := generate(input, "github.com/nextpkg/courier")
			if err != nil {
				t.Fatal(err)
			}
			dir, err := os.MkdirTemp("../../examples", "courier-gen-test-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			})
			for file, data := range map[string][]byte{"shapes.go": input, "shapes_courier.go": output} {
				if err := os.WriteFile(filepath.Join(dir, file), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("go", "test", ".")
			cmd.Dir = dir
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("generated package failed to compile: %v\n%s", err, output)
			}
		})
	}
}

func TestOfficialGeneratedFixtures(t *testing.T) {
	for _, name := range []string{"demo", "shapes"} {
		t.Run(name, func(t *testing.T) {
			base := filepath.Join("../../examples/gen", name)
			src, err := os.ReadFile(filepath.Join(base, name+".go"))
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(filepath.Join(base, name+"_courier.go"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := generate(src, "github.com/nextpkg/courier")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("generated wrapper drift: run go generate")
			}
			twice, err := generate(src, "github.com/nextpkg/courier")
			if err != nil || !bytes.Equal(got, twice) {
				t.Fatal("nondeterministic generation")
			}
		})
	}
}
func TestUnsupportedCompilerAndServiceShapes(t *testing.T) {
	src, err := os.ReadFile("../../examples/gen/demo/demo.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		data    []byte
		message string
	}{
		{"helper collision", append(append([]byte{}, src...), []byte("\ntype DialOrders struct{}\n")...), "generated helper name collision"},
		{"connectivity shape", bytes.ReplaceAll(src, []byte("thrift.ServerConnectivityCheckInterval > 0"), []byte("false")), "expected one connectivity check"},
		{"processor collision", append(append([]byte{}, src...), []byte("\ntype NewOrdersCourierProcessor struct{}\n")...), "generated helper name collision"},
		{"compiler", bytes.ReplaceAll(src, []byte("Compiler (0.25.0)"), []byte("Compiler (0.24.0)")), "expected official"},
		{"oneway", bytes.Replace(src, []byte("&_args2, &_result4"), []byte("&_args2, nil"), 1), "oneway"},
		{"inheritance", append(append([]byte{}, src...), []byte("\ntype Child interface { Orders }\n")...), "inheritance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := generate(tc.data, "github.com/nextpkg/courier"); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v", err)
			}
		})
	}
}
