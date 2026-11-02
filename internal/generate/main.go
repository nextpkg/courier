// Command generate runs the Thrift compiler and courier-gen for the example IDLs.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func command(name string, args ...string) error {
	c := exec.Command(name, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}
func run() error {
	thrift := os.Getenv("THRIFT")
	if thrift == "" {
		thrift = "thrift"
	}
	v, err := exec.Command(thrift, "-version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("install official Thrift compiler 0.25.0 or set THRIFT: %w (%s)", err, v)
	}
	if strings.TrimSpace(string(v)) != "Thrift version 0.25.0" {
		return fmt.Errorf("compiler/runtime mismatch: want 0.25.0, got %q", v)
	}
	if err = os.MkdirAll("examples/gen", 0755); err != nil {
		return err
	}
	for _, name := range []string{"demo", "shapes"} {
		if err = command(thrift, "-out", "examples/gen", "--gen", "go:skip_remote,package_prefix=github.com/nextpkg/courier/examples/gen/", filepath.Join("examples/idl", name+".thrift")); err != nil {
			return err
		}
		if err = command("go", "run", "./cmd/courier-gen", "-in", filepath.Join("examples/gen", name, name+".go"), "-out", filepath.Join("examples/gen", name, name+"_courier.go")); err != nil {
			return err
		}
	}
	return nil
}
