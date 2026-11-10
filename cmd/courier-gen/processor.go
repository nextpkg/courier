package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

// courierProcessor derives memory-transport processors from the pinned official
// implementation, preserving argument/result and exception handling. Official
// network processors and Thrift's process-wide configuration remain untouched.
func courierProcessor(f *ast.File, service string) (string, error) {
	var constructor *ast.FuncDecl
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "New"+service+"Processor" {
			constructor = fn
		}
	}
	if constructor == nil {
		return "", fmt.Errorf("courier-gen: missing %s processor", service)
	}
	names := map[string]string{}
	ast.Inspect(constructor.Body, func(n ast.Node) bool {
		if lit, ok := n.(*ast.CompositeLit); ok {
			name := ident(lit.Type)
			if name != "" && name != service+"Processor" {
				names[name] = "_courier" + name
			}
		}
		return true
	})
	for _, name := range names {
		if f.Scope.Lookup(name) != nil {
			return "", fmt.Errorf("courier-gen: generated helper name collision: %s", name)
		}
	}
	if f.Scope.Lookup("New"+service+"CourierProcessor") != nil {
		return "", fmt.Errorf("courier-gen: generated helper name collision: New%sCourierProcessor", service)
	}
	if len(names) == 0 {
		return "", fmt.Errorf("courier-gen: unsupported %s processor map", service)
	}
	var output strings.Builder
	// Clone ASTs so generating companion clients never changes the input model.
	emit := func(node ast.Node, strip bool) error {
		clone, err := parser.ParseFile(token.NewFileSet(), "processor.go", "package p\n"+render(node), 0)
		if err != nil {
			return err
		}
		if strip {
			fn := clone.Decls[0].(*ast.FuncDecl)
			decls, checks := 0, 0
			kept := fn.Body.List[:0]
			for _, stmt := range fn.Body.List {
				if a, ok := stmt.(*ast.AssignStmt); ok && len(a.Lhs) == 1 && ident(a.Lhs[0]) == "tickerCancel" {
					if len(a.Rhs) != 1 {
						return fmt.Errorf("courier-gen: unexpected ticker initializer in %s", fn.Name)
					}
					init, ok := a.Rhs[0].(*ast.FuncLit)
					if !ok || len(init.Body.List) != 0 {
						return fmt.Errorf("courier-gen: unexpected ticker initializer")
					}
					decls++
					continue
				}
				if branch, ok := stmt.(*ast.IfStmt); ok && strings.Contains(render(branch.Cond), "thrift.ServerConnectivityCheckInterval") {
					if render(branch.Cond) != "thrift.ServerConnectivityCheckInterval > 0" || branch.Else != nil {
						return fmt.Errorf("courier-gen: unsupported connectivity check")
					}
					checks++
					continue
				}
				kept = append(kept, stmt)
			}
			if decls != 1 || checks != 1 {
				return fmt.Errorf("courier-gen: expected one connectivity check in %s", receiver(fn))
			}
			fn.Body.List = kept
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if block, ok := n.(*ast.BlockStmt); ok {
					kept := block.List[:0]
					for _, stmt := range block.List {
						if expr, ok := stmt.(*ast.ExprStmt); ok {
							if call, ok := expr.X.(*ast.CallExpr); ok && ident(call.Fun) == "tickerCancel" && len(call.Args) == 0 {
								continue
							}
						}
						kept = append(kept, stmt)
					}
					block.List = kept
				}
				return true
			})
		}
		ast.Inspect(clone, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				if name, exists := names[id.Name]; exists {
					id.Name = name
				}
			}
			return true
		})
		for _, d := range clone.Decls {
			output.WriteString(render(d))
			output.WriteByte('\n')
		}
		return nil
	}
	// The constructor still returns the official processor type, with only its
	// per-method implementations replaced by memory-transport variants.
	names[constructor.Name.Name] = "New" + service + "CourierProcessor"
	if err := emit(constructor, false); err != nil {
		return "", err
	}
	for _, d := range f.Decls {
		switch node := d.(type) {
		case *ast.GenDecl:
			if node.Tok != token.TYPE {
				continue
			}
			for _, spec := range node.Specs {
				typ := spec.(*ast.TypeSpec)
				if _, ok := names[typ.Name.Name]; ok {
					if err := emit(&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{typ}}, false); err != nil {
						return "", err
					}
				}
			}
		case *ast.FuncDecl:
			if _, ok := names[receiver(node)]; ok {
				if node.Name.Name != "Process" {
					return "", fmt.Errorf("courier-gen: unsupported processor method %s", node.Name)
				}
				if err := emit(node, true); err != nil {
					return "", err
				}
			}
		}
	}
	return output.String(), nil
}
