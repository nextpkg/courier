package main

import (
	"embed"
	"fmt"
	"io"
	"text/template"
)

//go:embed templates/*.go.tmpl
var templateFiles embed.FS

var codeTemplates = template.Must(template.New("courier").Option("missingkey=error").ParseFS(templateFiles, "templates/*.go.tmpl"))

type serviceTemplateData struct {
	Name string
}

type importTemplateData struct {
	Alias, Path string
}

type fileTemplateData struct {
	Package string
	Imports []importTemplateData
	Body    string
}

type methodTemplateData struct {
	Receiver, Service, Name            string
	Params, ValueType, ResultSignature string
	Setup, Unwrap, Context             string
	WireMethod, Request, Response      string
	Arguments                          string
	Void                               bool
	Future, Error, Zero                string
}

func renderTemplate(w io.Writer, name string, data any) error {
	if err := codeTemplates.ExecuteTemplate(w, name, data); err != nil {
		return fmt.Errorf("courier-gen: render %s: %w", name, err)
	}
	return nil
}
