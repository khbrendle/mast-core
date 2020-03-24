package mast

import (
	"bytes"
	"text/template"
)

type FilterItem struct {
	Equality `json:"equality,omitempty"`
	Operator string `json:"operator,omitempty"`
}

// TemplateBytes will run an input template against a JoinItem object
func (s FilterItem) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateFilterItem").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, s); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against JoinItem object
func (s FilterItem) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (s FilterItem) GeneratePySpark() (string, error) {
	tmpl := `{{ if not .Operator }} {{ .Operator }} {{ end }}{{ .Equality.GeneratePySpark }}`
	return s.TemplateString(tmpl)
}
