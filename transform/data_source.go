package mast

import (
	"bytes"
	"text/template"
)

// DataSource contains the operations to create source/subquery
type DataSource struct {
	Select     []FieldTransform       `json:"select"`
	Location   DataLocation           `json:"location"`
	Filter     string                 `json:"filter"`
	Operations []*DataSourceOperation `json:"operations"`
}

// TemplateBytes will run an input template against a DataSource object
func (s DataSource) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateDataSource").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, s); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against DataSource object
func (s DataSource) TemplateString(input string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(input); err != nil {
		return "", err
	}
	return string(b), nil
}

func (s DataSource) GenerateSQL() (string, error) {
	tmpl := `select {{ .GenerateSQLSelect }}
from {{ .GenerateSQLFrom }}{{range .Operations}}{{.GenerateSQL}}{{end}}`
	return s.TemplateString(tmpl)
}

func (s DataSource) GenerateSQLSelect() (string, error) {
	tmpl := `{{ $len := sub1 (len .Select) }}{{range $i, $e := .Select}}{{ $e.GenerateSQL }}{{if lt $i $len }}, {{end}}{{end}}`
	return s.TemplateString(tmpl)
}

func (s DataSource) GenerateSQLFrom() (string, error) {
	tmpl := `"{{ .Location.Schema }}"."{{ .Location.Table }}"`
	return s.TemplateString(tmpl)
}
