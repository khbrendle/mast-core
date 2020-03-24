package mast

import (
	"bytes"
	"fmt"
	"text/template"
)

// DataSourceOperation instructs how to join or union a second source
type DataSourceOperation struct {
	Type   OperationType `json:"type,omitempty"`
	Source DataSource    `json:"source,omitempty"`
	Level  int           `json:"level,omitempty"`
}

// TemplateBytes will run an input template against a DataSourceOperation object
func (s DataSourceOperation) TemplateBytes(input string) ([]byte, error) {
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

// TemplateString will run template against DataSourceOperation object
func (s DataSourceOperation) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

// func (s *DataSourceOperation) CreateAlias() {
// 	s.Source.Location.CreateAlias()
// }

func (ds *DataSourceOperation) SetLevel(l int) {
	ds.Level = l
}

func (ds *DataSourceOperation) GetTableName() string {
	return ds.Source.Location.Table
}

func (ds *DataSourceOperation) SetLocationAlias() string {
	return ds.Source.SetAlias()
}

func (s *DataSourceOperation) GenerateSQL() (string, error) {
	// s.CreateAlias()
	// s.Source.PropogateAlias()
	switch s.Type.Method {
	case "union":
		return s.GenerateSQLUnion()
	case "join":
		return s.GenerateSQLJoin()
	default:
		return "", fmt.Errorf("unexpected DataSourceOperation type '%s'", s.Type.Method)
	}
}

func (s *DataSourceOperation) GenerateSQLUnion() (string, error) {
	tmpl := `
{{ levelSpaces .Level }}union{{ .Type.Modifier }}
{{ levelSpaces .Level }}{{ .Source.GenerateSQL }}`

	return s.TemplateString(tmpl)
}

func (s *DataSourceOperation) GenerateSQLJoin() (string, error) {
	tmpl := `
{{ levelSpaces .Level }}{{ .Type.GenerateSQLModifier }}join {{ .Source.GenerateSQLFrom }}
{{ levelSpaces .Level }}  on {{ .Type.GenerateSQLJoin }}`
	return s.TemplateString(tmpl)
}

func (s *DataSourceOperation) GeneratePySpark() (string, error) {
	switch s.Type.Method {
	case "union":
		return s.GeneratePySparkUnion()
	case "join":
		return s.GeneratePySparkJoin()
	default:
		return "", fmt.Errorf("unexpected DataSourceOperation type '%s'", s.Type.Method)
	}
}

func (s *DataSourceOperation) GeneratePySparkJoin() (string, error) {
	tmpl := `.join({{ .Source.GeneratePySpark }}, on = {{ .Type.GeneratePySparkJoin }}{{ if ne .Type.Modifier "" }}, how = "{{ .Type.Modifier }}"{{ end }})`
	return s.TemplateString(tmpl)
}

func (s *DataSourceOperation) GeneratePySparkUnion() (string, error) {
	tmpl := `.union({{ .Source.GeneratePySpark }})`
	return s.TemplateString(tmpl)
}
