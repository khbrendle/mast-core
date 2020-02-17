package mast

import (
	"bytes"
	"text/template"
)

// OperationType provides the information for how to combine 2 tables
// either by join or union
type OperationType struct {
	Method   string     `json:"method"`
	Modifier string     `json:"modifier"`
	JoinOn   []JoinItem `json:"join_on"`
}

// TemplateBytes will run an input template against a OperationType object
func (s OperationType) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateOperationType").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, s); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against OperationType object
func (s OperationType) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (s OperationType) GenerateSQLJoin() (string, error) {
	tmpl := `{{range $i, $e := .JoinOn }}{{if gt $i 0}} {{end}}{{ .GenerateSQL }}{{end}}`
	return s.TemplateString(tmpl)
}

func (s OperationType) GenerateSQLModifier() (string, error) {
	var tmpl string
	switch s.Method {
	case "union":
		tmpl = `{{if ne .Modifier ""}} {{end}}{{.Modifier}}`
	case "join":
		tmpl = `{{.Modifier}}{{if ne .Modifier ""}} {{end}}`
	}
	return s.TemplateString(tmpl)
}
