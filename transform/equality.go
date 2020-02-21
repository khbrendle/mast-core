package mast

import (
	"bytes"
	"text/template"
)

type Equality struct {
	Operator string          `json:"operator"`
	Arg      *FieldTransform `json:"arg"`
}

// TemplateBytes executes an input template against Equality object returning byte array
func (f Equality) TemplateBytes(input string) ([]byte, error) {
	var tmpl *template.Template
	var err error
	if tmpl, err = template.New("templateField").Parse(input); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	if err = tmpl.Execute(&b, f); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// TemplateString executes an input template against Equality object returning string
func (f Equality) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = f.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (e Equality) GenerateSQL() (string, error) {
	tmpl := `{{ .Operator }} {{ .Arg.GenerateSQL }}`

	return e.TemplateString(tmpl)
}

func (e Equality) GeneratePySpark() (string, error) {
	tmpl := `{{ .Operator }} {{ .Arg.GeneratePySpark }}`

	return e.TemplateString(tmpl)
}
