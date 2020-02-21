package mast

import (
	"bytes"
	"text/template"
)

// JoinItem represents how a table can be joined
// should be able to construct a function call, a field,
// a static value, and describe an operation of multiple
// join fields are necessary
type JoinItem struct {
	// single join group item representing equality of 2 fields or function call
	Entity FieldTransform `json:"entity"`
	// Logical operator; i.e. and, or, &, |
	Operator string `json:"operator"`
}

// TemplateBytes will run an input template against a JoinItem object
func (s JoinItem) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateJoinItem").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, s); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against JoinItem object
func (s JoinItem) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (j JoinItem) GenerateSQL() (string, error) {
	tmpl := `{{if ne .Operator ""}}{{ .Operator }} {{end}}{{ .Entity.GenerateSQL }}`
	return j.TemplateString(tmpl)
}
