package mast

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
)

type JoinGroup struct {
	Left     FieldTransform
	Right    FieldTransform
	Equality string
}

// TemplateBytes will run an input template against a JoinGroup object
func (j JoinGroup) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateJoinGroup").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, j); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against JoinGroup object
func (j JoinGroup) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = j.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (j JoinGroup) GenerateSQL() (string, error) {
	if (j.Left.Type != "") && (j.Right.Type != "") {
		// left and right both have info, then should be field join
		if j.Equality == "" {
			return "", errors.New("equality of join between 2 fields should not be blank")
		}
		return j.GenerateSQLFieldJoin()
		// left have info and right blank, single function quality
	} else if (j.Left.Type != "") && (j.Right.Type == "") {
		return j.GenerateSQLFunctionJoin()
	}
	var b []byte
	var err error
	if b, err = json.Marshal(j); err != nil {
		return "", err
	}
	return "", fmt.Errorf("unexpected pattern, could not create join operation for '%s'", string(b))
}

func (j JoinGroup) GenerateSQLFieldJoin() (string, error) {
	tmpl := `{{ .Left.GenerateSQL }} {{ .Equality }} {{ .Right.GenerateSQL }}`
	return j.TemplateString(tmpl)
}

func (j JoinGroup) GenerateSQLFunctionJoin() (string, error) {
	tmpl := `{{ .Left.GenerateSQL }}`
	return j.TemplateString(tmpl)
}
