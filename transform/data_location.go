package mast

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/rs/xid"
)

// DataLocation is the physical location of the data
type DataLocation struct {
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table,omitempty"`
	Alias    string `json:"alias,omitempty"`
}

// TemplateBytes will run an input template against a DataLocation object
func (s DataLocation) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateDataLocation").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, s); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against DataLocation object
func (s DataLocation) TemplateString(input string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(input); err != nil {
		return "", err
	}
	return string(b), nil
}

func (d *DataLocation) CreateAlias() string {
	// fmt.Printf("creating alias for %s\n", d.Table)
	var a string
	switch test {
	case true:
		a = fmt.Sprintf("a_%s", d.Table)
	case false:
		a = fmt.Sprintf("t_%s", xid.New().String())
	}
	d.Alias = a
	return a
}

func (d DataLocation) GenerateSQL() (string, error) {
	// d.CreateAlias()
	tmpl := `"{{ .Schema }}"."{{ .Table }}" as "{{ .Alias }}"`
	return d.TemplateString(tmpl)
}

func (d DataLocation) GeneratePySpark() (string, error) {
	// d.CreateAlias()
	tmpl := `df_{{ .Database }}_{{ .Schema }}_{{ .Table }}`
	return d.TemplateString(tmpl)
}
