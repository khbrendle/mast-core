package mast

import (
	"bytes"
	"text/template"
)

// DataLocation is the physical location of the data
type DataLocation struct {
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
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

func (d *DataLocation) GenerateSQL() (string, error) {
	tmpl := `"{{ d.Schema }}"."{{ .Table }}"`
	return d.TemplateString(tmpl)
}
