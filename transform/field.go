package mast

import (
	"bytes"
	"html/template"
)

type Field struct {
	Database   string `json:"database"`
	Table      string `json:"table"`
	Column     string `json:"column"`
	TableAlias string `json:"table_alias"`
}

// TemplateBytes executes an input template against Field object returning byte array
func (f Field) TemplateBytes(input string) ([]byte, error) {
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

// TemplateString executes an input template against Field object returning string
func (f Field) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = f.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (f *Field) SetTableAlias(x string) {
	f.TableAlias = x
}

func (f Field) GetTable() string {
	return f.Table
}

func (f Field) GenerateSQL() (string, error) {
	var tmpl string
	if f.TableAlias == "" {
		tmpl = `"{{ .Table }}"."{{ .Column }}"`
	} else {
		tmpl = `"{{ .TableAlias }}"."{{ .Column }}"`
	}
	return f.TemplateString(tmpl)
}

func (f Field) GeneratePySpark() (string, error) {
	var tmpl string
	if f.TableAlias == "" {
		tmpl = `F.col("{{ .Table }}_{{ .Column }}")`
	} else {
		tmpl = `F.col("{{ .TableAlias }}_{{ .Column }}")`
	}
	return f.TemplateString(tmpl)
}
