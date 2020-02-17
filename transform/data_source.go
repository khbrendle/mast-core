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
	Alias      string                 `json:"-"`
}

// TemplateBytes will run an input template against a DataSource object
func (ds DataSource) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateDataSource").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, ds); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against DataSource object
func (ds DataSource) TemplateString(input string) (string, error) {
	var b []byte
	var err error
	if b, err = ds.TemplateBytes(input); err != nil {
		return "", err
	}
	return string(b), nil
}

func (ds *DataSource) CreateAliases() {
	ds.Location.CreateAlias()
	// fmt.Printf("ds table %s, alias %s\n", ds.Location.Table, ds.Location.Alias)
	for i := range ds.Operations {
		ds.Operations[i].Source.CreateAliases()
	}
}

func (ds *DataSource) PropogateAlias() {
	var aliasMap = make(map[string]string)
	// get alias from anchor table
	aliasMap[ds.Location.Table] = ds.Location.Alias
	// create and get alias for each table in operations
	for i, e := range ds.Operations {
		// fmt.Printf("operation table: %s\n", e.Source.Location.Table)
		// ds.Operations[i].CreateAlias()
		aliasMap[e.Source.Location.Table] = ds.Operations[i].Source.Location.Alias
		// fmt.Printf("\talias: %s\n", ds.Operations[i].Source.Location.Alias)
	}

	// apply aliases to select statements
	for i, e := range ds.Select {
		ds.Select[i].Field.TableAlias = aliasMap[e.Field.Table]
	}
	// propograte to operations
	for i := range ds.Operations {
		ds.Operations[i].Source.PropogateAlias()
	}

	// appli aliases to join statements
	for i1, e1 := range ds.Operations {
		for i2, e2 := range e1.Type.JoinOn {
			// left side join
			ds.Operations[i1].Type.JoinOn[i2].Entity.Left.Field.TableAlias = aliasMap[e2.Entity.Left.Field.Table]
			// right side join
			ds.Operations[i1].Type.JoinOn[i2].Entity.Right.Field.TableAlias = aliasMap[e2.Entity.Right.Field.Table]
		}
	}
}

func (ds DataSource) GenerateSQL() (string, error) {
	// ds.Location.CreateAlias()
	// ds.PropogateAlias()
	tmpl := `select {{ .GenerateSQLSelect }}
from {{ .GenerateSQLFrom }}{{ range .Operations }}{{ .GenerateSQL }}{{end}}`
	return ds.TemplateString(tmpl)
}

func (ds DataSource) GenerateSQLSelect() (string, error) {
	tmpl := `{{ $len := sub1 (len .Select) }}{{range $i, $e := .Select}}{{ $e.GenerateSQL }}{{if lt $i $len }}, {{end}}{{end}}`
	return ds.TemplateString(tmpl)
}

func (ds DataSource) GenerateSQLFrom() (string, error) {
	tmpl := `"{{ .Location.Schema }}"."{{ .Location.Table }}" as "{{ .Location.Alias }}"`
	return ds.TemplateString(tmpl)
}
