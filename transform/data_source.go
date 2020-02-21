package mast

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// DataSource contains the operations to create source/subquery
type DataSource struct {
	// query, subquery, or table
	Type       string                 `json:"type"`
	Select     []FieldTransform       `json:"select"`
	From       *DataSource            `json:"from"`
	Location   DataLocation           `json:"location"`
	Filter     string                 `json:"filter"`
	Operations []*DataSourceOperation `json:"operations"`
	Level      int                    `json:"level"`
	AliasMap   map[string]string      `json:"alias_map"`
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

// func (ds *DataSource) CreateAliases() {
// 	ds.Location.CreateAlias()
// 	// fmt.Printf("ds table %s, alias %s\n", ds.Location.Table, ds.Location.Alias)
// 	for i := range ds.Operations {
// 		ds.Operations[i].Source.CreateAliases()
// 	}
// }

// func (ds *DataSource) PropogateAlias() {
// 	var aliasMap = make(map[string]string)
// 	// get alias from anchor table
// 	aliasMap[ds.Location.Table] = ds.Location.Alias
// 	// create and get alias for each table in operations
// 	for i, e := range ds.Operations {
// 		// fmt.Printf("operation table: %s\n", e.Source.Location.Table)
// 		// ds.Operations[i].CreateAlias()
// 		aliasMap[e.Source.Location.Table] = ds.Operations[i].Source.Location.Alias
// 		// fmt.Printf("\talias: %s\n", ds.Operations[i].Source.Location.Alias)
// 	}
//
// 	// apply aliases to select statements
// 	for i, e := range ds.Select {
// 		ds.Select[i].Field.TableAlias = aliasMap[e.Field.Table]
// 	}
// 	// propograte to operations
// 	for i := range ds.Operations {
// 		ds.Operations[i].Source.PropogateAlias()
// 	}
//
// 	// appli aliases to join statements
// 	for i1, e1 := range ds.Operations {
// 		for i2, e2 := range e1.Type.JoinOn {
// 			// left side join
// 			ds.Operations[i1].Type.JoinOn[i2].Entity.Left.Field.TableAlias = aliasMap[e2.Entity.Left.Field.Table]
// 			// right side join
// 			ds.Operations[i1].Type.JoinOn[i2].Entity.Right.Field.TableAlias = aliasMap[e2.Entity.Right.Field.Table]
// 		}
// 	}
// }

func (ds *DataSource) SetAlias() string {
	return ds.Location.CreateAlias()
}

func (ds DataSource) GetTableName() string {
	return ds.Location.Table
}

func (ds *DataSource) AddChildAlias() {
	// initialize map
	if ds.AliasMap == nil {
		ds.AliasMap = make(map[string]string)
	}
	// set alias of base table
	switch ds.Type {
	case "query":
		ds.AliasMap[ds.From.GetTableName()] = ds.From.SetAlias()
	}
	// set alias for operations
	for i, e := range ds.Operations {
		switch e.Source.Type {
		case "table":
			ds.AliasMap[e.GetTableName()] = ds.Operations[i].SetLocationAlias()
		}
	}
}

// maybe propogate alias by FieldTransform or DataSource
// this might make recursion easier
func (ds *DataSource) PropograteAlias() {
	// currently for type query
	var ta string
	// propogate to select statements
	for i, e := range ds.Select {
		ta = ds.AliasMap[e.GetFieldTable()]
		ds.Select[i].SetFieldTableAlias(ta)
	}
	// TODO: propogate to joins
	for i, e := range ds.Operations {
		switch e.Type.Method {
		case "join":

			// TODO: finish this
			for i2, e2 := range ds.Operations[i].Type.JoinOn {
				switch e2.Entity.Type {
				case "Field":
					// left side of equality`
					ta = ds.AliasMap[ds.Operations[i].Type.JoinOn[i2].Entity.GetFieldTable()]
					ds.Operations[i].Type.JoinOn[i2].Entity.SetFieldTableAlias(ta)
					// right side of equality`
					ta = ds.AliasMap[ds.Operations[i].Type.JoinOn[i2].Entity.Equality.Arg.GetFieldTable()]
					ds.Operations[i].Type.JoinOn[i2].Entity.Equality.Arg.SetFieldTableAlias(ta)
				}
			}
		}
	}
}

func (ds *DataSource) SetLevel(l int) {
	ds.Level = l
}

func (ds *DataSource) AddChildLevel() {
	// initial level state for highest level object would be 0 which is set by default on unmarshal of json
	// add level to single child
	switch ds.Type {
	case "query":
		switch ds.From.Type {
		case "subquery":
			ds.From.SetLevel(ds.Level + 1)
		}
	case "subquery":
		switch ds.From.Type {
		case "table":
			ds.From.SetLevel(ds.Level)
		}
	}

	// add levels to operations
	for i, e := range ds.Operations {
		switch e.Type.Method {
		case "union":
			ds.Operations[i].Source.SetLevel(ds.Level)
		default:
			ds.Operations[i].Source.SetLevel(ds.Level + 1)
		}
		ds.Operations[i].SetLevel(ds.Level)
	}
}

var templateMap = map[string]string{
	"table": `{{ .Location.GenerateSQL }}{{ range .Operations }}{{ .GenerateSQL }}{{end}}`,
	"query": `{{ levelSpaces .Level }}select {{ range $i, $e := .Select }}{{if gt $i 0}}, {{end}}{{ $e.GenerateSQL }}{{end}}
{{ levelSpaces .Level }}from {{ .From.GenerateSQL }}{{ range .Operations }}{{ .GenerateSQL }}{{end}}`,
}

func (ds DataSource) GenerateSQL() (string, error) {
	ds.AddChildAlias()
	ds.PropograteAlias()
	ds.AddChildLevel()
	var tmpl string

	switch ds.Type {
	case "table":
		fmt.Println("generating table")
		tmpl = templateMap["table"]
	case "query":
		fmt.Println("generating query")
		tmpl = templateMap["query"]
	case "subquery":
		fmt.Println("generating subquery")
		tmpl = fmt.Sprintf("(\n%s\n%s)", templateMap["query"], strings.Repeat("  ", ds.Level-1))
	}

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
