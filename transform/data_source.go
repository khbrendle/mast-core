package mast

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// DataSource contains the operations to create source/subquery
type DataSource struct {
	// 'query', 'subquery', or 'table'
	Type string `json:"type"`
	// list of columns to select from the source
	// only used in query or subquery
	Select []FieldTransform `json:"select"`
	// from is used for subqueries and queries
	From *DataSource `json:"from"`
	//
	Location   DataLocation           `json:"location,omitempty"`
	Filter     string                 `json:"filter,omitempty"`
	Operations []*DataSourceOperation `json:"operations,omitempty"`
	Level      int                    `json:"level,omitempty"`
	// alias should only exist on subquery
	Alias    string            `json:"alias,omitempty"`
	AliasMap map[string]string `json:"alias_map,omitempty"`
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
		switch ds.From.Type {
		case "table":
			ds.AliasMap[ds.From.GetTableName()] = ds.From.Location.Alias
		case "subquery":
			ds.AliasMap[ds.From.GetTableName()] = ds.From.Alias
		}
	case "subquery":
		ds.AliasMap[ds.From.GetTableName()] = ds.Alias
	}
	fmt.Printf("alias_map: %+v\n", ds.AliasMap)
	// set alias for operations
	for i, e := range ds.Operations {
		switch e.Source.Type {
		case "table":
			// ds.AliasMap[e.GetTableName()] = ds.Operations[i].SetLocationAlias()
			ds.AliasMap[e.GetTableName()] = ds.Operations[i].Source.Location.Alias
		}
	}
}

// func (ds *DataSource) GetChildAlias() {
// 	// initialize map
// 	if ds.AliasMap == nil {
// 		ds.AliasMap = make(map[string]string)
// 	}
// 	// set alias of base table
// 	switch ds.Type {
// 	case "query":
// 		switch ds.From.Type {
// 		case "table":
// 			ds.AliasMap[ds.From.GetTableName()] = ds.From.Location.Alias
// 		case "subquery":
// 			ds.AliasMap[ds.From.GetTableName()] = ds.From.Alias
// 		}
// 	case "subquery":
// 		ds.AliasMap[ds.From.GetTableName()] = ds.Alias
// 	}
// 	// set alias for operations
// 	for i, e := range ds.Operations {
// 		switch e.Source.Type {
// 		case "table":
// 			ds.AliasMap[e.GetTableName()] = ds.Operations[i].Source.Location.Alias
// 		}
// 	}
// }

// maybe propogate alias by FieldTransform or DataSource
// this might make recursion easier
// func (ds *DataSource) PropograteAlias() {
// 	// currently for type query
// 	var ta string
// 	// propogate to select statements
// 	for i, e := range ds.Select {
// 		// ta = ds.AliasMap[e.GetFieldTable()]
// 		// ds.Select[i].SetFieldTableAlias(ta)
//
// 	}
// 	// TODO: propogate to joins
// 	for i, e := range ds.Operations {
// 		switch e.Type.Method {
// 		case "join":
// 			// TODO: finish this
// 			for i2, e2 := range ds.Operations[i].Type.JoinOn {
// 				switch e2.Entity.Type {
// 				case "Field":
// 					// left side of equality`
// 					ta = ds.AliasMap[ds.Operations[i].Type.JoinOn[i2].Entity.GetFieldTable()]
// 					ds.Operations[i].Type.JoinOn[i2].Entity.SetFieldTableAlias(ta)
// 					// right side of equality`
// 					ta = ds.AliasMap[ds.Operations[i].Type.JoinOn[i2].Entity.Equality.Arg.GetFieldTable()]
// 					ds.Operations[i].Type.JoinOn[i2].Entity.Equality.Arg.SetFieldTableAlias(ta)
// 				}
// 			}
// 		}
// 	}
// }

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

var sqlTemplateMap = map[string]string{
	"table": `{{ .Location.GenerateSQL }}{{ range .Operations }}{{ .GenerateSQL }}{{end}}`,
	"query": `{{ levelSpaces .Level }}select {{ range $i, $e := .Select }}{{if gt $i 0}}, {{end}}{{ $e.GenerateSQL }}{{end}}
{{ levelSpaces .Level }}from {{ .From.GenerateSQL }}{{ range .Operations }}{{ .GenerateSQL }}{{end}}`,
}

func (ds DataSource) GenerateSQL() (string, error) {
	// ds.GetChildAlias()
	// fmt.Printf("alias_map: %+v\n", ds.AliasMap)
	// ds.PropograteAlias()
	ds.AddChildLevel()
	var tmpl string

	switch ds.Type {
	case "table":
		fmt.Println("generating table")
		tmpl = sqlTemplateMap["table"]
	case "query":
		fmt.Println("generating query")
		tmpl = sqlTemplateMap["query"]
	case "subquery":
		fmt.Println("generating subquery")
		tmpl = fmt.Sprintf("(\n%s\n%s) as \"%s\"", sqlTemplateMap["query"], strings.Repeat("  ", ds.Level-1), ds.Alias)
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

var pysparkTemplateMap = map[string]string{
	"table": `{{ .Location.GeneratePySpark }}`,
	"query": `{{ .From.GeneratePySpark }}{{ range .Operations }}{{ .GeneratePySpark }}{{ end }}{{ .GeneratePySparkSelect }}`,
	// "query": `{{ .From.GeneratePySpark }}{{ .GeneratePySparkSelect }}{{ range .Operations }}{{ .GeneratePySpark }}{{ end }}`,
}

func (ds DataSource) GeneratePySpark() (string, error) {
	var tmpl string
	// when joining can join then select, for union need to select from first table then union
	// if ds.Operations != nil {
	// 	var s strings.Builder
	// 	fmt.Println("operations exist")
	// 	fmt.Println()
	// 	// s.WriteString(`{{ .From.GeneratePySpark }}`) // this isn't working, maybe works with union but not join
	// 	// s.WriteString(`{{ .Location.GeneratePySpark }}`)
	// 	for _, e := range ds.Operations {
	// 		fmt.Printf("%s operation: %+v\n", e.Type.Method, e)
	// 		if e.Type.Method == "union" {
	// 			s.WriteString(`{{ .From.GeneratePySpark }}.select({{ .GeneratePySparkSelect }}){{ range .Operations }}{{ .GeneratePySpark }}{{ end }}`)
	// 		} else if e.Type.Method == "join" {
	// 			s.WriteString(`{{ .Location.GeneratePySpark }}{{ range .Operations }}{{ .GeneratePySpark }}{{ end }}{{ .GeneratePySparkSelect }}`)
	// 		}
	// 	}
	// 	fmt.Printf("executing template:\n\n%s\n", tmpl)
	// 	return ds.TemplateString(s.String())
	// }
	// if ds.Operations != nil {
	//
	// }
	// based on type, generate call
	switch ds.Type {
	case "table":
		fmt.Println("generating table")
		tmpl = pysparkTemplateMap["table"]
	case "query":
		fmt.Println("generating query")
		tmpl = pysparkTemplateMap["query"]
	case "subquery":
		fmt.Println("generating subquery")
		tmpl = pysparkTemplateMap["query"]
	}

	fmt.Printf("executing template:\n\n%v\n\n", tmpl)
	return ds.TemplateString(tmpl)
}

func (ds DataSource) GeneratePySparkSelect() (string, error) {
	var tmpl string
	if len(ds.Select) > 0 {
		tmpl = `.select({{ $len := sub1 (len .Select) }}{{range $i, $e := .Select}}{{ $e.GeneratePySpark }}{{if lt $i $len }}, {{end}}{{end}})`
	}
	return ds.TemplateString(tmpl)
}
