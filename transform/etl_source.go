package mast

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/rs/xid"
)

type ETLSource struct {
	Select     []FieldTransform       `json:"select"`
	Source     DataSource             `json:"source"`
	Operations []*DataSourceOperation `json:"operations"`
	Alias      string                 `json:"-"`
}

// TemplateBytes will run an input template against a ETLSource object
func (s ETLSource) TemplateBytes(input string) ([]byte, error) {
	var t *template.Template
	var err error
	if t, err = template.New("templateETLSource").Funcs(templateFuncMap).Parse(input); err != nil {
		return nil, err
	}

	var b bytes.Buffer

	if err = t.Execute(&b, s); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// TemplateString will run template against ETLSource object
func (s ETLSource) TemplateString(input string) (string, error) {
	var b []byte
	var err error
	if b, err = s.TemplateBytes(input); err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *ETLSource) CreateAlias() {
	switch test {
	case false:
		s.Alias = fmt.Sprintf("t_%s", xid.New().String())
	case true:
		var tmp strings.Builder
		tmp.WriteString("a_")
		tmp.WriteString(s.Source.Location.Table)
		for _, e := range s.Source.Operations {
			tmp.WriteString("-")
			tmp.WriteString(e.Source.Location.Table)
		}
		s.Alias = tmp.String()
	}
}

func (s *ETLSource) CreateAliases() {
	s.Source.CreateAliases()
	s.Source.PropogateAlias()
	for i := range s.Operations {
		s.Operations[i].Source.CreateAliases()
		s.Operations[i].Source.PropogateAlias()
	}
}

func (s *ETLSource) PropogateAlias() {
	var aliasMap = make(map[string]string)
	// get aliases from base
	// fmt.Printf("operation table: %s, alias '%s'\n", s.Source.Location.Table, s.Source.Location.Alias)
	// aliasMap[s.Source.Location.Table] = s.Source.Location.Alias
	aliasMap[s.Source.Location.Table] = s.Alias
	for i := range s.Source.Operations {
		// fmt.Printf("operation table: %s, alias '%s'\n", s.Source.Operations[i].Source.Location.Table, s.Source.Operations[i].Source.Location.Alias)
		// aliasMap[s.Source.Operations[i].Source.Location.Table] = s.Source.Operations[i].Source.Location.Alias
		aliasMap[s.Source.Operations[i].Source.Location.Table] = s.Alias
	}

	// get alias from operations
	for i := range s.Operations {
		// fmt.Printf("operation table: %s, alias '%s'\n", s.Operations[i].Source.Location.Table, s.Operations[i].Source.Location.Alias)
		aliasMap[s.Operations[i].Source.Location.Table] = s.Operations[i].Source.Location.Alias
	}

	// apply aliases to select
	for i, e := range s.Select {
		s.Select[i].Field.TableAlias = aliasMap[e.Field.Table]
	}

	// fmt.Printf("alias map %+v\n", aliasMap)
	// apply aliases to join fields
	for i1, e1 := range s.Operations {
		for i2, e2 := range e1.Type.JoinOn {
			s.Operations[i1].Type.JoinOn[i2].Entity.Left.Field.TableAlias = aliasMap[e2.Entity.Left.Field.Table]
			s.Operations[i1].Type.JoinOn[i2].Entity.Right.Field.TableAlias = aliasMap[e2.Entity.Right.Field.Table]
		}
	}
}

func (s *ETLSource) GenerateSQL() (string, error) {
	s.CreateAlias()
	s.CreateAliases()
	s.PropogateAlias()
	tmpl := `select {{ .GenerateSQLSelect }}
from (
  {{ .Source.GenerateSQL }}
) as "{{ .Alias }}"{{ range .Operations }}{{ .GenerateSQL }}{{end}}`
	return s.TemplateString(tmpl)
}

func (s ETLSource) GenerateSQLSelect() (string, error) {
	tmpl := `{{ $len := sub1 (len .Select) }}{{range $i, $e := .Select}}{{ $e.GenerateSQL }}{{if lt $i $len }}, {{end}}{{end}}`
	return s.TemplateString(tmpl)
}
