package mast

import (
	"bytes"
	"encoding/json"
	"fmt"
	"text/template"
)

type FieldTransform struct {
	Type         string            `json:"type"`
	IsArg        bool              `json:"is_arg"`
	ArgIndex     int               `json:"arg_index"`
	Field        Field             `json:"field"`
	Value        json.RawMessage   `json:"value"`
	Function     string            `json:"function"`
	Args         []*FieldTransform `json:"args"`
	ChainMethods []*FieldTransform `json:"chain_methods"`
	// for doing equality comprisons, if there is an equality then the
	// result should be compared to Args[0]
	Equality Equality `json:"equality"`
	Alias    string   `json:"alias"`
}

// TemplateBytes executes an input template against FieldTransform object returning byte array
func (ft FieldTransform) TemplateBytes(input string) ([]byte, error) {
	var tmpl *template.Template
	var err error
	if tmpl, err = template.New("templateFieldTransform").Parse(input); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	if err = tmpl.Execute(&b, ft); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// TemplateString executes an input template against FieldTransform object returning string
func (ft FieldTransform) TemplateString(tmpl string) (string, error) {
	var b []byte
	var err error
	if b, err = ft.TemplateBytes(tmpl); err != nil {
		return "", err
	}
	return string(b), nil
}

func (ft *FieldTransform) SetFieldTableAlias(x string) {
	ft.Field.SetTableAlias(x)
}

func (ft FieldTransform) GetFieldTable() string {
	return ft.Field.GetTable()
}

// GenerateSQL creates the SQL expression for the field transformation
func (ft FieldTransform) GenerateSQL() (string, error) {
	var err error
	var s string
	switch ft.Type {
	case "Field":
		if s, err = ft.Field.GenerateSQL(); err != nil {
			return "", err
		}
	case "Value":
		if s, err = ft.GenerateSQLValue(); err != nil {
			return "", err
		}
	case "Function":
		if s, err = ft.GenerateSQLFunction(); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unexpected FieldTransformation type '%s'", ft.Type)
	}
	// add equality
	// fmt.Printf("equality: %+v\n", ft.Equality)
	if (ft.Equality != Equality{}) {
		var eqa string
		if eqa, err = ft.Equality.GenerateSQL(); err != nil {
			return "", err
		}
		s = fmt.Sprintf(`%s %s`, s, eqa)
	}
	tmpl := fmt.Sprintf(`%s{{ if ne .Alias "" }} as "{{ .Alias }}"{{end}}`, s)
	return ft.TemplateString(tmpl)
}

func (ft FieldTransform) GenerateSQLValue() (string, error) {
	var err error
	var vInt int
	if err = json.Unmarshal(ft.Value, &vInt); err != nil {
		var vFloat float64
		if err = json.Unmarshal(ft.Value, &vFloat); err != nil {
			var vString string
			if err = json.Unmarshal(ft.Value, &vString); err != nil {
				return "", err
			} else {
				return fmt.Sprintf(`'%s'`, vString), nil
			}
		} else {
			return fmt.Sprintf("%f", vFloat), nil
		}
	} else {
		return fmt.Sprintf("%d", vInt), nil
	}
}

func (ft FieldTransform) GenerateSQLFunction() (string, error) {
	tmpl := `{{ .Function }}({{ range $i, $e := .Args }}{{ if gt $i 0 }}, {{end}}{{ .GenerateSQL }}{{end}})`
	return ft.TemplateString(tmpl)
}

// GenerateSQL creates the PySpark expression for the field transformation
func (ft FieldTransform) GeneratePySpark() (string, error) {
	var err error
	var s string
	switch ft.Type {
	case "Field":
		if s, err = ft.Field.GeneratePySpark(); err != nil {
			return "", err
		}
	case "Value":
		if s, err = ft.GeneratePySparkValue(); err != nil {
			return "", err
		}
	case "Function":
		if s, err = ft.GeneratePySparkFunction(); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unexpected FieldTransformation type '%s'", ft.Type)
	}
	if (ft.Equality != Equality{}) {
		var eqa string
		if eqa, err = ft.Equality.GeneratePySpark(); err != nil {
			return "", err
		}
		s = fmt.Sprintf(`%s %s`, s, eqa)
	}
	tmpl := fmt.Sprintf(`%s{{ if ne .Alias "" }}.alias("{{ .Alias }}"){{end}}`, s)
	return ft.TemplateString(tmpl)
}

// this could probably be more combined with the SQL version
func (ft FieldTransform) GeneratePySparkValue() (string, error) {
	var err error
	var vInt int
	if err = json.Unmarshal(ft.Value, &vInt); err != nil {
		var vFloat float64
		if err = json.Unmarshal(ft.Value, &vFloat); err != nil {
			var vString string
			if err = json.Unmarshal(ft.Value, &vString); err != nil {
				return "", err
			} else {
				return fmt.Sprintf(`F.lit('%s')`, vString), nil
			}
		} else {
			return fmt.Sprintf("F.lit(%f)", vFloat), nil
		}
	} else {
		return fmt.Sprintf("F.lit(%d)", vInt), nil
	}
}

func (ft FieldTransform) GeneratePySparkFunction() (string, error) {
	tmpl := `{{ .Function }}({{ range $i, $e := .Args }}{{ if gt $i 0 }}, {{end}}{{ .GeneratePySpark }}{{end}}){{ range .ChainMethods }}{{ .GeneratePySparkFunction }}{{end}}`
	return ft.TemplateString(tmpl)
}
