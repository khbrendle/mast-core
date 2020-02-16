package mast

import (
	"encoding/json"
	"testing"
)

func TestGenerateSQLFieldTransform0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{
    "type": "Field",
    "is_arg": false,
    "arg_index": null,
    "field": {
      "table": "person",
      "column": "name"
    },
    "value": "",
    "function": "",
    "args": []
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `"person"."name"`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestGenerateSQLValueString0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{
    "type": "Value",
    "is_arg": false,
    "arg_index": null,
    "field": {
      "table": "",
      "column": ""
    },
    "value": "static text",
    "function": "",
    "args": []
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `'static text'`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestGenerateSQLValueInt0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{
    "type": "Value",
    "is_arg": false,
    "arg_index": null,
    "field": {
      "table": "",
      "column": ""
    },
    "value": 1,
    "function": "",
    "args": []
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `1`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestGenerateSQLValueFloat0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{
    "type": "Value",
    "is_arg": false,
    "arg_index": null,
    "field": {
      "table": "",
      "column": ""
    },
    "value": 1.5,
    "function": "",
    "args": []
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `1.500000`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestGenerateSQLValueAlias0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{
    "type": "Value",
    "is_arg": false,
    "arg_index": null,
    "field": {
      "table": "",
      "column": ""
    },
    "value": "static text",
    "function": "",
    "args": [],
		"alias": "text_col"
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `'static text' as text_col`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLFunction0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{
		"type": "Function",
		"is_arg": false,
		"arg_index": null,
		"field": {
			"table": "",
			"column": ""
		},
		"value": "",
		"function": "array_contains",
		"args": [{
			"type": "Field",
			"is_arg": true,
			"arg_index": 0,
			"field": {
				"table": "person",
				"column": "name_array"
			},
			"value": "",
			"function": "",
			"args": []
		},{
			"type": "Field",
			"is_arg": true,
			"arg_index": 1,
			"field": {
				"table": "employee",
				"column": "name"
			},
			"value": "",
			"function": "",
			"args": []
		}]
	}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `array_contains("person"."name_array", "employee"."name")`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}
