package mast

import (
	"encoding/json"
	"testing"
)

func TestJoinGroupGenerateSQL0(t *testing.T) {
	var y JoinGroup
	x := []byte(`{
    "left": {
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
    },
    "right": {
      "type": "Field",
      "is_arg": false,
      "arg_index": null,
      "field": {
        "table": "employee",
        "column": "name"
      },
      "value": "",
      "function": "",
      "args": []
    },
    "equality": "="
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
	var got, expected string

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `"person"."name" = "employee"."name"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

// function join
func TestJoinGroupGenerateSQL1(t *testing.T) {
	var y JoinGroup
	x := []byte(`{
    "left": {
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
    },
    "right": {},
    "equality": ""
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
	var got, expected string

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `array_contains("person"."name_array", "employee"."name")`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
