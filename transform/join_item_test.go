package mast

import (
	"encoding/json"
	"testing"
)

var testDataJoinItem = make(map[string]JoinItem)
var tmpDataJoinItem JoinItem

func init() {
	var x []byte
	// single field compare
	x = []byte(`{
    "entity": {
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
    },
    "operator": ""
  }`)
	if err := json.Unmarshal(x, &tmpDataJoinItem); err != nil {
		panic(err)
	}
	testDataJoinItem["pagila_0"] = tmpDataJoinItem

	// with operator
	x = []byte(`{
    "entity": {
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
    },
    "operator": "and"
  }`)
	if err := json.Unmarshal(x, &tmpDataJoinItem); err != nil {
		panic(err)
	}
	testDataJoinItem["pagila_1"] = tmpDataJoinItem
}

func TestJoinItemGenerateSQL0(t *testing.T) {
	y := testDataJoinItem["pagila_0"]
	var got, expected string
	var err error

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `"person"."name" = "employee"."name"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestJoinItemGenerateSQL1(t *testing.T) {
	y := testDataJoinItem["pagila_1"]
	var got, expected string
	var err error

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `and "person"."name" = "employee"."name"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
