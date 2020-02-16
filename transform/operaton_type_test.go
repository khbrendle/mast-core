package mast

import (
	"encoding/json"
	"testing"
)

var testDataOperationType = make(map[string]OperationType)
var tmpDataOperationType OperationType

func init() {
	var x []byte
	// normal field join
	x = []byte(`{
    "method": "join",
    "modifier": "left",
    "join_on": [{
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
    },{
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
    }]
  }`)
	if err := json.Unmarshal(x, &tmpDataOperationType); err != nil {
		panic(err)
	}
	testDataOperationType["pagila_0"] = tmpDataOperationType
}

func TestOperatonTypeGenerateSQLJoin0(t *testing.T) {
	y := testDataOperationType["pagila_0"]
	var got, expected string
	var err error

	// test full
	if got, err = y.GenerateSQLJoin(); err != nil {
		t.Error(err)
	}
	expected = `"person"."name" = "employee"."name" and "person"."name" = "employee"."name"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
