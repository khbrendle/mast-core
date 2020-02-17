package mast

import (
	"encoding/json"
	"testing"
)

func TestDataSourceOperationGenerateSQL0(t *testing.T) {
	var y DataSourceOperation
	x := []byte(`{
    "type": {
      "method": "union"
    },
    "source": {
      "select": [{
          "type": "Field",
          "field": {
            "table": "actor",
            "column": "actor_id"
          }
        },{
          "type": "Field",
          "field": {
            "table": "actor",
            "column": "first_name"
          }
        },{
          "type": "Field",
          "field": {
            "table": "actor",
            "column": "last_name"
          }
        }],
      "location": {
        "database": "pagila",
        "schema": "public",
        "table": "actor"
      },
      "filter": "",
      "operations": []
    }
  }
`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
	// fmt.Printf("%+v\n", y)
	var got, expected string

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `
union
select "a_actor"."actor_id", "a_actor"."first_name", "a_actor"."last_name"
from "public"."actor" as "a_actor"`

	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceOperationGenerateSQL1(t *testing.T) {
	var y DataSourceOperation
	x := []byte(`{
			"type": {
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
		  },
		"source": {
			"select": [],
			"location": {
        "database": "pagila",
        "schema": "public",
        "table": "actor"
      }
		}
	}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
	// fmt.Printf("%+v\n", y)
	var got, expected string

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `
left join "public"."actor" as ""
	on "person"."name" = "employee"."name" and "person"."name" = "employee"."name"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
