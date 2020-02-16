package mast

import (
	"encoding/json"
	"testing"
)

var testDataDataSource = make(map[string]DataSource)
var tmpDataSource DataSource

func init() {
	var x []byte
	// union
	x = []byte(`{
    "select": [{
				"type": "Field",
				"field": {
					"table": "customer",
					"column": "customer_id"
				}
			},{
				"type": "Field",
				"field": {
					"table": "customer",
					"column": "first_name"
				}
			},{
				"type": "Field",
				"field": {
					"table": "customer",
					"column": "last_name"
				}
			}],
    "location": {
      "database": "pagila",
      "schema": "public",
      "table": "customer"
    },
    "filter": "",
    "operations": [
      {
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
          "operation": []
        }
      }
    ]
  }
`)
	if err := json.Unmarshal(x, &tmpDataSource); err != nil {
		panic(err)
	}
	testDataDataSource["pagila_0"] = tmpDataSource

	// left join
	x = []byte(`{
	  "select": [
	    {
	      "type": "Field",
	      "is_arg": false,
	      "arg_index": null,
	      "field": {
	        "table": "film",
	        "column": "title"
	      },
	      "value": "",
	      "function": "",
	      "args": []
	    },
	    {
	      "type": "Field",
	      "is_arg": false,
	      "arg_index": null,
	      "field": {
	        "table": "language",
	        "column": "name"
	      },
	      "value": "",
	      "function": "",
	      "args": []
	    }
	  ],
	  "location": {
	    "database": "pagila",
	    "schema": "public",
	    "table": "film"
	  },
	  "operations": [
	    {
	      "type": {
	        "method": "join",
	        "modifier": "left",
	        "join_on": [
	          {
	            "entity": {
	              "left": {
	                "type": "Field",
	                "is_arg": false,
	                "arg_index": null,
	                "field": {
	                  "table": "film",
	                  "column": "language_id"
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
	                  "table": "language",
	                  "column": "language_id"
	                },
	                "value": "",
	                "function": "",
	                "args": []
	              },
	              "equality": "="
	            },
	            "operator": ""
	          }
	        ]
	      },
	      "source": {
	        "select": [],
	        "location": {
	          "database": "pagila",
	          "schema": "public",
	          "table": "language"
	        }
	      }
	    }
	  ]
	}`)
	if err := json.Unmarshal(x, &tmpDataSource); err != nil {
		panic(err)
	}
	testDataDataSource["pagila_1"] = tmpDataSource
}

func TestDataSourceGenerateSQL0(t *testing.T) {
	var y DataSource
	x := []byte(`{
    "select": [{
				"type": "Field",
				"field": {
					"table": "customer",
					"column": "customer_id"
				}
			},{
				"type": "Field",
				"field": {
					"table": "customer",
					"column": "first_name"
				}
			},{
				"type": "Field",
				"field": {
					"table": "customer",
					"column": "last_name"
				}
			}],
    "location": {
      "database": "pagila",
      "schema": "public",
      "table": "customer"
    },
    "filter": "",
    "operations": [
      {
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
          "operation": []
        }
      }
    ]
  }
`)
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	var err error

	// test from
	if got, err = y.GenerateSQLFrom(); err != nil {
		t.Error(err)
	}
	expected = `"public"."customer"`
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `select "customer"."customer_id", "customer"."first_name", "customer"."last_name"
from "public"."customer"
union 
select "actor"."actor_id", "actor"."first_name", "actor"."last_name"
from "public"."actor"`

	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL1(t *testing.T) {
	d := testDataDataSource["pagila_1"]
	var got, expected string
	var err error

	if got, err = d.GenerateSQL(); err != nil {
		t.Error(err)
	}
	// fmt.Printf("\n%s\n", got)
	expected = `select "film"."title", "language"."name"
from "public"."film"
left join "public"."language"
	on "film"."language_id" = "language"."language_id"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
