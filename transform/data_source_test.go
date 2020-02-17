package mast

import (
	"encoding/json"
	"testing"
)

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
	expected = `"public"."customer" as ""`
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `select "a_customer"."customer_id", "a_customer"."first_name", "a_customer"."last_name"
from "public"."customer" as "a_customer"
union
select "a_actor"."actor_id", "a_actor"."first_name", "a_actor"."last_name"
from "public"."actor" as "a_actor"`

	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL1(t *testing.T) {
	var d DataSource
	x := []byte(`{
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
	var err error
	if err := json.Unmarshal(x, &d); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = d.GenerateSQL(); err != nil {
		t.Error(err)
	}
	// fmt.Printf("\n%s\n", got)
	expected = `select "a_film"."title", "a_language"."name"
from "public"."film" as "a_film"
left join "public"."language" as "a_language"
	on "a_film"."language_id" = "a_language"."language_id"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL2(t *testing.T) {
	var d DataSource
	x := []byte(`{
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
	var err error
	if err := json.Unmarshal(x, &d); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = d.GenerateSQL(); err != nil {
		t.Error(err)
	}

	expected = `select "a_film"."title", "a_language"."name"
from "public"."film" as "a_film"
left join "public"."language" as "a_language"
	on "a_film"."language_id" = "a_language"."language_id"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
