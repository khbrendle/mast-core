package mast

import (
	"encoding/json"
	"testing"
)

func TestETLSourceGenerateSQL0(t *testing.T) {
	x := []byte(`{
    "select": [
      {
        "type": "Field",
        "field": {
          "table": "customer",
          "column": "src_table"
        }
      },
      {
        "type": "Field",
        "field": {
          "table": "customer",
          "column": "first_name"
        }
      },
      {
        "type": "Field",
        "field": {
          "table": "staff",
          "column": "last_name"
        }
      },
      {
        "type": "Field",
        "field": {
          "table": "address",
          "column": "address"
        }
      }
    ],
    "source": {
      "select": [
        {
          "type": "Value",
          "field": {
            "table": "",
            "column": ""
          },
          "value": "customer",
          "alias": "src_table"
        },
        {
          "type": "Field",
          "field": {
            "table": "customer",
            "column": "first_name"
          }
        },
        {
          "type": "Field",
          "field": {
            "table": "customer",
            "column": "last_name"
          }
        },
        {
          "type": "Field",
          "field": {
            "table": "customer",
            "column": "address_id"
          }
        }
      ],
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
            "select": [
              {
                "type": "Value",
                "field": {
                  "table": "",
                  "column": ""
                },
                "value": "staff",
                "alias": "src_table"
              },
              {
                "type": "Field",
                "field": {
                  "table": "staff",
                  "column": "first_name"
                }
              },
              {
                "type": "Field",
                "field": {
                  "table": "staff",
                  "column": "last_name"
                }
              },
              {
                "type": "Field",
                "field": {
                  "table": "staff",
                  "column": "address_id"
                }
              }
            ],
            "location": {
              "database": "pagila",
              "schema": "public",
              "table": "staff"
            },
            "filter": "",
            "operation": []
          }
        }
      ]
    },
    "operations": [{
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
  		            "table": "customer",
  		            "column": "address_id"
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
  		            "table": "address",
  		            "column": "address_id"
  		          },
  		          "value": "",
  		          "function": "",
  		          "args": []
  		        },
  		        "equality": "="
  		      },
  		      "operator": ""
  		    }]
  		  },
  		"source": {
  			"select": [],
  			"location": {
          "database": "pagila",
          "schema": "public",
          "table": "address"
        }
  		}
  	}]
  }`)

	var y ETLSource
	var err error

	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}

	expected = `select "a_customer-staff"."src_table", "a_customer-staff"."first_name", "a_customer-staff"."last_name", "a_address"."address"
from (
  select 'customer' as "src_table", "a_customer"."first_name", "a_customer"."last_name", "a_customer"."address_id"
from "public"."customer" as "a_customer"
union
select 'staff' as "src_table", "a_staff"."first_name", "a_staff"."last_name", "a_staff"."address_id"
from "public"."staff" as "a_staff"
) as "a_customer-staff"
left join "public"."address" as "a_address"
	on "a_customer-staff"."address_id" = "a_address"."address_id"`

	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestETLSourceGenerateSQL1(t *testing.T) {
	x := []byte(`{
	  "select": [
	    {
	      "type": "Field",
	      "field": {
	        "table": "customer",
	        "column": "src_table"
	      }
	    },
	    {
	      "type": "Field",
	      "field": {
	        "table": "customer",
	        "column": "first_name"
	      }
	    },
	    {
	      "type": "Field",
	      "field": {
	        "table": "staff",
	        "column": "last_name"
	      }
	    },
	    {
	      "type": "Field",
	      "field": {
	        "table": "address",
	        "column": "address"
	      }
	    }
	  ],
	  "source": {
	    "select": [
	      {
	        "type": "Value",
	        "field": {
	          "table": "",
	          "column": ""
	        },
	        "value": "customer",
	        "alias": "src_table"
	      },
	      {
	        "type": "Field",
	        "field": {
	          "table": "customer",
	          "column": "first_name"
	        }
	      },
	      {
	        "type": "Field",
	        "field": {
	          "table": "customer",
	          "column": "last_name"
	        }
	      },
	      {
	        "type": "Field",
	        "field": {
	          "table": "address",
	          "column": "address"
	        }
	      }
	    ],
	    "location": {
	      "database": "pagila",
	      "schema": "public",
	      "table": "customer"
	    },
	    "filter": "",
	    "operations": [
	      {
	        "type": {
	          "method": "join",
	          "join_on": [{
	            "entity": {
	              "left": {
	                "type": "Field",
	                "is_arg": false,
	                "arg_index": null,
	                "field": {
	                  "table": "customer",
	                  "column": "address_id"
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
	                  "table": "address",
	                  "column": "address_id"
	                },
	                "value": "",
	                "function": "",
	                "args": []
	              },
	              "equality": "="
	            },
	            "operator": ""
	          }]
	        },
	        "source": {
	          "select": [],
	          "location": {
	            "database": "pagila",
	            "schema": "public",
	            "table": "address"
	          },
	          "filter": "",
	          "operation": []
	        }
	      }
	    ]
	  },
	  "operations": [{
	      "type": {
	        "method": "union"
	      },
	    "source": {
	      "select": [
	        {
	        "type": "Value",
	        "field": {
	          "table": "",
	          "column": ""
	        },
	        "value": "staff",
	        "alias": "src_table"
	      },
	      {
	        "type": "Field",
	        "field": {
	          "table": "staff",
	          "column": "first_name"
	        }
	      },
	      {
	        "type": "Field",
	        "field": {
	          "table": "staff",
	          "column": "last_name"
	        }
	      },
	      {
	        "type": "Field",
	        "field": {
	          "table": "address",
	          "column": "address"
	        }
	      }],
	      "location": {
	        "database": "pagila",
	        "schema": "public",
	        "table": "staff"
	      }
	    }
	  },{
	    "type": {
	      "method": "join",
	      "join_on": [{
	        "entity": {
	          "left": {
	            "type": "Field",
	            "is_arg": false,
	            "arg_index": null,
	            "field": {
	              "table": "staff",
	              "column": "address_id"
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
	              "table": "address",
	              "column": "address_id"
	            },
	            "value": "",
	            "function": "",
	            "args": []
	          },
	          "equality": "="
	        },
	        "operator": ""
	      }]
	    },
	    "source": {
	      "select": [],
	      "location": {
	        "database": "pagila",
	        "schema": "public",
	        "table": "address"
	      },
	      "filter": "",
	      "operation": []
	    }
	  }]
	}`)

	var y ETLSource
	var err error

	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	// var b []byte
	// if b, err = json.Marshal(y); err != nil {
	// 	t.Error(err)
	// }
	// fmt.Println(string(b))

	var got, expected string
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}

	expected = ``

	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
