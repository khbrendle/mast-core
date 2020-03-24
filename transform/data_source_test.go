package mast

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDataSourceGenerateSQL0(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","field":{"table":"customer","column":"first_name"}},{"type":"Field","field":{"table":"customer","column":"last_name"}}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"customer","alias":"a_customer"}}}`)
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	var err error

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}

	expected = `select "a_customer"."first_name", "a_customer"."last_name"
from "public"."customer" as "a_customer"`

	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL1(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table_alias":"a_film","column":"title"},"value":"","function":"","args":[]},{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"language","table_alias":"a_language","column":"name"},"value":"","function":"","args":[]}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"film","alias":"a_film"}},"operations":[{"type":{"method":"join","modifier":"left","join_on":[{"entity":{"type":"Field","is_arg":false,"field":{"table":"film","table_alias":"a_film","column":"language_id"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"field":{"table":"language","table_alias":"a_language","column":"language_id"}}}}}]},"source":{"type":"table","location":{"database":"pagila","schema":"public","table":"language","alias":"a_language"}}}]}`)
	var err error
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}

	// fmt.Printf("\n%s\n", got)
	expected = `select "a_film"."title", "a_language"."name"
from "public"."film" as "a_film"
left join "public"."language" as "a_language"
  on "a_film"."language_id" = "a_language"."language_id"`
	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL2(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","field":{"table":"customer","table_alias":"a_customer","column":"customer_id"}},{"type":"Field","field":{"table":"customer","table_alias":"a_customer","column":"first_name"}},{"type":"Field","field":{"table":"customer","table_alias":"a_customer","column":"last_name"}}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"customer","alias":"a_customer"}},"filter":"","operations":[{"type":{"method":"union"},"source":{"type":"query","select":[{"type":"Field","field":{"table":"actor","table_alias":"a_actor","column":"actor_id"}},{"type":"Field","field":{"table":"actor","table_alias":"a_actor","column":"first_name"}},{"type":"Field","field":{"table":"actor","table_alias":"a_actor","column":"last_name"}}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"actor","alias":"a_actor"}},"filter":"","operation":[]}}]}`)
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	var err error

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}

	expected = `select "a_customer"."customer_id", "a_customer"."first_name", "a_customer"."last_name"
from "public"."customer" as "a_customer"
union
select "a_actor"."actor_id", "a_actor"."first_name", "a_actor"."last_name"
from "public"."actor" as "a_actor"`

	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL3(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table_alias":"t1","column":"title"},"value":"","function":"","args":[]},{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"language","table_alias":"a_language","column":"name"},"value":"","function":"","args":[]}],"from":{"type":"subquery","select":[{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table":"a_film","column":"title"},"value":"","function":"","args":[]},{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table":"a_film","column":"language_id"},"value":"","function":"","args":[]},{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"category","table_alias":"a_category","column":"name"},"value":"","function":"","args":[]}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"film","alias":"a_film"},"operations":[{"type":{"method":"join","join_on":[{"entity":{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table_alias":"a_film","column":"film_id"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film_category","table_alias":"a_film_category","column":"film_id"},"value":"","function":"","args":[]}}},"operator":""}]},"source":{"type":"table","location":{"database":"pagila","schema":"public","table":"film_category","alias":"a_film_category"}}},{"type":{"method":"join","join_on":[{"entity":{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film_category","table_alias":"a_film_category","column":"category_id"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"category","table_alias":"a_category","column":"category_id"},"value":"","function":"","args":[]}}},"operator":""}]},"source":{"type":"table","location":{"database":"pagila","schema":"public","table":"category","alias":"a_category"}}}]},"alias":"t1"},"operations":[{"type":{"method":"join","modifier":"left","join_on":[{"entity":{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table_alias":"t1","column":"language_id"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"language","table_alias":"a_language","column":"language_id"},"value":"","function":"","args":[]}}},"operator":""}]},"source":{"type":"table","location":{"database":"pagila","schema":"public","table":"language","alias":"a_language"}}}]}`)
	var err error
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}

	expected = `select "t1"."title", "a_language"."name"
from (
  select "a_film"."title", "a_film"."language_id", "a_category"."name"
  from "public"."film" as "a_film"
  join "public"."film_category" as "a_film_category"
    on "a_film"."film_id" = "a_film_category"."film_id"
  join "public"."category" as "a_category"
    on "a_film_category"."category_id" = "a_category"."category_id"
) as "t1"
left join "public"."language" as "a_language"
  on "t1"."language_id" = "a_language"."language_id"`
	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGenerateSQL4(t *testing.T) {
	var y DataSource
	x := []byte(`{
	  "type": "table",
	  "location": {
	    "database": "pagila",
	    "schema": "public",
	    "table": "customer",
			"alias": "t1"
	  }
	}`)
	var err error
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	// fmt.Println(got)
	expected = `"public"."customer" as "t1"`
	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGeneratePySpark4(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"table","location":{"database":"pagila","schema":"public","table":"customer","alias":"t1"}}`)
	var err error
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	// fmt.Println(got)
	expected = `df_pagila_public_customer`
	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGeneratePySpark0(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","field":{"table":"customer","table":"a_customer","column":"first_name"}},{"type":"Field","field":{"table":"customer","table":"a_customer","column":"last_name"}}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"customer","alias":"a_customer"}}}`)
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	var err error

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}

	expected = `df_pagila_public_customer.select(F.col("a_customer_first_name"), F.col("a_customer_last_name"))`

	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGeneratePySpark1(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table_alias":"a_film","column":"title"},"value":"","function":"","args":[]},{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"language","table_alias":"a_language","column":"name"},"value":"","function":"","args":[]}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"film","alias":"a_film"},"operations":[{"type":{"method":"join","modifier":"left","join_on":[{"entity":{"type":"Field","is_arg":false,"field":{"table":"film","table_alias":"a_film","column":"language_id"},"equality":{"operator":"==","arg":{"type":"Field","is_arg":false,"field":{"table":"language","table_alias":"a_language","column":"language_id"}}}}}]},"source":{"type":"table","location":{"database":"pagila","schema":"public","table":"language","alias":"a_language"}}}]}}`)
	var err error
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}

	// fmt.Printf("\n%s\n", got)
	expected = `df_pagila_public_film.join(df_pagila_public_language, on = F.col("a_film_language_id") == F.col("a_language_language_id"), how = "left").select(F.col("a_film_title"), F.col("a_language_name"))`
	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGeneratePySpark2(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","from":{"type":"query","select":[{"type":"Field","field":{"table":"customer","table_alias":"a_customer","column":"customer_id"}},{"type":"Field","field":{"table":"customer","table_alias":"a_customer","column":"first_name"}},{"type":"Field","field":{"table":"customer","table_alias":"a_customer","column":"last_name"}}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"customer","alias":"a_customer"}}},"filter":"","operations":[{"type":{"method":"union"},"source":{"type":"query","select":[{"type":"Field","field":{"table":"actor","table_alias":"a_actor","column":"actor_id"}},{"type":"Field","field":{"table":"actor","table_alias":"a_actor","column":"first_name"}},{"type":"Field","field":{"table":"actor","table_alias":"a_actor","column":"last_name"}}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"actor","alias":"a_actor"}},"filter":"","operation":[]}}]}`)
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	var err error

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	fmt.Println(got)
	expected = `df_pagila_public_customer.select(F.col("a_customer_customer_id"), F.col("a_customer_first_name"), F.col("a_customer_last_name")).union(df_pagila_public_actor.select(F.col("a_actor_actor_id"), F.col("a_actor_first_name"), F.col("a_actor_last_name")))`

	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}

func TestDataSourceGeneratePySpark3(t *testing.T) {
	var y DataSource
	x := []byte(`{"type":"query","select":[{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"film","table_alias":"t1","column":"title"},"value":"","function":"","args":[]},{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"language","table_alias":"a_language","column":"name"},"value":"","function":"","args":[]}],"from":{"type":"table","location":{"database":"pagila","schema":"public","table":"film"}},"operations":[{"type":{"method":"join","modifier":"left","join_on":[{"entity":{"type":"Field","is_arg":false,"field":{"table":"film","table_alias":"a_film","column":"language_id"},"equality":{"operator":"==","arg":{"type":"Field","is_arg":false,"field":{"table":"language","table_alias":"a_language","column":"language_id"}}}}}]},"source":{"type":"query","from":{"type":"table","location":{"database":"pagila","schema":"public","table":"film","alias":"a_film"}},"operations":[{"type":{"method":"join","modifier":"left","join_on":[{"entity":{"type":"Field","is_arg":false,"field":{"table":"film","table_alias":"a_film","column":"language_id"},"equality":{"operator":"==","arg":{"type":"Field","is_arg":false,"field":{"table":"language","table_alias":"a_language","column":"language_id"}}}}}]},"source":{"type":"table","location":{"database":"pagila","schema":"public","table":"language","alias":"a_language"}}}]}}]}`)
	var err error
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}

	expected = `df_pagila_public_film.join(df_pagila_public_film.join(df_pagila_public_language, on = F.col("a_film_language_id") == F.col("a_language_language_id"), how = "left"), on = F.col("a_film_language_id") == F.col("a_language_language_id"), how = "left").select(F.col("t1_title"), F.col("a_language_name"))`
	if got != expected {
		var b []byte
		if b, err = json.Marshal(y); err != nil {
			t.Error(err)
		}
		fmt.Println(string(b))
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
