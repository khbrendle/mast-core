package mast

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDataSourceGenerateSQL_0(t *testing.T) {
	x := []byte(`{
    "select": ["name", "height"],
    "location": {
      "database": "db",
      "schema": "prod",
      "table": "people"
    },
    "filter": "",
    "operations": [
      {
        "type": "join|union"
      }
    ]
  }`)

	var y DataSource

	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
	var got, expected string
	var err error

	// test select
	if got, err = y.GenerateSQLSelect(); err != nil {
		t.Error(err)
	}
	expected = `"name", "height"`
	if got != expected {
		t.Errorf("got     : %s, expected: %s", got, expected)
	}

	// test from
	if got, err = y.GenerateSQLFrom(); err != nil {
		t.Error(err)
	}
	expected = `"prod"."people"`
	if got != expected {
		t.Errorf("got     : %s, expected: %s", got, expected)
	}

	// test full
	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	fmt.Println(got)
}

func TestDataSourceGenerateSQL_1(t *testing.T) {
	x := []byte(`{
    "select": ["name", "height"],
    "location": {
      "database": "db",
      "schema": "prod",
      "table": "people",
    },
    "filter": "",
    "operations": [
      {
        "type": "union",
        "source": {
          "select": ["name", "height"],
          "location": {
            "database": "db",
            "schema": "prod",
            "table": "pets",
          },
          "filter": "",
          "operation": []
        }
      }
    ]
  }
`)

	var y DataSource

	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
	var got string
	var err error
	if got, err = y.GenerateSQLSelect(); err != nil {
		t.Error(err)
	}
	expected := `"name", "height"`
	if got != expected {
		t.Errorf("got     : %s, expected: %s", got, expected)
	}
}
