package mast

import (
	"encoding/json"
	"testing"
)

func TestEqualityGenerateSQL0(t *testing.T) {
	var y Equality
	x := []byte(`{
    "operator": ">",
    "arg": {
      "type": "Value",
      "value": 1
    }
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `> 1`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestEqualityGeneratePySpark0(t *testing.T) {
	var y Equality
	x := []byte(`{
    "operator": ">",
    "arg": {
      "type": "Value",
      "value": 1
    }
  }`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `> 1`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}
