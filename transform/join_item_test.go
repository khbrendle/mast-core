package mast

import (
	"encoding/json"
	"testing"
)

func TestJoinItemGenerateSQL0(t *testing.T) {
	x := []byte(`{"entity":{"type":"Field","is_arg":false,"field":{"table":"person","column":"name"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"field":{"table":"employee","column":"name"}}}},"operator":""}`)
	var y JoinItem
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

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
	x := []byte(`{"entity":{"type":"Field","field":{"table":"person","column":"name"},"equality":{"operator":"=","arg":{"type":"Field","field":{"table":"employee","column":"name"}}}},"operator":"and"}`)
	var y JoinItem
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string
	var err error

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `and "person"."name" = "employee"."name"`
	if got != expected {
		t.Errorf("\ngot     :\n%s\nexpected:\n%s", got, expected)
	}
}
