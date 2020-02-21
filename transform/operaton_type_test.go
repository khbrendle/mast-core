package mast

import (
	"encoding/json"
	"testing"
)

func TestOperatonTypeGenerateSQLJoin0(t *testing.T) {
	x := []byte(`{"method":"join","modifier":"left","join_on":[{"entity":{"type":"Field","is_arg":false,"field":{"table":"person","column":"name"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"field":{"table":"employee","column":"name"}}}}},{"entity":{"type":"Field","is_arg":false,"field":{"table":"person","column":"name"},"equality":{"operator":"=","arg":{"type":"Field","is_arg":false,"field":{"table":"employee","column":"name"}}}},"operator":"and"}]}`)
	var y OperationType
	if err := json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}
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
