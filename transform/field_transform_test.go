package mast

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestFieldTransformGenerateSQLField0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"person","column":"name"},"value":"","function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `"person"."name"`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLValueString0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"static text","function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `'static text'`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLValueInt0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":1,"function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `1`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLValueFloat0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":1.5,"function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `1.500000`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLValueAlias0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"static text","function":"","args":[],"alias":"text_col"}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `'static text' as "text_col"`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLFunction0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Function","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"","function":"array_contains","args":[{"type":"Field","is_arg":true,"arg_index":0,"field":{"table":"person","column":"name_array"},"value":"","function":"","args":[]},{"type":"Field","is_arg":true,"arg_index":1,"field":{"table":"employee","column":"name"},"value":"","function":"","args":[]}]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `array_contains("person"."name_array", "employee"."name")`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGenerateSQLEquality0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","value":1.5,"equality":{"operator":">","arg":{"type":"Value","value":1}}}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GenerateSQL(); err != nil {
		t.Error(err)
	}
	expected = `1.500000 > 1`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGeneratePySparkField0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Field","is_arg":false,"arg_index":null,"field":{"table":"person","column":"name"},"value":"","function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `F.col("person_name")`
	// fmt.Println(got)
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGeneratePySparkValueString0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"static text","function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `"static text"`
	// fmt.Println(got)
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGeneratePySparkValueInt0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":1,"function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `1`
	// fmt.Println(got)
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGeneratePySparkValueFloat0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":1.5,"function":"","args":[]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `1.500000`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

// this shouldn't be thing, alias function F.lit(value)
// func TestFieldTransformGeneratePySparkValueAlias0(t *testing.T) {
// 	var y FieldTransform
// 	x := []byte(`{"type":"Value","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"static text","function":"","args":[],"alias":"text_col"}`)
// 	var err error
// 	if err = json.Unmarshal(x, &y); err != nil {
// 		t.Error(err)
// 	}
//
// 	var got, expected string
//
// 	if got, err = y.GeneratePySpark(); err != nil {
// 		t.Error(err)
// 	}
// 	expected = `F.lit('static text').alias("text_col")`
// 	fmt.Println(got)
// 	if got != expected {
// 		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
// 	}
// }

func TestFieldTransformGeneratePySparkFunction0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Function","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"","function":"F.array_contains","args":[{"type":"Field","is_arg":true,"arg_index":0,"field":{"table":"person","column":"name_array"},"value":"","function":"","args":[]},{"type":"Field","is_arg":true,"arg_index":1,"field":{"table":"employee","column":"name"},"value":"","function":"","args":[]}]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `F.array_contains(F.col("person_name_array"), F.col("employee_name"))`
	fmt.Println(got)
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGeneratePySparkEquality0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Function","function":"F.lit","args":[{"type":"Value","value":1.5}],"equality":{"operator":">","arg":{"type":"Function","function":"F.lit","args":[{"type":"Value","value":1}]}}}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `F.lit(1.500000) > F.lit(1)`

	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}

func TestFieldTransformGeneratePySparkChainMethods0(t *testing.T) {
	var y FieldTransform
	x := []byte(`{"type":"Function","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"","function":"F.when","args":[{"type":"Field","is_arg":true,"arg_index":0,"field":{"table":"person","column":"age"},"equality":{"operator":">","arg":{"type":"Value","is_arg":true,"value":25}}},{"type":"Function","is_arg":false,"function":"F.lit","args":[{"type":"Value","is_arg":true,"value":"yes"}]}],"chain_methods":[{"type":"Function","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"","function":".when","args":[{"type":"Field","is_arg":true,"arg_index":0,"field":{"table":"person","column":"age"},"equality":{"operator":"<","arg":{"type":"Value","is_arg":true,"value":18}}},{"type":"Function","is_arg":false,"function":"F.lit","args":[{"type":"Value","is_arg":true,"value":"no"}]}]},{"type":"Function","is_arg":false,"arg_index":null,"field":{"table":"","column":""},"value":"","function":".otherwise","args":[{"type":"Function","is_arg":false,"function":"F.lit","args":[{"type":"Value","is_arg":true,"value":"maybe"}]}]}]}`)
	var err error
	if err = json.Unmarshal(x, &y); err != nil {
		t.Error(err)
	}

	var got, expected string

	if got, err = y.GeneratePySpark(); err != nil {
		t.Error(err)
	}
	expected = `F.when(F.col("person_age") > 25, F.lit("yes")).when(F.col("person_age") < 18, F.lit("no")).otherwise(F.lit("maybe"))`
	// fmt.Println(got)
	if got != expected {
		t.Errorf("\ngot     : %s\nexpected: %s", got, expected)
	}
}
