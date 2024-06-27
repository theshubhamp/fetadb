package expr

import (
	"reflect"
	"strings"
	"testing"
)

func TestFunctionCall(t *testing.T) {
	testString := "ABCD"

	funcCall, err := NewFuncCall("lower", []Expression{Literal{Value: testString}})
	if err != nil {
		t.Errorf("cannot create func call: %v", err)
		return
	}

	result, err := funcCall.Evaluate(nil)
	if err != nil {
		t.Errorf("cannot evaluate func call: %v", err)
		return
	}
	if resultString, ok := result.(string); !ok {
		t.Errorf("expected result to be string, got: %v", reflect.TypeOf(result).Kind())
		return
	} else {
		if resultString != strings.ToLower(testString) {
			t.Errorf("expected lowercase string, got: %v (!= %v)", resultString, strings.ToLower(testString))
			return
		}
	}
}
