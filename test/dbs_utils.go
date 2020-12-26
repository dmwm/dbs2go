package main

import (
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
)

// TestOperatorValue
func TestOperatorValue(t *testing.T) {
	arg := "*val"
	op, res := dbs.OperatorValue(arg)
	if res != arg {
		t.Error("Fail TestOperatorValue invalid value")
	}
	if op != "like" {
		t.Error("Fail TestOperatorValue invalid operator")
	}
	// check equal condition
	arg = "val"
	op, res = dbs.OperatorValue(arg)
	if res != arg {
		t.Error("Fail TestOperatorValue invalid value")
	}
	if op != "=" {
		t.Error("Fail TestOperatorValue invalid operator")
	}
}
