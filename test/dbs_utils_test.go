package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
)

// TestOperatorValue
func TestOperatorValue(t *testing.T) {
	arg := "*val"
	op, res := dbs.OperatorValue(arg)
	if res != "%val" {
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

// TestStatementTemplateValues
func TestStatementTemplateValues(t *testing.T) {
	values := make(dbs.Record)
	values["id"] = 123
	values["name"] = "name"
	stm, vals, err := dbs.StatementTemplateValues("insert_test_tmpl_values", values)
	if err != nil {
		t.Error("Fail TestStatementTemplateValues", err)
	}
	fmt.Printf("### stm='%s' vals=%+v, err=%v\n", stm, vals, err)
	if stm != "INSERT INTO TEST (ID, NAME) VALUES (?, ?)\n" {
		t.Error("wrong statement", stm)
	}
	if vals[0] != 123 || vals[1] != "name" {
		t.Error("wrong values", vals)
	}
}

// TestStatementInsertValues
func TestStatementInsertValues(t *testing.T) {
	values := make(dbs.Record)
	values["id"] = 123
	values["name"] = "name"
	stm, vals, err := dbs.StatementValues("insert_test_values", values)
	if err != nil {
		t.Error("Fail TestStatementInsertValues", err)
	}
	stm = strings.Replace(stm, "\n", "", 0)
	fmt.Printf("### stm='%s' vals=%+v, err=%v\n", stm, vals, err)
	if stm != "INSERT INTO TEST (ID, NAME)\n VALUES (?,?)" {
		t.Error("wrong statement", stm)
	}
	if vals[0] != 123 || vals[1] != "name" {
		t.Error("wrong values", vals)
	}
}
