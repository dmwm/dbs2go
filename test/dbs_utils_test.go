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
	args := make(dbs.Record)
	args["Owner"] = "sqlite"
	values := make(dbs.Record)
	values["id"] = 123
	values["name"] = "name"
	stm, vals, err := dbs.StatementTemplateValues("insert_test_tmpl_values", args, values)
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

// TestUtilParseRuns
func TestUtilParseRuns(t *testing.T) {
	input := []string{"1", "11-22", "3", "4"}
	runs, err := dbs.ParseRuns(input)
	if len(runs) != 4 {
		t.Error("fail to parse runs input", input, runs)
	}
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("runs input %+v, parsed runs %+v\n", input, runs)
	input = []string{"1a", "11-22", "3", "4"}
	runs, err = dbs.ParseRuns(input)
	if err == nil {
		t.Error("invalid run number should be detected for input", input)
	} else {
		fmt.Printf("runs input %+v, parsed runs %+v, error %v\n", input, runs, err)
	}
}

// TestUtilGetChunks
func TestUtilGetChunks(t *testing.T) {
	input := []string{"1", "2", "3", "4", "5"}
	chunks := dbs.GetChunks(input, 20)
	if len(chunks) != 1 {
		t.Error("fail to parse chunks input", input, chunks)
	}
	fmt.Printf("input %+v, chunks %+v\n", input, chunks)
	chunks = dbs.GetChunks(input, 2)
	if len(chunks) != 3 {
		t.Error("fail to parse chunks input", input, chunks)
	}
	fmt.Printf("input %+v, chunks %+v\n", input, chunks)
}
