package main

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/dmwm/das2go/utils"
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
	//     fmt.Printf("runs input %+v, parsed runs %+v\n", input, runs)
	input = []string{"1a", "11-22", "3", "4"}
	runs, err = dbs.ParseRuns(input)
	if err == nil {
		t.Error("invalid run number should be detected for input", input)
		//     } else {
		//         fmt.Printf("runs input %+v, parsed runs %+v, error %v\n", input, runs, err)
	}
}

// TestGetID
func TestGetID(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// prepare record for insertion
	rec := make(dbs.Record)
	rec["data_tier_id"] = 1
	rec["data_tier_name"] = "RAW-TEST"
	rec["creation_date"] = 1607536535
	rec["create_by"] = "Valentin"

	// insert new record
	var api dbs.API
	utils.VERBOSE = 1
	err := api.InsertDataTiers(rec)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}
	// GetID(table, id, attribute, value)
	rid, err := dbs.GetID("data_tiers", "data_tier_id", "data_tier_name", "RAW-TEST")
	if err != nil {
		t.Error("fail to execute GetID", err)
	}
	if rid != 1 {
		t.Errorf("fail to execute GetID, found rid=%v need rid=1", rid)
	}
}

// TestUtilGetChunks
func TestUtilGetChunks(t *testing.T) {
	input := []string{"1", "2", "3", "4", "5"}
	chunks := dbs.GetChunks(input, 20)
	if len(chunks) != 1 {
		t.Error("fail to parse chunks input", input, chunks)
	}
	//     fmt.Printf("input %+v, chunks %+v\n", input, chunks)
	chunks = dbs.GetChunks(input, 2)
	if len(chunks) != 3 {
		t.Error("fail to parse chunks input", input, chunks)
	}
	//     fmt.Printf("input %+v, chunks %+v\n", input, chunks)
}

// TestUtilWhereClause
func TestUtilWhereClause(t *testing.T) {
	stm := "SELECT A FROM B"
	input := []string{"1", "2", "3"}
	newStm := dbs.WhereClause(stm, input)
	if newStm != "SELECT A FROM B WHERE 1 AND 2 AND 3" {
		t.Error("fail to create where clause for input", input, "whereClause", newStm)
	}
	input = []string{}
	newStm = dbs.WhereClause(stm, input)
	if newStm != "SELECT A FROM B" {
		t.Error("fail to create where clause for input", input, "whereClause is empty")
	}
	stm = "SELECT A FROM B WHERE"
	input = []string{"1", "2", "3"}
	newStm = dbs.WhereClause(stm, input)
	if newStm != "SELECT A FROM B WHERE 1 AND 2 AND 3" {
		t.Error("fail to create where clause for input", input, "whereClause", newStm)
	}
}

// TestUtilAddParam
func TestUtilAddParam(t *testing.T) {
	params := make(dbs.Record)
	params["name"] = []string{"1"} // must be list of strings due to how HTTP params are passed in request
	var conds []string
	var args []interface{}
	conds, args = dbs.AddParam("name", "Table.Name", params, conds, args)
	if strings.Trim(conds[0], " ") != "Table.Name = ?" {
		t.Error("fail to add condition")
	}
	if args[0] != "1" {
		t.Error("fail to add argument")
	}
	log.Println("conds", conds)
	log.Println("args", args)
}
