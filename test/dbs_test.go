package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// TestDBSCleanStatement
func TestDBSCleanStatement(t *testing.T) {
	stm := `SELECT A

	FROM B

	WHERE C=1`
	clean := `SELECT A
	FROM B
	WHERE C=1`
	stm = dbs.CleanStatement(stm)
	if stm != clean {
		t.Error("unable to clean the statement")
	}
}

// TestDBSOperatorValue
func TestDBSOperatorValue(t *testing.T) {
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

// TestDBSUtilParseRuns
func TestDBSUtilParseRuns(t *testing.T) {
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

// TestDBSGetID
func TestDBSGetID(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// prepare record for insertion
	rec := make(dbs.Record)
	rec["data_tier_name"] = "RAW-TEST-0"
	rec["creation_date"] = 1607536535
	rec["create_by"] = "Valentin"
	data, _ := json.Marshal(rec)
	reader := bytes.NewReader(data)

	// insert new record
	var api dbs.API
	utils.VERBOSE = 1
	_, err := api.InsertDataTiers(reader)
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}
	// GetID(table, id, attribute, value)
	rid, err := dbs.GetID("data_tiers", "data_tier_id", "data_tier_name", "RAW-TEST-0")
	if err != nil {
		t.Error("fail to execute GetID", err)
	}
	if rid != 1 {
		t.Errorf("fail to execute GetID, found rid=%v need rid=1", rid)
	}
}

// TestDBSUtilGetChunks
func TestDBSUtilGetChunks(t *testing.T) {
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

// TestDBSUtilWhereClause
func TestDBSUtilWhereClause(t *testing.T) {
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

// TestDBSUtilAddParam
func TestDBSUtilAddParam(t *testing.T) {
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

// TestDBSUtilFlatLumis
func TestDBSUtilFlatLumis(t *testing.T) {
	input := "[[1, 3], [5, 7]]"
	lumis, err := dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	output := []string{"1", "2", "3", "5", "6", "7"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input '%s' result='%s'", input, lumis)
		}
	}
	input = "[1, 3, 5, 7]"
	lumis, err = dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	output = []string{"1", "3", "5", "7"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input '%s' result='%s'", input, lumis)
		}
	}
}
