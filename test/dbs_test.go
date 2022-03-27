package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
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
	input := []string{"1", "11-12", "3", "4"}
	runs, err := dbs.ParseRuns(input)
	if len(runs) != 5 {
		t.Error("fail to parse runs input", input, runs)
	}
	if err != nil {
		t.Error(err)
	}
	log.Println("test input runs", input, "parsed runs", runs)
	//     fmt.Printf("runs input %+v, parsed runs %+v\n", input, runs)
	input = []string{"1a", "11-22", "3", "4"}
	runs, err = dbs.ParseRuns(input)
	if err == nil {
		t.Error("invalid run number should be detected for input", input)
	}
	fmt.Printf("runs input %+v, parsed runs %+v\n", input, runs)
}

// TestDBSGetID
func TestDBSGetID(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// prepare record for insertion
	createBy := "Valentin"
	rec := make(dbs.Record)
	rec["data_tier_name"] = "RAW-TEST-0"
	rec["creation_date"] = 1607536535
	rec["create_by"] = createBy
	data, _ := json.Marshal(rec)
	reader := bytes.NewReader(data)
	writer := utils.StdoutWriter("")

	// insert new record
	//     var api dbs.API
	api := dbs.API{
		Reader:   reader,
		Writer:   writer,
		CreateBy: createBy,
	}
	utils.VERBOSE = 1
	err := api.InsertDataTiers()
	if err != nil {
		t.Errorf("Fail in insert record %+v, error %v\n", rec, err)
	}
	// start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("unable to get DB transaction: %v\n", err)
	}
	defer tx.Rollback()
	// GetID(table, id, attribute, value)
	rid, err := dbs.GetID(tx, "data_tiers", "data_tier_id", "data_tier_name", "RAW-TEST-0")
	if err != nil {
		t.Error("fail to execute GetID", err)
	}
	if rid != 1 {
		t.Errorf("fail to execute GetID, found rid=%v need rid=1", rid)
	}
}

// TestDBSGetRecID
func TestDBSGetRecID(t *testing.T) {
	// initialize DB for testing
	db := initDB(false)
	defer db.Close()

	// prepare record for insertion
	tier := "RAW-TEST-0"
	cby := "Valentin"
	rec := dbs.DataTiers{DATA_TIER_NAME: tier, CREATE_BY: cby, CREATION_DATE: time.Now().Unix()}

	// start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("unable to get DB transaction: %v\n", err)
	}
	defer tx.Rollback()
	// GetID(table, id, attribute, value)
	rid, err := dbs.GetRecID(tx, &rec, "data_tiers", "data_tier_id", "data_tier_name", tier)
	if err != nil {
		t.Error("fail to execute GetRecID", err)
	}
	if rid != 1 {
		t.Errorf("fail to execute GetRecID, found rid=%v need rid=1", rid)
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
//gocyclo:ignore
func TestDBSUtilFlatLumis(t *testing.T) {
	input := "[[1, 3], [5, 7]]"
	lumis, err := dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	if len(lumis) == 0 {
		t.Error(fmt.Sprintf("fail to flat lumis, zero output"))
	}
	output := []string{"1", "2", "3", "5", "6", "7"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input='%s' lumis='%s' expected output='%v'", input, lumis, output)
		}
	}
	input = "[[[1, 3], [5, 7]]]"
	lumis, err = dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	if len(lumis) == 0 {
		t.Error(fmt.Sprintf("fail to flat lumis, zero output"))
	}
	output = []string{"1", "2", "3", "5", "6", "7"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input='%s' lumis='%s' expected output='%v'", input, lumis, output)
		}
	}
	input = "[[1,+3], [5,+7]]"
	lumis, err = dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	if len(lumis) == 0 {
		t.Error(fmt.Sprintf("fail to flat lumis, zero output"))
	}
	output = []string{"1", "2", "3", "5", "6", "7"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input='%s' lumis='%s' expected output='%v'", input, lumis, output)
		}
	}
	input = "[1, 3, 5, 7]"
	lumis, err = dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	if len(lumis) == 0 {
		t.Error(fmt.Sprintf("fail to flat lumis, zero output"))
	}
	output = []string{"1", "3", "5", "7"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input '%s' result='%s'", input, lumis)
		}
	}
	input = "[[1, 2, 3, 4, 5, 6]]"
	lumis, err = dbs.FlatLumis(input)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	if len(lumis) == 0 {
		t.Error(fmt.Sprintf("fail to flat lumis, zero output"))
	}
	output = []string{"1", "2", "3", "4", "5", "6"}
	log.Println("lumis input", input, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input '%s' result='%s'", input, lumis)
		}
	}
	input2 := []string{"[1, 2, 3, 4, 5, 6]"}
	lumis, err = dbs.FlatLumis(input2)
	if err != nil {
		t.Error(fmt.Sprintf("fail to flat lumis with error %v", err))
	}
	if len(lumis) == 0 {
		t.Error(fmt.Sprintf("fail to flat lumis, zero output"))
	}
	output = []string{"1", "2", "3", "4", "5", "6"}
	log.Println("lumis input", input2, "flat output", output)
	for i, v := range lumis {
		if v != output[i] {
			t.Errorf("fail to flat lumis input '%s' result='%s'", input2, lumis)
		}
	}
}

// TestDBSRunsConditions
func TestDBSRunsConditions(t *testing.T) {
	// run_num=97
	// run_num=97-99
	// run_num=[97]
	// run_num=[97-99]
	// run_num=['97']
	// run_num=['97-99']
	// run_num=['97-99', 200, 300]
	// run_num="['97-99', 200, 300]"
	// run_num="['97-99' 200 300]"
}
