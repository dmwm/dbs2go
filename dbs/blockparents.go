package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// BlockParents DBS API
func (API) BlockParents(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	blockparent := getValues(params, "block_name")
	if len(blockparent) > 1 {
		msg := "Unsupported list of blockparent"
		return 0, errors.New(msg)
	} else if len(blockparent) == 1 {
		op, val := OperatorValue(blockparent[0])
		cond := fmt.Sprintf(" BP.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blockparent")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertBlockParents DBS API
func (API) InsertBlockParents(values Record) error {
	args := make(Record)
	args["Owner"] = DBOWNER
	args["Query1"] = false
	args["Query2"] = false
	args["Query3"] = false
	if _, ok := values["BlockName"]; ok {
		args["Query1"] = true
	}
	if _, ok := values["ParentLogicalFileName"]; ok {
		args["Query2"] = true
	}
	if _, ok := values["ParentBlockID"]; ok {
		args["Query3"] = true
	}
	return InsertTemplateValues("insert_block_parents", args, values)
}
