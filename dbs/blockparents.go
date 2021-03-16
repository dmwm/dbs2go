package dbs

import (
	"errors"
	"io"
	"net/http"
)

// BlockParents DBS API
func (API) BlockParents(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	blockparent := getValues(params, "block_name")
	if len(blockparent) > 1 {
		msg := "Unsupported list of blockparent"
		return 0, errors.New(msg)
	} else if len(blockparent) == 1 {
		conds, args = AddParam("block_name", "BP.BLOCK_NAME", params, conds, args)
	}
	// get SQL statement from static area
	stm := getSQL("blockparent")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertBlockParents DBS API
func (API) InsertBlockParents(r io.Reader, cby string) (int64, error) {
	/*
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
	*/
	return 0, nil
}
