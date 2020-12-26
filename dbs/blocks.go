package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// Blocks DBS API
func (API) Blocks(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	blocks := getValues(params, "block_name")
	if len(blocks) > 1 {
		msg := "Unsupported list of blocks"
		return 0, errors.New(msg)
	} else if len(blocks) == 1 {
		op, val := OperatorValue(blocks[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		msg := "The files API does not support list of datasets"
		return 0, errors.New(msg)
	} else if len(datasets) == 1 {
		op, val := OperatorValue(datasets[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blocks")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertBlocks DBS API
func (API) InsertBlocks(values Record) error {
	return InsertData("insert_blocks", values)
}
