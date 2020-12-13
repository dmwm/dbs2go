package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// BlockOrigin DBS API
func (API) BlockOrigin(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	block := getValues(params, "block_name")
	if len(block) > 1 {
		msg := "Unsupported list of block"
		return 0, errors.New(msg)
	} else if len(block) == 1 {
		op, val := opVal(block[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	dataset := getValues(params, "dataset")
	if len(dataset) > 1 {
		msg := "Unsupported list of dataset"
		return 0, errors.New(msg)
	} else if len(dataset) == 1 {
		op, val := opVal(dataset[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blockorigin")
	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertBlockOrigin DBS API
func (API) InsertBlockOrigin(values Record) error {
	return InsertData("insert_block_origin", values)
}
