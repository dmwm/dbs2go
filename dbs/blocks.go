package dbs

import (
	"fmt"
)

// blocks API
func (API) Blocks(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	blocks := getValues(params, "block_name")
	if len(blocks) > 1 {
		panic("Unsupported list of blocks")
	} else if len(blocks) == 1 {
		op, val := opVal(blocks[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		panic("The files API does not support list of datasets")
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blocks")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
