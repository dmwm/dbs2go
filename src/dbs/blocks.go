package dbs

import (
	"fmt"
)

// blocks API
func blocks(params Record) []Record {
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
	// get SQL statement from static area
	stm := getSQL("blocks")
	// use generic query API to fetch the results from DB
	return execute(stm+where, args...)
}
