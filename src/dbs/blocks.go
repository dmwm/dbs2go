package dbs

import (
	"fmt"
)

// blocks API
func blocks(params Record) []Record {
	// query statement
	stm := fmt.Sprintf("select id, name from blocks where name = %s", placeholder("block_name"))

	// variables we'll use in where clause
	blocks := getValues(params, "block_name")
	block := blocks[0]

	// use generic query API to fetch the results from DB
	out := execute(stm, block)
	return out
}
