package dbs

import (
	"fmt"
	"log"
)

// blocks API
func blocks(params Record) []Record {
	// query statement
	stm := fmt.Sprintf("select id, name from blocks where name = %s", placeholder(1))

	// variables we'll use in where clause
	block := getValue(params, "block_name")

	// use generic query API to fetch the results from DB
	out := query(stm, block)
	return out
}
