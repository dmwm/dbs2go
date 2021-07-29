package dbs

import (
	"net/http"
)

// BlockChildren DBS API
func (API) BlockChildren(params Record, w http.ResponseWriter) error {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	conds, args = AddParam("block_name", "BP.BLOCK_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("blockchildren")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
