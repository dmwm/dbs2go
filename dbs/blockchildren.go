package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// BlockChildren DBS API
func (API) BlockChildren(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	// parse dataset argument
	blockchildren := getValues(params, "block_name")
	if len(blockchildren) > 1 {
		msg := "Unsupported list of blockchildren"
		return 0, errors.New(msg)
	} else if len(blockchildren) == 1 {
		op, val := OperatorValue(blockchildren[0])
		cond := fmt.Sprintf(" BP.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blockchildren")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
