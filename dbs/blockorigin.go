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
	var conds []string

	// parse given parameters
	site := getValues(params, "origin_site_name")
	if len(site) > 1 {
		msg := "Unsupported list of sites"
		return 0, errors.New(msg)
	} else if len(site) == 1 {
		op, val := OperatorValue(site[0])
		cond := fmt.Sprintf(" B.ORIGIN_SITE_NAME %s %s", op, placeholder("original_site_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	block := getValues(params, "block_name")
	if len(block) > 1 {
		msg := "Unsupported list of block"
		return 0, errors.New(msg)
	} else if len(block) == 1 {
		op, val := OperatorValue(block[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	dataset := getValues(params, "dataset")
	if len(dataset) > 1 {
		msg := "Unsupported list of dataset"
		return 0, errors.New(msg)
	} else if len(dataset) == 1 {
		op, val := OperatorValue(dataset[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("blockorigin")
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
