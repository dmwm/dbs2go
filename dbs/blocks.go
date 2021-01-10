package dbs

import (
	"fmt"
	"net/http"
	"strings"
)

// Blocks DBS API
func (API) Blocks(params Record, w http.ResponseWriter) (int64, error) {
	// get SQL statement from static area
	stm := getSQL("blocks")
	var args []interface{}
	var conds []string
	owner := ""
	if DBOWNER != "sqlite" {
		owner = fmt.Sprintf("%s.", DBOWNER)
	}

	// parse arguments
	lfns := getValues(params, "logical_file_name")
	if len(lfns) == 1 {
		stm += fmt.Sprintf("\nJOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID\n", owner)
		conds, args = AddParam("logical_file_name", "FL.LOGICAL_FILE_NAME", params, conds, args)
	}

	conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
	conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", params, conds, args)
	conds, args = AddParam("cdate", "B.CREATION_DATE", params, conds, args)

	minDate := getValues(params, "min_cdate")
	maxDate := getValues(params, "max_cdate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE BETWEEN %s and %s", placeholder("min_cdate"), placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE > %s", placeholder("min_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE < %s", placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}

	conds, args = AddParam("ldate", "B.LAST_MODIFICATION_DATE", params, conds, args)

	minDate = getValues(params, "min_ldate")
	maxDate = getValues(params, "max_ldate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE BETWEEN %s and %s", placeholder("min_ldate"), placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE > %s", placeholder("min_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE < %s", placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}

	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		stm = strings.Replace(stm, "SELECT ", "SELECT DISTINCT ", 1)
		if len(lfns) == 1 { // lfn is present in a query
			stm += fmt.Sprintf("\nJOIN %sFILE_LUMIS FLM on FLM.FILE_ID = FL.FILE_ID\n", owner)
		} else {
			stm += fmt.Sprintf("\nJOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID\n", owner)
		}
		// handle runs where clause
		token, whereRuns, bindsRuns := runsClause("FLM", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertBlocks DBS API
func (API) InsertBlocks(values Record) error {
	return InsertValues("insert_blocks", values)
}
