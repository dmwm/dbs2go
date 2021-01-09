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
		op, val := OperatorValue(lfns[0])
		stm += fmt.Sprintf("\nJOIN %sFILES FL ON FL.BLOCK_ID = B.BLOCK_ID\n", owner)
		cond := fmt.Sprintf(" LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	blocks := getValues(params, "block_name")
	if len(blocks) == 1 {
		op, val := OperatorValue(blocks[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	datasets := getValues(params, "dataset")
	if len(datasets) == 1 {
		op, val := OperatorValue(datasets[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	sites := getValues(params, "origin_site_name")
	if len(sites) == 1 {
		op, val := OperatorValue(sites[0])
		cond := fmt.Sprintf(" B.ORIGIN_SITE_NAME %s %s", op, placeholder("origin_site_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	cdate := getValues(params, "cdate")
	if len(cdate) == 1 {
		op, val := OperatorValue(cdate[0])
		cond := fmt.Sprintf(" B.CREATION_DATE %s %s", op, placeholder("cdate"))
		conds = append(conds, cond)
		args = append(args, val)
	}

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

	ldate := getValues(params, "ldate")
	if len(ldate) == 1 {
		op, val := OperatorValue(ldate[0])
		cond := fmt.Sprintf(" B.LAST_MODIFICATION_DATE %s %s", op, placeholder("ldate"))
		conds = append(conds, cond)
		args = append(args, val)
	}

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
		whereRuns, bindsRuns := runsClause("FLM", runs)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}
	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertBlocks DBS API
func (API) InsertBlocks(values Record) error {
	return InsertValues("insert_blocks", values)
}
