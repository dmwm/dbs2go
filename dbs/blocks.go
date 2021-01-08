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
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "
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
		where += addCond(where, cond)
		args = append(args, val)
		//     } else {
		//         return 0, errors.New("wrong logical_file_name parameter")
	}

	blocks := getValues(params, "block_name")
	if len(blocks) == 1 {
		op, val := OperatorValue(blocks[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
		//     } else {
		//         return 0, errors.New("wrong block_name parameter")
	}

	datasets := getValues(params, "dataset")
	if len(datasets) == 1 {
		op, val := OperatorValue(datasets[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
		//     } else {
		//         return 0, errors.New("wrong dataset parameter")
	}

	sites := getValues(params, "origin_site_name")
	if len(sites) == 1 {
		op, val := OperatorValue(sites[0])
		cond := fmt.Sprintf(" B.ORIGIN_SITE_NAME %s %s", op, placeholder("origin_site_name"))
		where += addCond(where, cond)
		args = append(args, val)
		//     } else {
		//         return 0, errors.New("wrong origin_site_name parameter")
	}

	cdate := getValues(params, "cdate")
	if len(cdate) == 1 {
		op, val := OperatorValue(cdate[0])
		cond := fmt.Sprintf(" B.CREATION_DATE %s %s", op, placeholder("cdate"))
		where += addCond(where, cond)
		args = append(args, val)
		//     } else {
		//         return 0, errors.New("wrong cdate parameter")
	}

	minDate := getValues(params, "min_cdate")
	maxDate := getValues(params, "max_cdate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE BETWEEN %s and %s", placeholder("min_cdate"), placeholder("max_cdate"))
			where += addCond(where, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE > %s", placeholder("min_cdate"))
			where += addCond(where, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE < %s", placeholder("max_cdate"))
			where += addCond(where, cond)
			args = append(args, maxval)
		}
		//     } else {
		//         return 0, errors.New("wrong min_cdate/max_cdate parameter")
	}

	ldate := getValues(params, "ldate")
	if len(ldate) == 1 {
		op, val := OperatorValue(ldate[0])
		cond := fmt.Sprintf(" B.LAST_MODIFICATION_DATE %s %s", op, placeholder("ldate"))
		where += addCond(where, cond)
		args = append(args, val)
		//     } else {
		//         return 0, errors.New("wrong ldate parameter")
	}

	minDate = getValues(params, "min_ldate")
	maxDate = getValues(params, "max_ldate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE BETWEEN %s and %s", placeholder("min_ldate"), placeholder("max_ldate"))
			where += addCond(where, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE > %s", placeholder("min_ldate"))
			where += addCond(where, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE < %s", placeholder("max_ldate"))
			where += addCond(where, cond)
			args = append(args, maxval)
		}
		//     } else {
		//         return 0, errors.New("wrong min_ldate/max_ldate parameter")
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
		var runList []string
		for _, r := range runs {
			if strings.Contains(r, "-") { // run-range argument
				cond := fmt.Sprintf(" FLM.RUN_NUM between %s and %s ", placeholder("minrun"), placeholder("maxrun"))
				where += addCond(where, cond)
				rr := strings.Split(r, "-")
				args = append(args, rr[0])
				args = append(args, rr[1])
			} else {
				runList = append(runList, r)
			}
		}
		cond := "FLM.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		where += addCond(where, cond)
		token, binds := CreateTokenGenerator(runList)
		where += addCond(where, token)
		for _, v := range binds {
			args = append(args, v)
		}
	}

	// use generic query API to fetch the results from DB
	return executeAll(w, stm+where, args...)
}

// InsertBlocks DBS API
func (API) InsertBlocks(values Record) error {
	return InsertValues("insert_blocks", values)
}
