package dbs

import (
	"fmt"
	"strings"
)

// files API
func (API) Files(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	files := getValues(params, "logical_file_name")
	if len(files) > 1 {
		msg := "The files API does not support list of files"
		return errorRecord(msg)
	} else if len(files) == 1 {
		op, val := opVal(files[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		msg := "The files API does not support list of datasets"
		return errorRecord(msg)
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	block_names := getValues(params, "block_name")
	if len(block_names) > 1 {
		msg := "The files API does not support list of block_names"
		return errorRecord(msg)
	} else if len(block_names) == 1 {
		op, val := opVal(block_names[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("files")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// filechildren API
func (API) FileChildren(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	filechildren := getValues(params, "logical_file_name")
	if len(filechildren) > 1 {
		msg := "The filechildren API does not support list of filechildren"
		return errorRecord(msg)
	} else if len(filechildren) == 1 {
		op, val := opVal(filechildren[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("filechildren")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// fileparent API
func (API) FileParent(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	fileparent := getValues(params, "logical_file_name")
	if len(fileparent) > 1 {
		msg := "The fileparent API does not support list of fileparent"
		return errorRecord(msg)
	} else if len(fileparent) == 1 {
		op, val := opVal(fileparent[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("fileparent")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// filesummaries API
func (API) FileSummaries(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	var stm, whererun, wheresql_run_list, wheresql_run_range, wheresql_isFileValid string
	var join_valid_ds1, join_valid_ds2 string

	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		wheresql_isFileValid = " and f.is_file_valid = 1 and DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') "
		join_valid_ds1 = fmt.Sprintf(" JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER, DBOWNER)
		join_valid_ds2 = fmt.Sprintf(" JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER)
	}
	runs := false
	run_num := getValues(params, "run_num")
	if len(run_num) > 0 {
		wheresql_run_list = fmt.Sprintf(" fl.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) ")
		//         var vals []string
		genSQL, vals := tokens(run_num)
		for _, d := range vals {
			run_range := strings.Split(d, "-")
			if len(run_range) > 0 {
				wheresql_run_range += fmt.Sprintf(" fl.RUN_NUM between :minrun and :maxrun ")
				args = append(args, run_range[0], run_range[1])
			} else {
				args = append(args, d, d, d) // append three values since tokens generates placeholders for them
			}
		}
		stm = genSQL
		runs = true
		if wheresql_run_list != "" && wheresql_run_range != "" {
			whererun = fmt.Sprintf(" %s or %s ", wheresql_run_list, wheresql_run_range)
		} else if wheresql_run_list != "" {
			whererun = wheresql_run_list
		} else if wheresql_run_range != "" {
			whererun = wheresql_run_range
		}
	}

	block_name := getValues(params, "block_name")
	if len(block_name) == 1 {
		_, b := opVal(block_name[0])
		args = append(args, b, b, b, b, b) // pass 5 block values
		if runs {
			stm = getSQL("filesummaries4block_run")
		} else {
			stm = getSQL("filesummaries4block_norun")
		}
	}

	dataset := getValues(params, "dataset")
	if len(dataset) == 1 {
		_, d := opVal(dataset[0])
		args = append(args, d, d, d, d, d) // pass 5 dataset values
		if runs {
			stm = getSQL("filesummaries4dataset_run")
		} else {
			stm = getSQL("filesummaries4dataset_norun")
		}
	}
	stm = strings.Replace(stm, "whererun", whererun, -1)
	stm = strings.Replace(stm, "wheresql_isFileValid", wheresql_isFileValid, -1)
	stm = strings.Replace(stm, "join_valid_ds1", join_valid_ds1, -1)
	stm = strings.Replace(stm, "join_valid_ds2", join_valid_ds2, -1)

	// use generic query API to fetch the results from DB
	return executeAll(stm, args...)
}
