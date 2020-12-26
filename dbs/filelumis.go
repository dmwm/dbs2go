package dbs

import (
	"fmt"
	"net/http"
	"strings"
)

// FileLumis API
func (API) FileLumis(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	var wheresql, wheresql_run_list, wheresql_run_range string

	stm := "SELECT DISTINCT FL.RUN_NUM as RUN_NUM, FL.LUMI_SECTION_NUM as LUMI_SECTION_NUM"

	validOnly := "0"
	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		_, v := OperatorValue(validFileOnly[0])
		validOnly = v
	}

	lfn := getValues(params, "logical_file_name")
	if len(lfn) == 1 {
		_, b := OperatorValue(lfn[0])
		args = append(args, b)
		if validOnly == "0" {
			stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID WHERE F.LOGICAL_FILE_NAME = :logical_file_name ", DBOWNER, DBOWNER)
		} else {
			stm += fmt.Sprintf("  , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID WHERE F.IS_FILE_VALID = 1 AND F.LOGICAL_FILE_NAME = :logical_file_name AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION'", DBOWNER, DBOWNER, DBOWNER)
		}
	} else if len(lfn) > 1 {
		stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID ", DBOWNER, DBOWNER)
		if validOnly == "0" {
			wheresql = fmt.Sprintf(" WHERE F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR) ")
		} else {
			stm += fmt.Sprintf(" JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER, DBOWNER)
			wheresql = fmt.Sprintf(" WHERE F.IS_FILE_VALID = 1 AND F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR) AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')")
		}
		genSQL, vals := tokens(lfn)
		for _, d := range vals {
			args = append(args, d, d, d)
		}
		stm = genSQL + stm + wheresql
	}

	block_name := getValues(params, "block_name")
	if len(block_name) == 1 {
		_, b := OperatorValue(block_name[0])
		args = append(args, b)
		if validOnly == "0" {
			stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID JOIN %s.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID WHERE B.BLOCK_NAME = :block_name", DBOWNER, DBOWNER)
		} else {
			stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID JOIN %s.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID WHERE F.IS_FILE_VALID = 1 AND B.BLOCK_NAME = :block_name AND DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ", DBOWNER, DBOWNER, DBOWNER, DBOWNER, DBOWNER)
		}
	}

	run_num := getValues(params, "run_num")
	if len(run_num) > 0 {
		wheresql_run_list = fmt.Sprintf(" fl.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR) ")
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
		stm = fmt.Sprintf("%s %s ", stm, genSQL)
		if wheresql_run_list != "" && wheresql_run_range != "" {
			stm = fmt.Sprintf(" %s and ( %s or %s ) ", stm, wheresql_run_list, wheresql_run_range)
		} else if wheresql_run_list != "" {
			stm = fmt.Sprintf("%s and %s", stm, wheresql_run_list)
		} else if wheresql_run_range != "" {
			stm = fmt.Sprintf("%s and %s", stm, wheresql_run_range)
		}
	}

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileLumis DBS API
func (API) InsertFileLumis(values Record) error {
	return InsertData("insert_file_lumis", values)
}
