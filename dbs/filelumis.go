package dbs

import (
	"fmt"
	"net/http"
)

// FileLumis API
func (API) FileLumis(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	stm := "SELECT DISTINCT FL.RUN_NUM as RUN_NUM, FL.LUMI_SECTION_NUM as LUMI_SECTION_NUM"

	validOnly := "0"
	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		_, v := OperatorValue(validFileOnly[0])
		validOnly = v
	}

	lfn := getValues(params, "logical_file_name")
	if len(lfn) == 1 {
		op, val := OperatorValue(lfn[0])
		if validOnly == "0" {
			stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID", DBOWNER, DBOWNER)
		} else {
			stm += fmt.Sprintf("  , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER, DBOWNER, DBOWNER)
			cond := fmt.Sprintf("F.IS_FILE_VALID = 1")
			conds = append(conds, cond)
			cond = fmt.Sprintf("DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')")
			conds = append(conds, cond)
		}
		cond := fmt.Sprintf("F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	} else if len(lfn) > 1 {
		stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID ", DBOWNER, DBOWNER)
		if validOnly != "0" {
			stm += fmt.Sprintf(" JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER, DBOWNER)
			cond := fmt.Sprintf("F.IS_FILE_VALID = 1")
			conds = append(conds, cond)
			cond = fmt.Sprintf("DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')")
			conds = append(conds, cond)
		}
		cond := fmt.Sprintf("F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR) ")
		token, binds := TokenGenerator(lfn, 100) // 100 is max for # of allowed lfns
		conds = append(conds, cond+token)
		for _, v := range binds {
			args = append(args, v)
		}
	}

	block_name := getValues(params, "block_name")
	if len(block_name) == 1 {
		op, val := OperatorValue(block_name[0])
		if validOnly == "0" {
			stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID JOIN %s.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID", DBOWNER, DBOWNER)
		} else {
			stm += fmt.Sprintf(" , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME FROM %s.FILE_LUMIS FL JOIN %s.FILES F ON F.FILE_ID = FL.FILE_ID JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID JOIN %s.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID", DBOWNER, DBOWNER, DBOWNER)
			cond := fmt.Sprintf("F.IS_FILE_VALID = 1")
			conds = append(conds, cond)
			cond = fmt.Sprintf("DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ")
			conds = append(conds, cond)
		}
		cond := fmt.Sprintf("B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		condRuns, bindsRuns := runsClause("FL", runs)
		conds = append(conds, condRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}

	stm += WhereClause(conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileLumis DBS API
func (API) InsertFileLumis(values Record) error {
	return InsertValues("insert_file_lumis", values)
}
