package dbs

import (
	"fmt"
	"net/http"
	"strings"
)

// filesummaries API
func (API) FileSummaries(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var stm string
	var conds []string
	var join_valid_ds1, join_valid_ds2 string

	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		conds = append(conds, "f.is_file_valid = 1")
		conds = append(conds, "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ")
		join_valid_ds1 = fmt.Sprintf(" JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER, DBOWNER)
		join_valid_ds2 = fmt.Sprintf(" JOIN %s.DATASET_ACCESS_TYPES DT ON  DT.DATASET_ACCESS_TYPE_ID = D.DATASET_ACCESS_TYPE_ID", DBOWNER)
	}
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		token, runsCond, runsBinds := runsClause("fl", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, runsCond)
		for _, v := range runsBinds {
			args = append(args, v)
		}
	}

	block_name := getValues(params, "block_name")
	if len(block_name) == 1 {
		_, b := OperatorValue(block_name[0])
		args = append(args, b, b, b, b, b) // pass 5 block values
		if len(runs) > 0 {
			stm = getSQL("filesummaries4block_run")
		} else {
			stm = getSQL("filesummaries4block_norun")
		}
	}

	dataset := getValues(params, "dataset")
	if len(dataset) == 1 {
		_, d := OperatorValue(dataset[0])
		args = append(args, d, d, d, d, d) // pass 5 dataset values
		if len(runs) > 0 {
			stm = getSQL("filesummaries4dataset_run")
		} else {
			stm = getSQL("filesummaries4dataset_norun")
		}
	}
	stm = strings.Replace(stm, "join_valid_ds1", join_valid_ds1, -1)
	stm = strings.Replace(stm, "join_valid_ds2", join_valid_ds2, -1)
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileSummaries DBS API
func (API) InsertFileSummaries(values Record) error {
	return InsertValues("insert_file_summaries", values)
}
