package dbs

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Runs DBS API
func (API) Runs(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	//     runs := getValues(params, "run_num")
	lfn := getValues(params, "logical_file_name")
	block := getValues(params, "block_name")
	dataset := getValues(params, "dataset")
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}

	if len(lfn) == 1 {
		tmpl["Lfn"] = true
		conds, args = AddParam("logical_file_name", "FILES.LOGICAL_FILE_NAME", params, conds, args)
	} else if len(block) == 1 {
		tmpl["Block"] = true
		conds, args = AddParam("block_name", "BLOCKS.BLOCK_NAME", params, conds, args)
	} else if len(dataset) == 1 {
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "DATASETS.DATASET", params, conds, args)
	} else {
		msg := fmt.Sprintf("No arguments for runs API")
		return 0, errors.New(msg)
	}

	stm, err := LoadTemplateSQL("runs", tmpl)
	if err != nil {
		return 0, err
	}
	if len(runs) > 1 {
		//         msg := "The runs API does not support list of runs"
		//         return 0, errors.New(msg)
		token, whereRuns, bindsRuns := runsClause("FL", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	} else if len(runs) == 1 {
		conds, args = AddParam("run_num", "FL.run_num", params, conds, args)
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertRuns DBS API
func (API) InsertRuns(r io.Reader, cby string) error {
	//     return InsertValues("insert_runs", values)
	return nil
}
