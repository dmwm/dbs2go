package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// RunSummaries DBS API
func (API) RunSummaries(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)

	// parse arguments
	runs := getValues(params, "run_num")
	if len(runs) > 1 {
		msg := "The runs API does not support list of runs"
		return 0, errors.New(msg)
	} else if len(runs) == 1 {
		conds, args = AddParam("run_num", "R.RUN_NUM", params, conds, args)
	} else {
		msg := fmt.Sprintf("No arguments for runsummaries API")
		return 0, errors.New(msg)
	}

	dataset := getValues(params, "dataset")
	if len(dataset) == 1 {
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
	}
	stm := LoadTemplateSQL("runsummaries", tmpl)
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
