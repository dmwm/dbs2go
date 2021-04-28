package dbs

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// RunSummaries DBS API
func (API) RunSummaries(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	// parse arguments
//     runs := getValues(params, "run_num")
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}

	dataset := getValues(params, "dataset")
	if len(dataset) == 1 {
		if strings.Contains(dataset[0], "*") {
			msg := "wild-card dataset value is not allowed"
			return 0, errors.New(msg)
		}
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
	}
	stm, err := LoadTemplateSQL("runsummaries", tmpl)
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
		conds, args = AddParam("run_num", "R.RUN_NUM", params, conds, args)
	} else {
		msg := fmt.Sprintf("No arguments for runsummaries API")
		return 0, errors.New(msg)
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}
