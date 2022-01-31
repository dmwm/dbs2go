package dbs

import (
	"fmt"
	"strings"
)

// RunSummaries DBS API
func (a *API) RunSummaries() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	// parse arguments
	//     runs := getValues(a.Params, "run_num")
	runs, err := ParseRuns(getValues(a.Params, "run_num"))
	if err != nil {
		return Error(err, ParseErrorCode, "", "dbs.runsummaries.RunSummaries")
	}

	dataset := getValues(a.Params, "dataset")
	if len(dataset) == 1 {
		if strings.Contains(dataset[0], "*") {
			msg := "wild-card dataset value is not allowed"
			return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.runsummaries.RunSummaries")
		}
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "DS.DATASET", a.Params, conds, args)
	}
	stm, err := LoadTemplateSQL("runsummaries", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.runsummaries.RunSummaries")
	}

	if len(runs) > 1 {
		//         msg := "The runs API does not support list of runs"
		//         return errors.New(msg)
		token, whereRuns, bindsRuns := runsClause("FL", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	} else if len(runs) == 1 {
		conds, args = AddParam("run_num", "FL.RUN_NUM", a.Params, conds, args)
	} else {
		msg := fmt.Sprintf("No arguments for runsummaries API")
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.runsummaries.RunSummaries")
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.runsummaries.RunSummaries")
	}
	return nil
}
