package dbs

import (
	"fmt"
	"strings"
)

// Runs DBS API
func (a *API) Runs() error {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	//     runs := getValues(a.Params, "run_num")
	lfn := getValues(a.Params, "logical_file_name")
	block := getValues(a.Params, "block_name")
	dataset := getValues(a.Params, "dataset")
	runs, err := ParseRuns(getValues(a.Params, "run_num"))
	if err != nil {
		return err
	}
	if len(lfn) == 1 {
		tmpl["Lfn"] = true
	} else if len(block) == 1 {
		tmpl["Block"] = true
	} else if len(dataset) == 1 {
		tmpl["Dataset"] = true
	}

	stm, err := LoadTemplateSQL("runs", tmpl)
	if err != nil {
		return err
	}

	if len(runs) > 1 {
		token, whereRuns, bindsRuns := runsClause("FL", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	} else if len(runs) == 1 {
		if strings.Contains(runs[0], "[") || strings.Contains(runs[0], "'") { // ['97-99']
			rrr := strings.Replace(runs[0], "[", "", -1)
			rrr = strings.Replace(rrr, "]", "", -1)
			rrr = strings.Replace(rrr, "'", "", -1)
			token, whereRuns, bindsRuns := runsClause("FL", []string{rrr})
			stm = fmt.Sprintf("%s %s", token, stm)
			conds = append(conds, whereRuns)
			for _, v := range bindsRuns {
				args = append(args, v)
			}
		} else {
			conds, args = AddParam("run_num", "FL.run_num", a.Params, conds, args)
		}
	}
	// we need to provide conditions after runs since runs will generate token
	if len(lfn) == 1 {
		conds, args = AddParam("logical_file_name", "FILES.LOGICAL_FILE_NAME", a.Params, conds, args)
	} else if len(block) == 1 {
		conds, args = AddParam("block_name", "BLOCKS.BLOCK_NAME", a.Params, conds, args)
	} else if len(dataset) == 1 {
		conds, args = AddParam("dataset", "DATASETS.DATASET", a.Params, conds, args)
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// InsertRuns DBS API
func (a *API) InsertRuns() error {
	//     return InsertValues("insert_runs", values)
	return nil
}
