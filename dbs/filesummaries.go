package dbs

import (
	"fmt"
	"strings"
)

// FileSummaries API
func (a *API) FileSummaries() error {
	var args []interface{}
	var stm string
	//     var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Valid"] = false
	var wheresqlIsFileValid, whererun string

	validFileOnly := getValues(a.Params, "validFileOnly")
	if len(validFileOnly) == 1 {
		tmpl["Valid"] = true
		wheresqlIsFileValid = " and f.is_file_valid = 1 and DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')"
		//         conds = append(conds, "f.is_file_valid = 1")
		//         conds = append(conds, "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ")
	}
	runs, err := ParseRuns(getValues(a.Params, "run_num"))
	if err != nil {
		return err
	}
	if len(runs) > 0 {
		token, runsCond, runsBinds := runsClause("fl", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		//         conds = append(conds, runsCond)
		for _, v := range runsBinds {
			args = append(args, v)
		}
		whererun = runsCond
	}

	blockName := getValues(a.Params, "block_name")
	if len(blockName) == 1 {
		_, b := OperatorValue(blockName[0])
		args = append(args, b, b, b, b, b) // pass 5 block values
		if len(runs) > 0 {
			s, e := LoadTemplateSQL("filesummaries4block_run", tmpl)
			if e != nil {
				return e
			}
			stm += s
			//             stm += getSQL("filesummaries4block_run")
		} else {
			s, e := LoadTemplateSQL("filesummaries4block_norun", tmpl)
			if e != nil {
				return e
			}
			stm += s
			//             stm += getSQL("filesummaries4block_norun")
		}
	}

	dataset := getValues(a.Params, "dataset")
	if len(dataset) == 1 {
		_, d := OperatorValue(dataset[0])
		args = append(args, d, d, d, d, d) // pass 5 dataset values
		if len(runs) > 0 {
			s, e := LoadTemplateSQL("filesummaries4dataset_run", tmpl)
			if e != nil {
				return e
			}
			stm += s
			//             stm += getSQL("filesummaries4dataset_run")
		} else {
			s, e := LoadTemplateSQL("filesummaries4dataset_norun", tmpl)
			if e != nil {
				return e
			}
			stm += s
			//             stm += getSQL("filesummaries4dataset_norun")
		}
	}
	// replace whererun in stm
	stm = strings.Replace(stm, "whererun", whererun, -1)
	stm = strings.Replace(stm, "wheresqlIsFileValid", wheresqlIsFileValid, -1)
	//     stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// InsertFileSummaries DBS API
func (a *API) InsertFileSummaries() error {
	//     return InsertValues("insert_file_summaries", values)
	return nil
}
