package dbs

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// filesummaries API
func (API) FileSummaries(params Record, w http.ResponseWriter) error {
	var args []interface{}
	var stm string
	//     var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Valid"] = false
	var wheresql_isFileValid, whererun string

	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		tmpl["Valid"] = true
		wheresql_isFileValid = " and f.is_file_valid = 1 and DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')"
		//         conds = append(conds, "f.is_file_valid = 1")
		//         conds = append(conds, "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ")
	}
	runs, err := ParseRuns(getValues(params, "run_num"))
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

	block_name := getValues(params, "block_name")
	if len(block_name) == 1 {
		_, b := OperatorValue(block_name[0])
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

	dataset := getValues(params, "dataset")
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
	stm = strings.Replace(stm, "wheresql_isFileValid", wheresql_isFileValid, -1)
	//     stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileSummaries DBS API
func (API) InsertFileSummaries(r io.Reader, cby string) error {
	//     return InsertValues("insert_file_summaries", values)
	return nil
}
