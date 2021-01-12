package dbs

import (
	"fmt"
	"net/http"
)

// Files DBS API
func (API) Files(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	var stm string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Addition"] = false
	tmpl["RunNumber"] = false
	tmpl["LumiList"] = false

	lumis := getValues(params, "lumi_list")
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		tmpl["RunNumber"] = true
	}
	if len(lumis) > 0 {
		tmpl["LumiList"] = true
		stm, err = LoadTemplateSQL("files_sumoverlumi", tmpl)
		if err != nil {
			return 0, err
		}
		token, binds := TokenGenerator(lumis, 4000)
		stm = fmt.Sprintf("%s %s", token, stm)
		for _, v := range binds {
			args = append(args, v)
		}
	} else {
		stm = getSQL("files")
	}

	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		_, val := OperatorValue(validFileOnly[0])
		if val == "1" {
			cond := "F.IS_FILE_VALID = 1"
			conds = append(conds, cond)
			cond = "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')"
			conds = append(conds, cond)
		} else if val == "0" {
			cond := "F.IS_FILE_VALID <> -1"
			conds = append(conds, cond)
		}
	}

	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		token, binds := TokenGenerator(lfns, 100)
		stm = fmt.Sprintf("%s %s", token, stm)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	}
	conds, args = AddParam("dataset", "D.DATASET", params, conds, args)
	conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
	conds, args = AddParam("pset_hash", "PSH.PSET_HASH", params, conds, args)
	conds, args = AddParam("app_name", "AEX.APP_NAME", params, conds, args)
	conds, args = AddParam("output_module_label", "OMC.OUTPUT_MODULE_LABEL", params, conds, args)
	conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", params, conds, args)

	if len(runs) > 0 {
		token, whereRuns, bindsRuns := runsClause("FL", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFiles DBS API
func (API) InsertFiles(values Record) error {
	return InsertValues("insert_files", values)
}
