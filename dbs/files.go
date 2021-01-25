package dbs

import (
	"fmt"
	"net/http"
	"strings"
)

// Files DBS API
func (API) Files(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	if len(params) == 0 {
		msg := "Files API with empty parameter map"
		return dbsError(w, msg)
	}
	// When sumOverLumi=1, no lfn list or run_num list allowed
	if _, ok := params["sumOverLumi"]; ok {
		if vals, ok := params["run_num"]; ok {
			runs := fmt.Sprintf("%v", vals)
			if strings.Contains(runs, "[") {
				msg := "When sumOverLumi=1, no lfn list or run_num list allowed"
				return dbsError(w, msg)
			}
		} else if vals, ok := params["logical_file_name"]; ok {
			lfns := fmt.Sprintf("%v", vals)
			if strings.Contains(lfns, "[") {
				msg := "When sumOverLumi=1, no lfn list or run_num list allowed"
				return dbsError(w, msg)
			}
		}
	}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Addition"] = false
	tmpl["RunNumber"] = false
	tmpl["LumiList"] = false
	tmpl["Addition"] = false

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
	}
	// files API does not supprt run_num=1 when no lumi
	if len(runs) == 1 && len(lumis) == 0 && runs[0] == "1" {
		msg := "files API does not supprt run_num=1 when no lumi"
		return dbsError(w, msg)
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

	conds, args = AddParam("dataset", "D.DATASET", params, conds, args)
	conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	if _, e := getSingleValue(params, "release_version"); e == nil {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(params, "pset_hash"); e == nil {
		conds, args = AddParam("pset_hash", "PSH.PSET_HASH", params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(params, "app_name"); e == nil {
		conds, args = AddParam("app_name", "AEX.APP_NAME", params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(params, "output_module_label"); e == nil {
		conds, args = AddParam("output_module_label", "OMC.OUTPUT_MODULE_LABEL", params, conds, args)
		tmpl["Addition"] = true
	}
	conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", params, conds, args)

	// load our SQL statement
	stm, err := LoadTemplateSQL("files", tmpl)
	if err != nil {
		return 0, err
	}

	// add lfns conditions
	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		token, binds := TokenGenerator(lfns, 100, "lfns_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := " F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	}
	// add run conditions
	if len(runs) > 1 {
		token, whereRuns, bindsRuns := runsClause("FL", runs)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	} else if len(runs) == 1 {
		conds, args = AddParam("run_num", "FL.RUN_NUM", params, conds, args)
	}

	// add lumis conditions
	if len(lumis) > 1 {
		token, binds := TokenGenerator(lumis, 4000, "lumis_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := " FL.LUMI_SECTION_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lumis) == 1 {
		conds, args = AddParam("lumi_list", "FL.LUMI_SECTION_NUM", params, conds, args)
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFiles DBS API
func (API) InsertFiles(values Record) error {
	return InsertValues("insert_files", values)
}
