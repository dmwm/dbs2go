package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// Files DBS API
func (API) Files(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	var stm string

	tmpl := make(Record)
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
		stm = LoadTemplateSQL("files_sumoverlumi", tmpl)
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
		op, val := OperatorValue(lfns[0])
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		msg := "The files API does not support list of datasets"
		return 0, errors.New(msg)
	} else if len(datasets) == 1 {
		op, val := OperatorValue(datasets[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	block_names := getValues(params, "block_name")
	if len(block_names) > 1 {
		msg := "The files API does not support list of block_names"
		return 0, errors.New(msg)
	} else if len(block_names) == 1 {
		op, val := OperatorValue(block_names[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	relVersions := getValues(params, "release_version")
	if len(relVersions) == 1 {
		op, val := OperatorValue(relVersions[0])
		cond := fmt.Sprintf(" RV.RELEASE_VERSION %s %s", op, placeholder("release_version"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	psetHash := getValues(params, "pset_hash")
	if len(psetHash) == 1 {
		op, val := OperatorValue(psetHash[0])
		cond := fmt.Sprintf(" PSH.PSET_HASH %s %s", op, placeholder("pset_hash"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	appName := getValues(params, "app_name")
	if len(appName) == 1 {
		op, val := OperatorValue(appName[0])
		cond := fmt.Sprintf(" AEX.APP_NAME %s %s", op, placeholder("app_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	outModLabel := getValues(params, "output_module_label")
	if len(outModLabel) == 1 {
		op, val := OperatorValue(outModLabel[0])
		cond := fmt.Sprintf(" OMC.OUTPUT_MODULE_LABEL %s %s", op, placeholder("output_module_label"))
		conds = append(conds, cond)
		args = append(args, val)
	}
	origSiteName := getValues(params, "origin_site_name")
	if len(origSiteName) == 1 {
		op, val := OperatorValue(origSiteName[0])
		cond := fmt.Sprintf(" B.ORIGIN_SITE_NAME %s %s", op, placeholder("origin_site_name"))
		conds = append(conds, cond)
		args = append(args, val)
	}

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
