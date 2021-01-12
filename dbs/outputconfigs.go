package dbs

import (
	"errors"
	"net/http"
)

// OutputConfigs DBS API
func (API) OutputConfigs(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	bid := "0"
	block_id := getValues(params, "block_id")
	if len(block_id) > 1 {
		msg := "The outputconfigs API does not support list of block_id"
		return 0, errors.New(msg)
	} else if len(block_id) == 1 {
		_, bid = OperatorValue(block_id[0])
	}
	if bid == "0" {
		tmpl["Main"] = true
		dataset := getValues(params, "dataset")
		if len(dataset) == 1 {
			tmpl["Dataset"] = true
			conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
		}
		lfn := getValues(params, "logical_file_name")
		if len(lfn) == 1 {
			tmpl["Lfn"] = true
			conds, args = AddParam("logical_file_name", "FS.LOGICAL_FILE_NAME", params, conds, args)
		}
		conds, args = AddParam("app_name", "A.APP_NAME", params, conds, args)
		conds, args = AddParam("release_version", "R.RELEASE_VERSION", params, conds, args)
		conds, args = AddParam("pset_hash", "P.PSET_HASH", params, conds, args)
		conds, args = AddParam("output_module_label", "P.OUTPUT_MODULE_LABEL", params, conds, args)
		conds, args = AddParam("global_tag", "P.GLOBAL_TAG", params, conds, args)
	} else {
		tmpl["Main"] = false
	}
	stm, err := LoadTemplateSQL("outputconfigs", tmpl)
	if err != nil {
		return 0, err
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertOutputConfigs DBS API
func (API) InsertOutputConfigs(values Record) error {
	return InsertValues("insert_output_configs", values)
}
