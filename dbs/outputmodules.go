package dbs

import (
	"net/http"
)

// OutputModules DBS API
func (API) OutputModules(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["BlockId"] = false
	tmpl["Dataset"] = false
	tmpl["Lfn"] = false

	if v, _ := getSingleValue(params, "block_id"); v != "" {
		conds, args = AddParam("block_id", "FS.BLOCK_ID", params, conds, args)
		tmpl["BlockId"] = true
	}
	if v, _ := getSingleValue(params, "dataset"); v != "" {
		conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
		tmpl["Dataset"] = true
	}
	if v, _ := getSingleValue(params, "logical_file_name"); v != "" {
		conds, args = AddParam("logical_file_name", "FS.LOGICAL_FILE_NAME", params, conds, args)
		tmpl["Lfn"] = true
	}
	conds, args = AddParam("app_name", "A.APP_NAME", params, conds, args)
	conds, args = AddParam("pset_hash", "P.PSET_HASH", params, conds, args)
	conds, args = AddParam("release_version", "R.RELEASE_VERSION", params, conds, args)
	conds, args = AddParam("output_label", "O.OUTPUT_MODULE_LABEL", params, conds, args)
	conds, args = AddParam("global_tag", "O.GLOBAL_TAG", params, conds, args)

	// get SQL statement from static area
	stm := LoadTemplateSQL("outputmodule", tmpl)
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertOutputModules DBS API
func (API) InsertOutputModules(values Record) error {
	return InsertValues("insert_outputmodule", values)
	args := make(Record)
	args["Owner"] = DBOWNER
	return InsertTemplateValues("insert_outputmodule", args, values)
}
