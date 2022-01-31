package dbs

// OutputModules DBS API
func (a *API) OutputModules() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["BlockId"] = false
	tmpl["Dataset"] = false
	tmpl["Lfn"] = false

	if v, _ := getSingleValue(a.Params, "block_id"); v != "" {
		conds, args = AddParam("block_id", "FS.BLOCK_ID", a.Params, conds, args)
		tmpl["BlockId"] = true
	}
	if v, _ := getSingleValue(a.Params, "dataset"); v != "" {
		conds, args = AddParam("dataset", "DS.DATASET", a.Params, conds, args)
		tmpl["Dataset"] = true
	}
	if v, _ := getSingleValue(a.Params, "logical_file_name"); v != "" {
		conds, args = AddParam("logical_file_name", "FS.LOGICAL_FILE_NAME", a.Params, conds, args)
		tmpl["Lfn"] = true
	}
	conds, args = AddParam("app_name", "A.APP_NAME", a.Params, conds, args)
	conds, args = AddParam("pset_hash", "P.PSET_HASH", a.Params, conds, args)
	conds, args = AddParam("release_version", "R.RELEASE_VERSION", a.Params, conds, args)
	conds, args = AddParam("output_label", "O.OUTPUT_MODULE_LABEL", a.Params, conds, args)
	conds, args = AddParam("global_tag", "O.GLOBAL_TAG", a.Params, conds, args)

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("outputmodule", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.outputmodules.OutputModules")
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.outputmodules.OutputModules")
	}
	return nil
}

// InsertOutputModules DBS API
// func (a *API) InsertOutputModules(values Record) error {
//     args := make(Record)
//     args["Owner"] = DBOWNER
//     return InsertTemplateValues("insert_outputmodule", args, values)
// }
