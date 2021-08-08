package dbs

// ParentDatasetFileLumiIds API
func (a *API) ParentDatasetFileLumiIds() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	// create our SQL statement
	stm, err := LoadTemplateSQL("parentdatasetfilelumiids", tmpl)
	if err != nil {
		return err
	}

	// add dataset condition
	conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}
