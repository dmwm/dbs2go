package dbs

// DataTypes DBS API
func (a *API) DataTypes() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Dataset"] = false

	conds, args = AddParam("datatype", "PDT.PRIMARY_DS_TYPE", a.Params, conds, args)
	datasets := getValues(a.Params, "dataset")
	if len(datasets) == 1 {
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "DS.DATASET", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("datatypes", tmpl)
	if err != nil {
		return err
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// InsertDataTypes DBS API
func (a *API) InsertDataTypes() error {
	//     return InsertValues("insert_data_types", values)
	return nil
}
