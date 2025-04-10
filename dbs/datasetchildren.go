package dbs

// DatasetChildren API
func (a *API) DatasetChildren() error {
	var args []interface{}
	var conds []string

	// parse dataset argument
	datasetchildren := getValues(a.Params, "dataset")
	if len(datasetchildren) > 1 {
		msg := "The datasetchildren API does not support list of datasetchildren"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasetchildren.DatasetChildren")
	} else if len(datasetchildren) == 1 {
		conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("datasetchildren")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query dataset children", "dbs.datasetchildren.DatasetChildren")
	}
	return nil
}

// InsertDatasetChildren DBS API
func (a *API) InsertDatasetChildren() error {
	//     return InsertValues("insert_dataset_children", values)
	return nil
}
