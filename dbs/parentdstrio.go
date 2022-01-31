package dbs

// ParentDSTrio API
func (a *API) ParentDSTrio() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("datasetchildren")

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.parentdstrio.ParentDSTrio")
	}
	return nil
}
