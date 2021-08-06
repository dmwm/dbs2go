package dbs

// ParentDSTrio API
func (a API) ParentDSTrio() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("datasetchildren")

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}
