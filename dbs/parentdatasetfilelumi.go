package dbs

import (
	"net/http"
)

// ParentDatasetFileLumiIds API
func (API) ParentDatasetFileLumiIds(params Record, sep string, w http.ResponseWriter) error {
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
	conds, args = AddParam("dataset", "D.DATASET", params, conds, args)

	// use generic query API to fetch the results from DB
	return executeAll(w, sep, stm, args...)
}
