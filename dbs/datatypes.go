package dbs

import (
	"io"
	"net/http"
)

// DataTypes DBS API
func (API) DataTypes(params Record, sep string, w http.ResponseWriter) error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("datatype", "DT.DATATYPE", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datatypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, sep, stm, args...)
}

// InsertDataTypes DBS API
func (API) InsertDataTypes(r io.Reader, cby string) error {
	//     return InsertValues("insert_data_types", values)
	return nil
}
