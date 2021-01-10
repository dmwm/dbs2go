package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// DataTypes DBS API
func (API) DataTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	datatypes := getValues(params, "datatype")
	if len(datatypes) > 1 {
		msg := "The datatypes API does not support list of datatypes"
		return 0, errors.New(msg)
	} else if len(datatypes) == 1 {
		op, val := OperatorValue(datatypes[0])
		cond := fmt.Sprintf(" DT.datatype %s %s", op, placeholder("datatype"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("datatypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDataTypes DBS API
func (API) InsertDataTypes(values Record) error {
	return InsertValues("insert_data_types", values)
}
