package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// PrimaryDSTypes DBS API
func (API) PrimaryDSTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	primarydstypes := getValues(params, "primary_ds_type")
	if len(primarydstypes) > 1 {
		msg := "The primarydstypes API does not support list of primarydstypes"
		return 0, errors.New(msg)
	} else if len(primarydstypes) == 1 {
		op, val := OperatorValue(primarydstypes[0])
		cond := fmt.Sprintf(" PDT.PRIMARY_DS_TYPE %s %s", op, placeholder("primary_ds_type"))
		conds = append(conds, cond)
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("primarydstypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertPrimaryDSTypes DBS API
func (API) InsertPrimaryDSTypes(values Record) error {
	return InsertValues("insert_primary_ds_types", values)
}
