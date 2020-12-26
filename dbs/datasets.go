package dbs

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
)

// Datasets API
func (API) Datasets(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse detail arugment
	detail := getSingleValue(params, "detail")

	// parse is_dataset_valid argument
	isValid := getSingleValue(params, "is_dataset_valid")
	if isValid == "" {
		isValid = "1"
	}
	where += fmt.Sprintf("D.IS_DATASET_VALID = %s", placeholder("is_dataset_valid"))
	args = append(args, isValid)

	// parse dataset_id argument
	dataset_access_type := getSingleValue(params, "dataset_access_type")
	if dataset_access_type == "" {
		dataset_access_type = "VALID"
	}
	where += fmt.Sprintf(" AND DP.DATASET_ACCESS_TYPE = %s", placeholder("dataset_access_type"))
	args = append(args, dataset_access_type)

	// parse dataset argument
	datasets := getValues(params, "dataset")
	genSQL := ""
	if len(datasets) > 1 {
		where += fmt.Sprintf(" AND D.DATASET in (SELECT TOKEN FROM TOKEN_GENERATOR)")
		var vals []string
		genSQL, vals = tokens(datasets)
		for _, d := range vals {
			args = append(args, d, d, d) // append three values since tokens generates placeholders for them
		}
	} else if len(datasets) == 1 {
		op, val := OperatorValue(datasets[0])
		where += fmt.Sprintf(" AND D.DATASET %s %s", op, placeholder("dataset"))
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("datasets")
	cols := []string{"dataset_id", "dataset", "prep_id", "xtcrosssection", "creation_date", "create_by", "last_modification_date", "last_modified_by", "primary_ds_name", "primary_ds_type", "processed_ds_name", "data_tier_name", "dataset_access_type", "acquisition_era_name", "processing_version", "physics_group_name"}
	vals := []interface{}{new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullFloat64), new(sql.NullInt64), new(sql.NullString), new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
	if strings.ToLower(detail) != "true" {
		stm = getSQL("datasets_short")
		cols = []string{"dataset"}
		vals = []interface{}{new(sql.NullString)}
	}
	// use generic query API to fetch the results from DB
	return execute(w, genSQL+stm+where, cols, vals, args...)
}
