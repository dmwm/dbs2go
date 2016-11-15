package dbs

import (
	"database/sql"
	"fmt"
)

// datasets API
func (API) Datasets(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse is_dataset_valid argument
	isValid := getSingleValue(params, "is_dataset_valid")
	if isValid == "" {
		isValid = "1"
	}
	where += fmt.Sprintf("D.IS_DATASET_VALID = %s", placeholder("is_dataset_valid"))
	args = append(args, isValid)

	// parse dataset argument
	datasets := getValues(params, "dataset")
	genSQL := ""
	if len(datasets) > 1 {
		where += fmt.Sprintf(" AND D.DATASET in (SELECT TOKEN FROM TOKEN_GENERATOR)")
		var vals []string
		genSQL, vals = tokens(datasets)
		for _, d := range vals {
			args = append(args, d)
		}
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		where += fmt.Sprintf(" AND D.DATASET %s %s", op, placeholder("dataset"))
		args = append(args, val)
	}

	// get SQL statement from static area
	stm := getSQL("datasets")
	cols := []string{"dataset_id", "dataset", "prep_id", "xtcrosssection", "creation_date", "create_by", "last_modification_date", "last_modified_by", "primary_ds_name", "primary_ds_type", "processed_ds_name", "data_tier_name", "dataset_access_type", "acquisition_era_name", "processing_version", "physics_group_name"}
	vals := []interface{}{new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullFloat64), new(sql.NullInt64), new(sql.NullString), new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
	// use generic query API to fetch the results from DB
	return execute(genSQL+stm+where, cols, vals, args...)
}

// datasetparent API
func (API) DatasetParent(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	datasetparent := getValues(params, "dataset")
	if len(datasetparent) > 1 {
		panic("The datasetparent API does not support list of datasetparent")
	} else if len(datasetparent) == 1 {
		op, val := opVal(datasetparent[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for datasetparent API")
		panic(msg)
	}
	// get SQL statement from static area
	stm := getSQL("datasetparent")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
