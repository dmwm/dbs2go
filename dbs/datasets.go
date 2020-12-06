package dbs

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
)

// Datasets API
func (API) Datasets(params Record) []Record {
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
		where += fmt.Sprintf(" AND DP.DATASET_ACCESS_TYPE = %s", placeholder("dataset_access_type"))
		var vals []string
		genSQL, vals = tokens(datasets)
		for _, d := range vals {
			args = append(args, d, d, d) // append three values since tokens generates placeholders for them
		}
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		where += fmt.Sprintf(" AND D.DATASET %s %s", op, placeholder("dataset"))
		where += fmt.Sprintf(" AND DP.DATASET_ACCESS_TYPE = %s", placeholder("dataset_access_type"))
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
		msg := "The datasetparent API does not support list of datasetparent"
		return errorRecord(msg)
	} else if len(datasetparent) == 1 {
		op, val := opVal(datasetparent[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for datasetparent API")
		return errorRecord(msg)
	}
	// get SQL statement from static area
	stm := getSQL("datasetparent")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// datasetchildren API
func (API) DatasetChildren(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	datasetchildren := getValues(params, "dataset")
	if len(datasetchildren) > 1 {
		msg := "The datasetchildren API does not support list of datasetchildren"
		return errorRecord(msg)
	} else if len(datasetchildren) == 1 {
		op, val := opVal(datasetchildren[0])
		cond := fmt.Sprintf(" D.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		msg := fmt.Sprintf("No arguments for datasetchildren API")
		return errorRecord(msg)
	}
	// get SQL statement from static area
	stm := getSQL("datasetchildren")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// datasetaccesstypes API
func (API) DatasetAccessTypes(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := " WHERE "

	// parse dataset argument
	datasetaccesstypes := getValues(params, "dataset_access_type")
	if len(datasetaccesstypes) > 1 {
		msg := "The datasetaccesstypes API does not support list of datasetaccesstypes"
		return errorRecord(msg)
	} else if len(datasetaccesstypes) == 1 {
		op, val := opVal(datasetaccesstypes[0])
		cond := fmt.Sprintf(" DT.DATASET_ACCESS_TYPE %s %s", op, placeholder("dataset_access_type"))
		where += addCond(where, cond)
		args = append(args, val)
	} else {
		where = ""
	}
	// get SQL statement from static area
	stm := getSQL("datasetaccesstypes")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// Datasets API
func (API) DatasetsNew(params Record, w http.ResponseWriter) error {
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
		where += fmt.Sprintf(" AND DP.DATASET_ACCESS_TYPE = %s", placeholder("dataset_access_type"))
		var vals []string
		genSQL, vals = tokens(datasets)
		for _, d := range vals {
			args = append(args, d, d, d) // append three values since tokens generates placeholders for them
		}
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		where += fmt.Sprintf(" AND D.DATASET %s %s", op, placeholder("dataset"))
		where += fmt.Sprintf(" AND DP.DATASET_ACCESS_TYPE = %s", placeholder("dataset_access_type"))
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
	return executeNew(w, genSQL+stm+where, cols, vals, args...)
}
