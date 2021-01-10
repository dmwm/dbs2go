package dbs

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
)

// Datasets API
func (API) Datasets(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse detail arugment
	detail, _ := getSingleValue(params, "detail")

	// parse is_dataset_valid argument
	isValid, _ := getSingleValue(params, "is_dataset_valid")
	if isValid == "" {
		isValid = "1"
	}
	cond := fmt.Sprintf("D.IS_DATASET_VALID = %s", placeholder("is_dataset_valid"))
	conds = append(conds, cond)
	args = append(args, isValid)

	// parse dataset_id argument
	dataset_access_type, _ := getSingleValue(params, "dataset_access_type")
	if dataset_access_type == "" {
		dataset_access_type = "VALID"
	}
	cond = fmt.Sprintf("DP.DATASET_ACCESS_TYPE = %s", placeholder("dataset_access_type"))
	conds = append(conds, cond)
	args = append(args, dataset_access_type)

	// parse dataset argument
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		cond = fmt.Sprintf("D.DATASET in (SELECT TOKEN FROM TOKEN_GENERATOR)")
		token, binds := TokenGenerator(datasets, 100) // 100 is max for # of allowed datasets
		conds = append(conds, cond+token)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(datasets) == 1 {
		conds, args = AddParam("dataset", "D.DATASET", params, conds, args)
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
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return execute(w, stm, cols, vals, args...)
}

// InsertDatasets DBS API
func (API) InsertDatasets(values Record) error {
	args := make(Record)
	args["Owner"] = DBOWNER
	return InsertTemplateValues("insert_datasets", args, values)
}
