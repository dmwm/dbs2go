package dbs

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
)

// Datasets API
func (API) Datasets(params Record, w http.ResponseWriter) (int64, error) {
	log.Printf("datasets params %+v", params)
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["TokenGenerator"] = ""
	tmpl["Runs"] = false
	tmpl["Lfns"] = false
	tmpl["Version"] = false
	tmpl["ParentDataset"] = false
	tmpl["Detail"] = false

	// run_num shouhld come first since it may produce TokenGenerator
	// whose bind parameters should appear first
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		tmpl["Runs"] = true
		token, whereRuns, bindsRuns := runsClause("FLLU", runs)
		tmpl["TokenGenerator"] = token
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}

	// parse detail arugment
	detail, _ := getSingleValue(params, "detail")
	if detail == "1" { // for backward compatibility with Python detail=1 and detail=True
		detail = "true"
	}
	if strings.ToLower(detail) == "true" {
		tmpl["Detail"] = true
	}

	// parse dataset argument
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		cond := fmt.Sprintf("D.DATASET in (SELECT TOKEN FROM TOKEN_GENERATOR)")
		token, binds := TokenGenerator(datasets, 100, "dataset_token") // 100 is max for # of allowed datasets
		conds = append(conds, cond+token)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(datasets) == 1 {
		conds, args = AddParam("dataset", "D.DATASET", params, conds, args)
	}

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
	oper := "="
	if dataset_access_type == "" {
		dataset_access_type = "VALID"
	} else if dataset_access_type == "*" {
		dataset_access_type = "%"
		oper = "like"
	}
	cond = fmt.Sprintf("DP.DATASET_ACCESS_TYPE %s %s", oper, placeholder("dataset_access_type"))
	conds = append(conds, cond)
	args = append(args, dataset_access_type)

	// optional arguments
	if _, e := getSingleValue(params, "parent_dataset"); e == nil {
		tmpl["ParentDataset"] = true
		conds, args = AddParam("parent_dataset", "PDS.DATASET PARENT_DATASET", params, conds, args)
	}
	if _, e := getSingleValue(params, "release_version"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
	}
	if _, e := getSingleValue(params, "pset_hash"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("pset_hash", "PSH.PSET_HASH", params, conds, args)
	}
	if _, e := getSingleValue(params, "app_name"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("app_name", "AEX.APP_NAME", params, conds, args)
	}
	if _, e := getSingleValue(params, "output_module_label"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("output_module_label", "OMC.OUTPUT_MODULE_LABEL", params, conds, args)
	}
	if _, e := getSingleValue(params, "logical_file_name"); e == nil {
		tmpl["Lfns"] = true
		conds, args = AddParam("logical_file_name", "FL.LOGICAL_FILE_NAME", params, conds, args)
	}
	conds, args = AddParam("primary_ds_name", "P.PRIMARY_DS_NAME", params, conds, args)
	conds, args = AddParam("processed_ds_name", "PD.PROCESSED_DS_NAME", params, conds, args)
	conds, args = AddParam("data_tier_name", "DT.DATA_TIER_NAME", params, conds, args)
	conds, args = AddParam("primary_ds_type", "PDT.PRIMARY_DS_TYPE", params, conds, args)
	conds, args = AddParam("physics_group_name", "PH.PHYSICS_GROUP_NAME", params, conds, args)
	conds, args = AddParam("global_tag", "OMC.GLOBAL_TAG", params, conds, args)
	conds, args = AddParam("processing_version", "PE.PROCESSING_VERSION", params, conds, args)
	conds, args = AddParam("acqusition_era", "AE.ACQUISITION_ERA_NAME", params, conds, args)
	conds, args = AddParam("cdate", "D.CREATION_DATE", params, conds, args)
	minDate := getValues(params, "min_cdate")
	maxDate := getValues(params, "max_cdate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE BETWEEN %s and %s", placeholder("min_cdate"), placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE > %s", placeholder("min_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE < %s", placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}
	conds, args = AddParam("ldate", "D.LAST_MODIFICATION_DATE", params, conds, args)
	minDate = getValues(params, "min_ldate")
	maxDate = getValues(params, "max_ldate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE BETWEEN %s and %s", placeholder("min_ldate"), placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE > %s", placeholder("min_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE < %s", placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}
	conds, args = AddParam("create_by", "D.CREATE_BY", params, conds, args)
	conds, args = AddParam("last_modified_by", "D.LAST_MODIFIED_BY", params, conds, args)
	conds, args = AddParam("prep_id", "D.PREP_ID", params, conds, args)
	conds, args = AddParam("dataset_id", "D.DATASET_ID", params, conds, args)

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("datasets", tmpl)
	if err != nil {
		return 0, err
	}
	cols := []string{"dataset_id", "dataset", "prep_id", "xtcrosssection", "creation_date", "create_by", "last_modification_date", "last_modified_by", "primary_ds_name", "primary_ds_type", "processed_ds_name", "data_tier_name", "dataset_access_type", "acquisition_era_name", "processing_version", "physics_group_name"}
	vals := []interface{}{new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullFloat64), new(sql.NullInt64), new(sql.NullString), new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
	if strings.ToLower(detail) != "true" {
		//         stm = getSQL("datasets_short")
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
