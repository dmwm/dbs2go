package dbs

import (
	"fmt"
	"strings"
)

// helper function to generate token's SQL statement out of given datasets
func tokens(datasets []string) (string, []string) {
	var vals []string
	values := ""
	limit := 100
	for _, d := range datasets {
		if values == "" { // first time
			values = d
			continue
		}
		if len(values)+1+len(d) < limit {
			values += fmt.Sprintf(",%s", d)
		} else {
			vals = append(vals, values)
			values = d
		}
	}

	stm := ""
	for i, _ := range vals {
		if i > 0 {
			stm += "\n UNION ALL \n"
		}
		t := fmt.Sprintf(":token%d", i)
		stm += fmt.Sprintf("SELECT REGEXP_SUBSTR(%s, '[^,]+', 1, LEVEL) token FROM DUAL ", t)
		stm += fmt.Sprintf("CONNECT BY LEVEL <= LENGTH(%s) - LENGTH(REPLACE(%s, ',', '')) + 1\n", t, t)
	}
	out := fmt.Sprintf("WITH TOKEN_GENERATOR AS(\n%s)", stm)
	return out, vals
}

// helper function to generate operator, value pair for given argument
func opVal(arg string) (string, string) {
	op := "="
	val := arg
	if strings.Contains(arg, "*") {
		op = "like"
		val = strings.Replace(arg, "*", "%", -1)
	}
	return op, val
}

// datasets API
func datasets(params Record) []Record {
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
	//    vals := []interface{}{new(uint64),new(string),new(string),new(float64),new(uint64),new(string),new(uint64),new(string),new(string),new(string),new(string),new(string),new(string),new(string),new(string),new(string)}
	// use generic query API to fetch the results from DB
	return execute(genSQL+stm+where, cols, args...)
	//    return execute(genSQL+stm+where, cols, vals, args...)
	//    return executeAll(genSQL+stm+where, args...)
}
