package dbs

import (
	"fmt"
)

// outputconfigs API
func (API) OutputConfigs(params Record) []Record {
	// variables we'll use in where clause
	var sql1, sql2, stm string
	var args []interface{}
	where := " WHERE "
	sql1 = fmt.Sprintf(" SELECT R.RELEASE_VERSION, P.PSET_HASH, P.PSET_NAME, A.APP_NAME, O.OUTPUT_MODULE_LABEL, O.GLOBAL_TAG, O.CREATION_DATE, O.CREATE_BY ")
	sql2 = fmt.Sprintf(" FROM %s.OUTPUT_MODULE_CONFIGS O JOIN %s.RELEASE_VERSIONS R ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID JOIN %s.APPLICATION_EXECUTABLES A  ON O.APP_EXEC_ID=A.APP_EXEC_ID  JOIN %s.PARAMETER_SET_HASHES P ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID ", DBOWNER, DBOWNER, DBOWNER, DBOWNER)

	bid := "0"
	block_id := getValues(params, "block_id")
	if len(block_id) > 1 {
		msg := "The outputconfigs API does not support list of block_id"
		return errorRecord(msg)
	} else if len(block_id) == 1 {
		_, bid = opVal(block_id[0])
	}
	if bid == "0" {
		stm = sql1 + sql2
		dataset := getValues(params, "dataset")
		if len(dataset) == 1 {
			stm += fmt.Sprintf(" JOIN %s.DATASET_OUTPUT_MOD_CONFIGS DC ON DC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID", DBOWNER)
			stm += fmt.Sprintf(" JOIN %s.DATASETS DS ON DS.DATASET_ID=DC.DATASET_ID", DBOWNER)
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
			where += addCond(where, cond)
			args = append(args, val)
		}
		lfn := getValues(params, "logical_file_name")
		if len(lfn) == 1 {
			stm += fmt.Sprintf(" JOIN %s.FILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID", DBOWNER)
			stm += fmt.Sprintf(" JOIN %s.FILES FS ON FS.FILE_ID=FC.FILE_ID", DBOWNER)
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" FS.LOGICAL_FILE_NAME %s %s", op, placeholder("logical_file_name"))
			where += addCond(where, cond)
			args = append(args, val)
		}
		app_name := getValues(params, "app_name")
		if len(app_name) == 1 {
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" A.APP_NAME %s %s", op, placeholder("app_name"))
			where += addCond(where, cond)
			args = append(args, val)
		}
		release_version := getValues(params, "release_version")
		if len(release_version) == 1 {
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" R.RELEASE_VERSION %s %s", op, placeholder("release_version"))
			where += addCond(where, cond)
			args = append(args, val)
		}
		pset_hash := getValues(params, "pset_hash")
		if len(pset_hash) == 1 {
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" P.PSET_HASH %s %s", op, placeholder("pset_hash"))
			where += addCond(where, cond)
			args = append(args, val)
		}
		output_module_label := getValues(params, "output_module_label")
		if len(output_module_label) == 1 {
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" P.OUTPUT_MODULE_LABEL %s %s", op, placeholder("output_module_label"))
			where += addCond(where, cond)
			args = append(args, val)
		}
		global_tag := getValues(params, "global_tag")
		if len(global_tag) == 1 {
			op, val := opVal(dataset[0])
			cond := fmt.Sprintf(" P.GLOBAL_TAG %s %s", op, placeholder("global_tag"))
			where += addCond(where, cond)
			args = append(args, val)
		}
	} else {
		stm = sql1 + " , FS.LOGICAL_FILE_NAME LFN " + sql2 + fmt.Sprint(" JOIN %s.FILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID", DBOWNER) + fmt.Sprintf(" JOIN %s.FILES FS ON FS.FILE_ID=FC.FILE_ID", DBOWNER)
	}

	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}
