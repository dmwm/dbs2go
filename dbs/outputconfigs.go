package dbs

import (
	"errors"
	"fmt"
	"net/http"
)

// OutputConfigs DBS API
func (API) OutputConfigs(params Record, w http.ResponseWriter) (int64, error) {
	var sql1, sql2, stm string
	var args []interface{}
	var conds []string
	sql1 = fmt.Sprintf(" SELECT R.RELEASE_VERSION, P.PSET_HASH, P.PSET_NAME, A.APP_NAME, O.OUTPUT_MODULE_LABEL, O.GLOBAL_TAG, O.CREATION_DATE, O.CREATE_BY ")
	sql2 = fmt.Sprintf(" FROM %s.OUTPUT_MODULE_CONFIGS O JOIN %s.RELEASE_VERSIONS R ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID JOIN %s.APPLICATION_EXECUTABLES A  ON O.APP_EXEC_ID=A.APP_EXEC_ID  JOIN %s.PARAMETER_SET_HASHES P ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID ", DBOWNER, DBOWNER, DBOWNER, DBOWNER)

	bid := "0"
	block_id := getValues(params, "block_id")
	if len(block_id) > 1 {
		msg := "The outputconfigs API does not support list of block_id"
		return 0, errors.New(msg)
	} else if len(block_id) == 1 {
		_, bid = OperatorValue(block_id[0])
	}
	if bid == "0" {
		stm = sql1 + sql2
		dataset := getValues(params, "dataset")
		if len(dataset) == 1 {
			stm += fmt.Sprintf(" JOIN %s.DATASET_OUTPUT_MOD_CONFIGS DC ON DC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID", DBOWNER)
			stm += fmt.Sprintf(" JOIN %s.DATASETS DS ON DS.DATASET_ID=DC.DATASET_ID", DBOWNER)
			conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
		}
		lfn := getValues(params, "logical_file_name")
		if len(lfn) == 1 {
			stm += fmt.Sprintf(" JOIN %s.FILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID", DBOWNER)
			stm += fmt.Sprintf(" JOIN %s.FILES FS ON FS.FILE_ID=FC.FILE_ID", DBOWNER)
			conds, args = AddParam("logical_file_name", "FS.LOGICAL_FILE_NAME", params, conds, args)
		}
		conds, args = AddParam("app_name", "A.APP_NAME", params, conds, args)
		conds, args = AddParam("release_version", "R.RELEASE_VERSION", params, conds, args)
		conds, args = AddParam("pset_hash", "P.PSET_HASH", params, conds, args)
		conds, args = AddParam("output_module_label", "P.OUTPUT_MODULE_LABEL", params, conds, args)
		conds, args = AddParam("global_tag", "P.GLOBAL_TAG", params, conds, args)
	} else {
		stm = sql1 + " , FS.LOGICAL_FILE_NAME LFN " + sql2 + fmt.Sprint(" JOIN %s.FILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID", DBOWNER) + fmt.Sprintf(" JOIN %s.FILES FS ON FS.FILE_ID=FC.FILE_ID", DBOWNER)
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertOutputConfigs DBS API
func (API) InsertOutputConfigs(values Record) error {
	return InsertValues("insert_output_configs", values)
}
