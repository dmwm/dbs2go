package dbs

import (
	"errors"
	"fmt"
	"log"
	"net/http"
)

// OutputConfigs DBS API
func (API) OutputConfigs(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	bid := "0"
	block_id := getValues(params, "block_id")
	if len(block_id) > 1 {
		msg := "The outputconfigs API does not support list of block_id"
		return 0, errors.New(msg)
	} else if len(block_id) == 1 {
		_, bid = OperatorValue(block_id[0])
	}
	if bid == "0" {
		tmpl["Main"] = true
		dataset := getValues(params, "dataset")
		if len(dataset) == 1 {
			tmpl["Dataset"] = true
			conds, args = AddParam("dataset", "DS.DATASET", params, conds, args)
		}
		lfn := getValues(params, "logical_file_name")
		if len(lfn) == 1 {
			tmpl["Lfn"] = true
			conds, args = AddParam("logical_file_name", "FS.LOGICAL_FILE_NAME", params, conds, args)
		}
		conds, args = AddParam("app_name", "A.APP_NAME", params, conds, args)
		conds, args = AddParam("release_version", "R.RELEASE_VERSION", params, conds, args)
		conds, args = AddParam("pset_hash", "P.PSET_HASH", params, conds, args)
		conds, args = AddParam("output_module_label", "O.OUTPUT_MODULE_LABEL", params, conds, args)
		conds, args = AddParam("global_tag", "P.GLOBAL_TAG", params, conds, args)
	} else {
		tmpl["Main"] = false
	}
	stm, err := LoadTemplateSQL("outputconfigs", tmpl)
	if err != nil {
		return 0, err
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertOutputConfigs DBS API
func (API) InsertOutputConfigs(values Record) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSOutputConfig.py
	// intput values: app_name, release_version, pset_hash, global_tag and output_module_label
	// creation_date, create_by
	// optional: scenario, pset_name
	optional := []string{"scenario"}
	for _, key := range optional {
		if _, ok := values[key]; !ok {
			values[key] = ""
		}
	}
	params := []string{"app_name", "release_version", "pset_hash", "global_tag", "output_module_label", "creation_date", "create_by"}
	params = append(params, optional...)
	if err := checkParams(values, params); err != nil {
		return err
	}
	if DBOWNER == "sqlite" {
		// we decouple multiple inserts into individual one

		// start transaction
		tx, err := DB.Begin()
		if err != nil {
			msg := fmt.Sprintf("unable to get DB transaction %v", err)
			return errors.New(msg)
		}
		defer tx.Rollback()
		aid, err := LastInsertId(tx, "APPLICATION_EXECUTABLES", "app_exec_id")
		if err != nil {
			log.Println("fail to obtain app_exec_id", err)
			return err
		}
		vals := make(Record)
		vals["app_exec_id"] = fmt.Sprintf("%d", aid+1)
		vals["app_name"] = values["app_name"]
		err = InsertValuesTxt(tx, "insert_application_executables", vals)
		values["app_exec_id"] = fmt.Sprintf("%d", aid+1)
		delete(values, "app_name")

		rid, err := LastInsertId(tx, "RELEASE_VERSIONS", "release_version_id")
		if err != nil {
			log.Println("fail to obtain release_version_id", err)
			return err
		}
		vals = make(Record)
		vals["release_version_id"] = fmt.Sprintf("%d", rid+1)
		vals["release_version"] = values["release_version"]
		err = InsertValuesTxt(tx, "insert_release_versions", vals)
		values["release_version_id"] = fmt.Sprintf("%d", rid+1)
		delete(values, "release_version")

		pid, err := LastInsertId(tx, "PARAMETER_SET_HASHES", "parameter_set_hash_id")
		if err != nil {
			log.Println("fail to obtain parameter_set_hash_id", err)
			return err
		}
		vals = make(Record)
		vals["parameter_set_hash_id"] = fmt.Sprintf("%d", pid+1)
		if _, ok := values["pset_name"]; ok {
			vals["pset_name"] = values["pset_name"]
		} else {
			vals["pset_name"] = ""
		}
		vals["pset_hash"] = values["pset_hash"]
		err = InsertValuesTxt(tx, "insert_parameter_set_hashes", vals)
		values["parameter_set_hash_id"] = fmt.Sprintf("%d", pid+1)
		delete(values, "pset_hash")

		oid, err := LastInsertId(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id")
		if err != nil {
			log.Println("fail to obtain output_mod_config_id", err)
			return err
		}
		values["output_mod_config_id"] = fmt.Sprintf("%d", oid+1)
		err = InsertValuesTxt(tx, "insert_outputconfigs_sqlite", values)
		if err != nil {
			log.Println("fail to insert_outputconfigs_sqlite", err)
			return err
		}

		// commit transaction
		err = tx.Commit()
		if err != nil {
			log.Println("faile to insert_outputconfigs_sqlite", err)
			return err
		}
		return err
	}
	return InsertValues("insert_outputconfigs", values)
}
