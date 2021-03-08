package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	"unsafe"

	"github.com/vkuznet/dbs2go/utils"
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
		conds, args = AddParam("global_tag", "O.GLOBAL_TAG", params, conds, args)
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

// OutputConfigs
type OutputConfigs struct {
	OUTPUT_MOD_CONFIG_ID  int64  `json:"output_mod_config_id"`
	APP_EXEC_ID           int64  `json:"app_exec_id"`
	RELEASE_VERSION_ID    int64  `json:"release_version_id"`
	PARAMETER_SET_HASH_ID int64  `json:"parameter_set_hash_id"`
	OUTPUT_MODULE_LABEL   string `json:"output_module_label"`
	GLOBAL_TAG            string `json:"global_tag"`
	SCENARIO              string `json:"scenario"`
	CREATION_DATE         int64  `json:"creation_date"`
	CREATE_BY             string `json:"create_by"`
}

// Insert implementation of OutputConfigs
func (r *OutputConfigs) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.OUTPUT_MOD_CONFIG_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id")
			r.OUTPUT_MOD_CONFIG_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_OMC")
			r.OUTPUT_MOD_CONFIG_ID = tid
		}
		if err != nil {
			return err
		}
	}
	err = r.Validate()
	if err != nil {
		log.Printf("fail to validate output config record\n%+v\nerror %v", r, err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_outputconfigs")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_outputconfigs_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert OutputConfigs\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.OUTPUT_MOD_CONFIG_ID, r.APP_EXEC_ID, r.RELEASE_VERSION_ID, r.PARAMETER_SET_HASH_ID, r.OUTPUT_MODULE_LABEL, r.GLOBAL_TAG, r.SCENARIO, r.CREATION_DATE, r.CREATE_BY)
	return err
}

// Validate implementation of OutputConfigs
func (r *OutputConfigs) Validate() error {
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	if r.APP_EXEC_ID == 0 {
		return errors.New("missing app_exec_id")
	}
	if r.RELEASE_VERSION_ID == 0 {
		return errors.New("missing release_version_id")
	}
	if r.PARAMETER_SET_HASH_ID == 0 {
		return errors.New("missing parameter_set_hash_id")
	}
	if r.OUTPUT_MODULE_LABEL == "" {
		return errors.New("missing data_output_module_label")
	}
	if r.CREATION_DATE == 0 {
		return errors.New("missing creation_date")
	}
	if r.CREATE_BY == "" {
		return errors.New("missing create_by")
	}
	return nil
}

// Decode implementation for OutputConfigs
func (r *OutputConfigs) Decode(reader io.Reader) (int64, error) {
	// init record with given data record
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return 0, err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return 0, err
	}
	size := int64(len(data))
	return size, nil
}

// Size implementation for OutputConfigs
func (r *OutputConfigs) Size() int64 {
	size := int64(unsafe.Sizeof(*r))
	size += int64(len(r.OUTPUT_MODULE_LABEL))
	size += int64(len(r.CREATE_BY))
	return size
}

// OutputConfigRecord represents input to InsertOutputConfigs API
type OutputConfigRecord struct {
	APP_NAME            string `json:"app_name"`
	RELEASE_VERSION     string `json:"release_version"`
	PSET_HASH           string `json:"pset_hash"`
	PSET_NAME           string `json:"pset_name"`
	GLOBAL_TAG          string `json:"global_tag"`
	OUTPUT_MODULE_LABEL string `json:"output_module_label"`
	CREATION_DATE       int64  `json:"creation_date"`
	CREATE_BY           string `json:"create_by"`
	SCENARIO            string `json:"scenario"`
}

// InsertOutputConfigs DBS API
func (API) InsertOutputConfigs(r io.Reader) (int64, error) {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSOutputConfig.py
	// intput values: app_name, release_version, pset_hash, global_tag and output_module_label
	// creation_date, create_by
	// optional: scenario, pset_name

	// read given input
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println("fail to read data", err)
		return 0, err
	}
	size := int64(len(data))
	var rec OutputConfigRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return 0, err
	}
	if rec.CREATION_DATE == 0 {
		rec.CREATION_DATE = time.Now().Unix()
	}
	if rec.CREATE_BY == "" {
		rec.CREATE_BY = CreateBy()
	}
	arec := ApplicationExecutables{APP_NAME: rec.APP_NAME}
	rrec := ReleaseVersions{RELEASE_VERSION: rec.RELEASE_VERSION}
	prec := ParameterSetHashes{PSET_HASH: rec.PSET_HASH, PSET_NAME: rec.PSET_NAME}
	orec := OutputConfigs{GLOBAL_TAG: rec.GLOBAL_TAG, OUTPUT_MODULE_LABEL: rec.OUTPUT_MODULE_LABEL, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY, SCENARIO: rec.SCENARIO}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()
	err = arec.Insert(tx)
	if err != nil {
		return 0, err
	}
	err = rrec.Insert(tx)
	if err != nil {
		return 0, err
	}
	err = prec.Insert(tx)
	if err != nil {
		return 0, err
	}

	// init all foreign Id's in output config record
	orec.APP_EXEC_ID = arec.APP_EXEC_ID
	orec.RELEASE_VERSION_ID = rrec.RELEASE_VERSION_ID
	orec.PARAMETER_SET_HASH_ID = prec.PARAMETER_SET_HASH_ID
	err = orec.Insert(tx)
	if err != nil {
		return 0, err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("faile to insert_outputconfigs_sqlite", err)
		return 0, err
	}
	return size, err
}
