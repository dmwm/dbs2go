package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"
	"unsafe"

	"github.com/dmwm/dbs2go/utils"
)

// OutputConfigs DBS API
func (a *API) OutputConfigs() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	bid := "0"
	blockID := getValues(a.Params, "block_id")
	if len(blockID) > 1 {
		msg := "The outputconfigs API does not support list of block_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.outputconfigs.OutputConfigs")
	} else if len(blockID) == 1 {
		_, bid = OperatorValue(blockID[0])
	}
	if bid == "0" {
		tmpl["Main"] = true
		dataset := getValues(a.Params, "dataset")
		if len(dataset) == 1 {
			tmpl["Dataset"] = true
			conds, args = AddParam("dataset", "DS.DATASET", a.Params, conds, args)
		}
		lfn := getValues(a.Params, "logical_file_name")
		if len(lfn) == 1 {
			tmpl["Lfn"] = true
			conds, args = AddParam("logical_file_name", "FS.LOGICAL_FILE_NAME", a.Params, conds, args)
		}
		conds, args = AddParam("app_name", "A.APP_NAME", a.Params, conds, args)
		conds, args = AddParam("release_version", "R.RELEASE_VERSION", a.Params, conds, args)
		conds, args = AddParam("pset_hash", "P.PSET_HASH", a.Params, conds, args)
		conds, args = AddParam("output_module_label", "O.OUTPUT_MODULE_LABEL", a.Params, conds, args)
		conds, args = AddParam("global_tag", "O.GLOBAL_TAG", a.Params, conds, args)
	} else {
		tmpl["Main"] = false
	}
	stm, err := LoadTemplateSQL("outputconfigs", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "fail to load outputconfigs sql template", "dbs.outputconfigs.OutputConfigs")
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query output config", "dbs.outputconfigs.OutputConfigs")
	}
	return nil
}

// OutputConfigs represents Output Configs DBS DB table
type OutputConfigs struct {
	OUTPUT_MOD_CONFIG_ID  int64  `json:"output_mod_config_id"`
	APP_EXEC_ID           int64  `json:"app_exec_id" validate:"required,number,gt=0"`
	RELEASE_VERSION_ID    int64  `json:"release_version_id" validate:"required,number,gt=0"`
	PARAMETER_SET_HASH_ID int64  `json:"parameter_set_hash_id" validate:"required,number,gt=0"`
	OUTPUT_MODULE_LABEL   string `json:"output_module_label" validate:"required"`
	GLOBAL_TAG            string `json:"global_tag" validate:"required"`
	SCENARIO              string `json:"scenario"`
	CREATION_DATE         int64  `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY             string `json:"create_by" validate:"required"`
}

// Insert implementation of OutputConfigs
func (r *OutputConfigs) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.OUTPUT_MOD_CONFIG_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id")
			r.OUTPUT_MOD_CONFIG_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_OMC")
			r.OUTPUT_MOD_CONFIG_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "unable to increment output mod config sequence number", "dbs.outputconfigs.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "fail to validate output config record", "dbs.outputconfigs.Insert")
	}

	// check if our data already exist in DB
	var vals []interface{}
	vals = append(vals, r.APP_EXEC_ID)
	vals = append(vals, r.PARAMETER_SET_HASH_ID)
	vals = append(vals, r.RELEASE_VERSION_ID)
	vals = append(vals, r.OUTPUT_MODULE_LABEL)
	vals = append(vals, r.GLOBAL_TAG)
	args := []string{
		"app_exec_id",
		"parameter_set_hash_id",
		"release_version_id",
		"output_module_label",
		"global_tag"}
	if IfExistMulti(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id", args, vals...) {
		return nil
	}

	// get SQL statement from static area
	stm := getSQL("insert_outputconfigs")
	if utils.VERBOSE > 0 {
		log.Printf("Insert OutputConfigs\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.OUTPUT_MOD_CONFIG_ID,
		r.APP_EXEC_ID,
		r.RELEASE_VERSION_ID,
		r.PARAMETER_SET_HASH_ID,
		r.OUTPUT_MODULE_LABEL,
		r.GLOBAL_TAG,
		r.SCENARIO,
		r.CREATION_DATE,
		r.CREATE_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert into OutputConfigs, error", err)
		}
		return Error(err, InsertOutputConfigErrorCode, "unable to insert output config record", "dbs.outputconfigs.Insert")
	}
	return nil
}

// Validate implementation of OutputConfigs
func (r *OutputConfigs) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.outputconfigs.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for OutputConfigs
func (r *OutputConfigs) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
}

// Decode implementation for OutputConfigs
func (r *OutputConfigs) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read output config record", "dbs.outputconfigs.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode output config record", "dbs.outputconfigs.Decode")
	}
	return nil
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

// InsertOutputConfigsTx DBS API
//
//gocyclo:ignore
func (a *API) InsertOutputConfigsTx(tx *sql.Tx) error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read output config record", "dbs.outputconfigs.InsertOutputConfigs")
	}
	rec := OutputConfigRecord{CREATE_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode output config record", "dbs.outputconfigs.InsertOutputConfigs")
	}

	// check if our data already exist in DB
	var vals []interface{}
	vals = append(vals, rec.APP_NAME)
	vals = append(vals, rec.PSET_HASH)
	vals = append(vals, rec.RELEASE_VERSION)
	vals = append(vals, rec.OUTPUT_MODULE_LABEL)
	vals = append(vals, rec.GLOBAL_TAG)
	args := []string{
		"app_name",
		"pset_hash",
		"release_version",
		"output_module_label",
		"global_tag"}
	if IfExistMulti(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id", args, vals...) {
		return nil
	}

	if rec.CREATION_DATE == 0 {
		rec.CREATION_DATE = time.Now().Unix()
	}
	if rec.CREATE_BY == "" {
		rec.CREATE_BY = a.CreateBy
	}
	arec := ApplicationExecutables{APP_NAME: rec.APP_NAME}
	rrec := ReleaseVersions{RELEASE_VERSION: rec.RELEASE_VERSION}
	prec := ParameterSetHashes{PSET_HASH: rec.PSET_HASH, PSET_NAME: rec.PSET_NAME}
	orec := OutputConfigs{
		GLOBAL_TAG:          rec.GLOBAL_TAG,
		OUTPUT_MODULE_LABEL: rec.OUTPUT_MODULE_LABEL,
		CREATION_DATE:       rec.CREATION_DATE,
		CREATE_BY:           rec.CREATE_BY,
		SCENARIO:            rec.SCENARIO}

	// get and insert (if necessary) records IDs
	var appID, psetID, relID int64
	appID, err = GetRecID(
		tx,
		&arec,
		"APPLICATION_EXECUTABLES",
		"app_exec_id",
		"app_name",
		arec.APP_NAME)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to find app_exec_id", err, "will insert")
		}
		err = arec.Insert(tx)
		if err != nil {
			return Error(err, InsertApplicationExecutableErrorCode, "unable to insert application executable record", "dbs.outputconfigs.InsertOutputConfigs")
		}
	}
	psetID, err = GetRecID(
		tx,
		&prec,
		"PARAMETER_SET_HASHES",
		"parameter_set_hash_id",
		"pset_hash",
		prec.PSET_HASH)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to find parameter_set_hash_id", err)
		}
		err = prec.Insert(tx)
		if err != nil {
			return Error(err, InsertParameterSetHashErrorCode, "unable to insert parameter set hash record", "dbs.outputconfigs.InsertOutputConfigs")
		}
	}
	relID, err = GetRecID(
		tx,
		&rrec,
		"RELEASE_VERSIONS",
		"release_version_id",
		"release_version",
		rrec.RELEASE_VERSION)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to find release_version_id", err)
		}
		err = rrec.Insert(tx)
		if err != nil {
			return Error(err, InsertReleaseVersionErrorCode, "unable to insert release version record", "dbs.outputconfigs.InsertOutputConfigs")
		}
	}

	// init all foreign Id's in output config record
	orec.APP_EXEC_ID = appID
	orec.RELEASE_VERSION_ID = relID
	orec.PARAMETER_SET_HASH_ID = psetID
	err = orec.Insert(tx)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert OutputConfigs record, error", err)
		}
		return Error(err, InsertOutputConfigErrorCode, "unable to insert output config record", "dbs.outputconfigs.InsertOutputConfigs")
	}
	return nil
}

// InsertOutputConfigs DBS API
func (a *API) InsertOutputConfigs() error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "transaction error", "dbs.outputconfigs.InsertOutputConfigs")
	}
	defer tx.Rollback()

	err = a.InsertOutputConfigsTx(tx)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert output configs", err)
		}
		return Error(err, InsertOutputConfigErrorCode, "unable to insert output config record", "dbs.outputconfigs.InsertOutputConfigs")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return Error(err, InsertOutputConfigErrorCode, "unable to commit insert of output config record", "dbs.outputconfigs.InsertOutputConfigs")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
