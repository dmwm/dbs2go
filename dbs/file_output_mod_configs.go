package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// FileOutputModConfigs DBS API
func (a *API) FileOutputModConfigs() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("file_output_mod_configs")

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query file output mod config", "dbs.file_output_mod_configs.FileOutputModConfigs")
	}
	return nil
}

// FileOutputModConfigs represents file output mod config DBS DB table
type FileOutputModConfigs struct {
	FILE_OUTPUT_CONFIG_ID int64 `json:"file_output_config_id"`
	FILE_ID               int64 `json:"file_id" validate:"required,number,gt=0"`
	OUTPUT_MOD_CONFIG_ID  int64 `json:"output_mod_config_id" validate:"required,number,gt=0"`
}

// Insert implementation of FileOutputModConfigs
func (r *FileOutputModConfigs) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.FILE_OUTPUT_CONFIG_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "FILE_OUTPUT_MOD_CONFIGS", "file_output_config_id")
			r.FILE_OUTPUT_CONFIG_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FC")
			r.FILE_OUTPUT_CONFIG_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "unable to increment file output mod config sequence number", "dbs.file_output_mod_configs.Insert")
		}
	}
	// check if record already exists in DB
	if IfExist(tx, "FILE_OUTPUT_MOD_CONFIGS", "file_output_config_id", "file_id", r.FILE_ID) {
		if utils.VERBOSE > 1 {
			log.Printf("skip %d as it already exists in DB", r.FILE_ID)
		}
		return nil
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "fail to validate file output mod config record", "dbs.file_output_mod_configs.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_file_output_mod_configs")
	if utils.VERBOSE > 1 {
		log.Printf("Insert FileOutputModConfigs\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.FILE_OUTPUT_CONFIG_ID, r.FILE_ID, r.OUTPUT_MOD_CONFIG_ID)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("fail to insert file_output_config record", err)
		}
		return Error(err, InsertFileOutputModConfigErrorCode, "unable to insert file output mod config record", "dbs.file_output_mod_configs.Insert")
	}
	return nil
}

// Validate implementation of FileOutputModConfigs
func (r *FileOutputModConfigs) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for FileOutputModConfigs
func (r *FileOutputModConfigs) SetDefaults() {
}

// Decode implementation for FileOutputModConfigs
func (r *FileOutputModConfigs) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read file output mod config record", "dbs.file_output_mod_configs.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode file output mod config record", "dbs.file_output_mod_configs.Decode")
	}
	return nil
}

// FileOutputModConfigRecord represents file output mod config input record
type FileOutputModConfigRecord struct {
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	Lfn               string `json:"lfn"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
	GlobalTag         string `json:"global_tag"`
}

// InsertFileOutputModConfigs DBS API
func (a *API) InsertFileOutputModConfigs(tx *sql.Tx) error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(
			err,
			ReaderErrorCode,
			"unable to read file output mod config record",
			"dbs.file_output_mod_configs.InsertFileOutputModConfigs")
	}
	var rec FileOutputModConfigRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(
			err,
			UnmarshalErrorCode,
			"unable to decode file output mod config record",
			"dbs.file_output_mod_configs.InsertFileOutputModConfigs")
	}

	// get file id for given lfn
	fid, err := GetID(tx, "FILES", "file_id", "logical_file_name", rec.Lfn)
	if err != nil {
		msg := fmt.Sprintf("unable to find file_id for %s", rec.Lfn)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(
			err,
			GetFileIDErrorCode,
			msg,
			"dbs.file_output_mod_configs.InsertFileOutputModConfigs")
	}
	// find output module config id
	var args []interface{}
	var conds []string
	a.Params["logical_file_name"] = rec.Lfn
	a.Params["app_name"] = rec.AppName
	a.Params["pset_hash"] = rec.PsetHash
	a.Params["output_module_label"] = rec.OutputModuleLabel
	a.Params["global_tag"] = rec.GlobalTag
	conds, args = AddParam("app_name", "A.APP_NAME", a.Params, conds, args)
	conds, args = AddParam("pset_hash", "P.PSET_HASH", a.Params, conds, args)
	conds, args = AddParam("output_module_label", "O.OUTPUT_MODULE_LABEL", a.Params, conds, args)
	conds, args = AddParam("global_tag", "O.GLOBAL_TAG", a.Params, conds, args)
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("outputconfigs_id", tmpl)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to load outputconfigs_id sql template, error", err)
		}
		return Error(err, LoadErrorCode, "fail to load outputconfigs_id template", "dbs.file_output_mod_configs.InsertFileOutputModConfigs")
	}
	stm = WhereClause(stm, conds)
	var oid int64
	err = tx.QueryRow(stm, args...).Scan(&oid)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to find output_mod_config_id for\n%s\n%+v", stm, args)
		}
		return Error(err, QueryErrorCode, "unable to query output mod config", "dbs.file_output_mod_configs.InsertFileOutputModConfigs")
	}

	// init all foreign Id's in output config record
	var rrr FileOutputModConfigs
	rrr.FILE_ID = fid
	rrr.OUTPUT_MOD_CONFIG_ID = oid
	if utils.VERBOSE > 1 {
		log.Printf("Insert FileOutputModConfigs\n%s\n%+v", stm, rrr)
	}
	err = rrr.Insert(tx)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert FileOutputModConfigs, error", err)
		}
		return Error(err, InsertFileOutputModConfigErrorCode, "unable to insert file output mod config record", "dbs.file_output_mod_configs.InsertFileOutputModConfigs")
	}

	return nil
}
