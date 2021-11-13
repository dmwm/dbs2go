package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// DatasetOutputModConfigs DBS API
func (a *API) DatasetOutputModConfigs() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("dataset_output_mod_configs")

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// DatasetOutputModConfigs represents dataset output mod configs DBS DB table
type DatasetOutputModConfigs struct {
	DS_OUTPUT_MOD_CONF_ID int64 `json:"ds_output_mod_conf_id"`
	DATASET_ID            int64 `json:"dataset_id" validate:"required,number,gt=0"`
	OUTPUT_MOD_CONFIG_ID  int64 `json:"output_mod_config_id" validate:"required,number,gt=0"`
}

// Insert implementation of DatasetOutputModConfigs
func (r *DatasetOutputModConfigs) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DS_OUTPUT_MOD_CONF_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "DATASET_OUTPUT_MOD_CONFIGS", "ds_output_mod_conf_id")
			r.DS_OUTPUT_MOD_CONF_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DC")
			r.DS_OUTPUT_MOD_CONF_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// check if our data already exist in DB
	var vals []interface{}
	vals = append(vals, r.DATASET_ID)
	vals = append(vals, r.OUTPUT_MOD_CONFIG_ID)
	args := []string{"dataset_id", "output_mod_config_id"}
	if IfExistMulti(tx, "DATASET_OUTPUT_MOD_CONFIGS", "ds_output_mod_conf_id", args, vals...) {
		return nil
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_dataset_output_mod_configs")
	if utils.VERBOSE > 0 {
		log.Printf("Insert DatasetOutputModConfigs\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DS_OUTPUT_MOD_CONF_ID, r.DATASET_ID, r.OUTPUT_MOD_CONFIG_ID)
	if utils.VERBOSE > 0 {
		log.Printf("unable to insert DatasetOutputModConfigs %+v", err)
	}
	return err
}

// Validate implementation of DatasetOutputModConfigs
func (r *DatasetOutputModConfigs) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for DatasetOutputModConfigs
func (r *DatasetOutputModConfigs) SetDefaults() {
}

// Decode implementation for DatasetOutputModConfigs
func (r *DatasetOutputModConfigs) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	return nil
}

// InsertDatasetOutputModConfigs DBS API
func (a *API) InsertDatasetOutputModConfigs() error {
	return insertRecord(&DatasetOutputModConfigs{}, a.Reader)
}
