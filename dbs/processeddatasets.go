package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// ProcessedDatasets DBS API
func (a API) ProcessedDatasets() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("processed_datasets")

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// ProcessedDatasets
type ProcessedDatasets struct {
	PROCESSED_DS_ID   int64  `json:"processed_ds_id"`
	PROCESSED_DS_NAME string `json:"processed_ds_name" validate:"required"`
}

// Insert implementation of ProcessedDatasets
func (r *ProcessedDatasets) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PROCESSED_DS_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "PROCESSED_DATASETS", "processed_ds_id")
			r.PROCESSED_DS_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PDT")
			r.PROCESSED_DS_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_processed_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ProcessedDatasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PROCESSED_DS_ID, r.PROCESSED_DS_NAME)
	return err
}

// Validate implementation of ProcessedDatasets
func (r *ProcessedDatasets) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("processed_ds_name", r.PROCESSED_DS_NAME); err != nil {
		return err
	}
	return nil
}

// SetDefaults implements set defaults for ProcessedDatasets
func (r *ProcessedDatasets) SetDefaults() {
}

// Decode implementation for ProcessedDatasets
func (r *ProcessedDatasets) Decode(reader io.Reader) error {
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

// InsertProcessedDatasets DBS API
func (a API) InsertProcessedDatasets() error {
	return insertRecord(&ProcessedDatasets{}, a.Reader)
}
