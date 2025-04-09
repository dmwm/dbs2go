package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// ProcessedDatasets DBS API
func (a *API) ProcessedDatasets() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("processed_datasets")

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query processed dataset", "dbs.processeddatasets.ProcessedDatasets")
	}
	return nil
}

// ProcessedDatasets represents Processed Datasets DBS DB table
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
			tid, err = IncrementSequence(tx, "SEQ_PSDS")
			r.PROCESSED_DS_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "unable to increment processed dataset sequence number", "dbs.processeddatasets.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "fail to validate processed dataset record", "dbs.processeddatasets.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_processed_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ProcessedDatasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PROCESSED_DS_ID, r.PROCESSED_DS_NAME)
	if err != nil {
		return Error(err, InsertProcessedDatasetErrorCode, "unable to insert processed dataset record", "dbs.processeddatasets.Insert")
	}
	return nil
}

// Validate implementation of ProcessedDatasets
func (r *ProcessedDatasets) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("processed_ds_name", r.PROCESSED_DS_NAME); err != nil {
		return Error(err, InvalidParameterErrorCode, "fail to validate processed dataset name", "dbs.processeddatasets.Validate")
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
		return Error(err, ReaderErrorCode, "unable to read processed dataset record", "dbs.processeddatasets.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode processed dataset record", "dbs.processeddatasets.Decode")
	}
	return nil
}

// InsertProcessedDatasets DBS API
func (a *API) InsertProcessedDatasets() error {
	err := insertRecord(&ProcessedDatasets{}, a.Reader)
	if err != nil {
		return Error(err, InsertProcessedDatasetErrorCode, "unable to insert processed dataset record", "dbs.processeddatasets.InsertProcessedDatasets")
	}
	return nil
}
