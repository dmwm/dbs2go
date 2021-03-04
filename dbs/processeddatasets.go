package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// ProcessedDatasets DBS API
func (API) ProcessedDatasets(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("processed_datasets")

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// ProcessedDatasets
type ProcessedDatasets struct {
	PROCESSED_DS_ID   int64  `json:"processed_ds_id"`
	PROCESSED_DS_NAME string `json:"processed_ds_name"`
}

// Insert implementation of ProcessedDatasets
func (r *ProcessedDatasets) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PROCESSED_DS_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "PROCESS_DATASETS", "processed_ds_id")
			r.PROCESSED_DS_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PDT")
			r.PROCESSED_DS_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_processed_datasets")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_processed_datasets_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert ProcessedDatasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PROCESSED_DS_ID, r.PROCESSED_DS_NAME)
	return err
}

// Validate implementation of ProcessedDatasets
func (r *ProcessedDatasets) Validate() error {
	if matched := procDSPattern.MatchString(r.PROCESSED_DS_NAME); !matched {
		log.Println("validate ProcessedDatasets", r)
		return errors.New("invalid pattern for data tier")
	}
	if r.PROCESSED_DS_NAME == "" {
		return errors.New("missing processed_ds_name")
	}
	return nil
}

// Decode implementation for ProcessedDatasets
func (r *ProcessedDatasets) Decode(reader io.Reader) (int64, error) {
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

// InsertProcessedDatasets DBS API
func (API) InsertProcessedDatasets(r io.Reader) (int64, error) {
	return insertRecord(&ProcessedDatasets{}, r)
}
