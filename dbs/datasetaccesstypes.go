package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// DatasetAccessTypes DBS API
func (API) DatasetAccessTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("dataset_access_type", "DT.DATASET_ACCESS_TYPE", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datasetaccesstypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// DatasetAccessTypes
type DatasetAccessTypes struct {
	DATASET_ACCESS_TYPE_ID int64  `json:"dataset_access_type_id"`
	DATASET_ACCESS_TYPE    string `json:"dataset_access_type" validate:"required"`
}

// Insert implementation of DatasetAccessTypes
func (r *DatasetAccessTypes) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DATASET_ACCESS_TYPE_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "DATASET_ACCESS_TYPES", "dataset_access_type_id")
			r.DATASET_ACCESS_TYPE_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DAT")
			r.DATASET_ACCESS_TYPE_ID = tid
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
	stm := getSQL("insert_dataset_access_types")
	if utils.VERBOSE > 0 {
		log.Printf("Insert DatasetAccessTypes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATASET_ACCESS_TYPE_ID, r.DATASET_ACCESS_TYPE)
	return err
}

// Validate implementation of DatasetAccessTypes
func (r *DatasetAccessTypes) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for DatasetAccessTypes
func (r *DatasetAccessTypes) SetDefaults() {
}

// Decode implementation for DatasetAccessTypes
func (r *DatasetAccessTypes) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := ioutil.ReadAll(reader)
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

// InsertDatasetAccessTypes DBS API
func (API) InsertDatasetAccessTypes(r io.Reader, cby string) error {
	return insertRecord(&DatasetAccessTypes{}, r)
}
