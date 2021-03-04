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

// InsertDatasetAccessTypes DBS API
func (API) InsertDatasetAccessTypes(values Record) error {
	return InsertValues("insert_dataset_access_types", values)
}

// DatasetAccessTypes
type DatasetAccessTypes struct {
	DATASET_ACCESS_TYPE_ID int64  `json:"datset_access_type_id"`
	DATASET_ACCESS_TYPE    string `json:"dataset_access_type"`
}

// Insert implementation of DatasetAccessTypes
func (r *DatasetAccessTypes) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DATASET_ACCESS_TYPE_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "DATASET_ACCESS_TYPES", "datset_access_type_id")
			r.DATASET_ACCESS_TYPE_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DAT")
			r.DATASET_ACCESS_TYPE_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_datasetaccesstypes")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_datasetaccesstypes_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert DatasetAccessTypes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATASET_ACCESS_TYPE_ID, r.DATASET_ACCESS_TYPE)
	return err
}

// Validate implementation of DatasetAccessTypes
func (r *DatasetAccessTypes) Validate() error {
	if r.DATASET_ACCESS_TYPE == "" {
		return errors.New("missing dataset_access_type")
	}
	return nil
}

// Decode implementation for DatasetAccessTypes
func (r *DatasetAccessTypes) Decode(reader io.Reader) (int64, error) {
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
