package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// DatasetAccessTypes DBS API
func (a *API) DatasetAccessTypes() error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("dataset_access_type", "DT.DATASET_ACCESS_TYPE", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datasetaccesstypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.datasetaccesstypes.DatasetAccessTypes")
	}
	return nil
}

// DatasetAccessTypes represents Dataset Access Types DBS DB table
type DatasetAccessTypes struct {
	DATASET_ACCESS_TYPE_ID int64  `json:"dataset_access_type_id"`
	DATASET_ACCESS_TYPE    string `json:"dataset_access_type" validate:"required"`
}

// Insert implementation of DatasetAccessTypes
func (r *DatasetAccessTypes) Insert(tx *sql.Tx) error {

	// check if our data already exist in DB
	if IfExist(
		tx,
		"DATASET_ACCESS_TYPES",
		"dataset_access_type_id",
		"dataset_access_type",
		r.DATASET_ACCESS_TYPE) {
		return nil
	}
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
			return Error(err, LastInsertErrorCode, "", "dbs.datasetaccesstypes.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.datasetaccesstypes.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_dataset_access_types")
	if utils.VERBOSE > 0 {
		log.Printf("Insert DatasetAccessTypes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATASET_ACCESS_TYPE_ID, r.DATASET_ACCESS_TYPE)
	if utils.VERBOSE > 0 {
		log.Printf("unable to insert DatasetAccessTypes %+v", err)
	}
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.datasetaccesstypes.Insert")
	}
	return nil
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
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.datasetaccesstypes.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.datasetaccesstypes.Insert")
	}
	return nil
}

// InsertDatasetAccessTypes DBS API
func (a *API) InsertDatasetAccessTypes() error {
	err := insertRecord(&DatasetAccessTypes{}, a.Reader)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.datasetaccesstypes.InsertDatasetAccessTypes")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
