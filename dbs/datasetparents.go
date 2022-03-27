package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// DatasetParents API
func (a *API) DatasetParents() error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datasetparent")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.datasetparents.DatasetParents")
	}
	return nil
}

// DatasetParents represents Dataset Parents DBS DB table
type DatasetParents struct {
	THIS_DATASET_ID   int64 `json:"this_dataset_id" validate:"required,number,gt=0"`
	PARENT_DATASET_ID int64 `json:"parent_dataset_id" validate:"required,number,gt=0"`
}

// Insert implementation of DatasetParents
func (r *DatasetParents) Insert(tx *sql.Tx) error {
	var err error
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.datasetparents.Insert")
	}
	// check if record exists in DB
	if IfExist(tx, "DATASET_PARENTS", "this_dataset_id", "this_dataset_id", r.THIS_DATASET_ID) {
		if utils.VERBOSE > 1 {
			log.Printf("skip %v as it already exists in DB", r.THIS_DATASET_ID)
		}
		return nil
	}
	// get SQL statement from static area
	stm := getSQL("insert_dataset_parents")
	if utils.VERBOSE > 0 {
		log.Printf("Insert DatasetParents\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.THIS_DATASET_ID, r.PARENT_DATASET_ID)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert DatasetParents record, error", err)
		}
		return Error(err, QueryErrorCode, "", "dbs.datasetparents.Insert")
	}
	return nil
}

// Validate implementation of DatasetParents
func (r *DatasetParents) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if r.THIS_DATASET_ID == 0 {
		msg := "missing this_dataset_id"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.datasetparents.Validate")
	}
	if r.PARENT_DATASET_ID == 0 {
		msg := "missing parent_dataset_id"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.datasetparents.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for DatasetParents
func (r *DatasetParents) SetDefaults() {
}

// Decode implementation for DatasetParents
func (r *DatasetParents) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.datasetparents.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.datasetparents.Decode")
	}
	return nil
}
