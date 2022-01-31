package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// PrimaryDatasets DBS API
func (a *API) PrimaryDatasets() error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("primary_ds_name", "P.PRIMARY_DS_NAME", a.Params, conds, args)
	conds, args = AddParam("primary_ds_type", "PT.PRIMARY_DS_TYPE", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("primarydatasets")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.primarydatasets.PrimaryDataset")
	}
	return nil
}

// PrimaryDatasets represents Primary Datasets DBS DB table
type PrimaryDatasets struct {
	PRIMARY_DS_ID      int64  `json:"primary_ds_id"`
	PRIMARY_DS_NAME    string `json:"primary_ds_name" validate:"required"`
	PRIMARY_DS_TYPE_ID int64  `json:"primary_ds_type_id" validate:"required,number,gt=0"`
	CREATION_DATE      int64  `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY          string `json:"create_by" validate:"required"`
}

// Insert implementation of PrimaryDatasets
func (r *PrimaryDatasets) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PRIMARY_DS_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "PRIMARY_DATASETS", "primary_ds_id")
			r.PRIMARY_DS_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PDS")
			r.PRIMARY_DS_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "", "dbs.primarydatasets.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.primarydatasets.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_primary_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("Insert PrimaryDatasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.PRIMARY_DS_ID,
		r.PRIMARY_DS_NAME,
		r.PRIMARY_DS_TYPE_ID,
		r.CREATION_DATE,
		r.CREATE_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unablt to insert PrimaryDatasets", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.primarydatasets.Insert")
	}
	return nil
}

// Validate implementation of PrimaryDatasets
func (r *PrimaryDatasets) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.primarydatasets.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for PrimaryDatasets
func (r *PrimaryDatasets) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
}

// Decode implementation for PrimaryDatasets
func (r *PrimaryDatasets) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.primarydatasets.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.primarydatasets.Decode")
	}
	return nil
}

// PrimaryDatasetRecord represents primary dataset record
type PrimaryDatasetRecord struct {
	PRIMARY_DS_NAME string `json:"primary_ds_name" validate:"required"`
	PRIMARY_DS_TYPE string `json:"primary_ds_type" validate:"required"`
	CREATION_DATE   int64  `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY       string `json:"create_by" validate:"required"`
}

// InsertPrimaryDatasets DBS API
func (a *API) InsertPrimaryDatasets() error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSPrimaryDataset.py
	// intput values: primary_ds_name, primary_ds_type, creation_date, create_by
	// insert primary_ds_type and get primary_ds_type_id
	// businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
	// insert primary_ds_name, creation_date, create_by, primary_ds_id

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.primarydatasets.InsertPrimaryDatasets")
	}
	rec := PrimaryDatasetRecord{CREATE_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.primarydatasets.InsertPrimaryDatasets")
	}
	pdst := rec.PRIMARY_DS_TYPE
	pdsname := rec.PRIMARY_DS_NAME
	trec := PrimaryDSTypes{PRIMARY_DS_TYPE: pdst}
	prec := PrimaryDatasets{
		PRIMARY_DS_NAME: pdsname,
		CREATION_DATE:   rec.CREATION_DATE,
		CREATE_BY:       rec.CREATE_BY,
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "", "dbs.primarydatasets.InsertPrimaryDatasets")
	}
	defer tx.Rollback()

	// check if our data already exist in DB
	if IfExist(tx, "PRIMARY_DATASETS", "primary_ds_id", "primary_ds_name", pdsname) {
		if a.Writer != nil {
			a.Writer.Write([]byte(`[]`))
		}
		return nil
	}

	// check if PrimaryDSType exists in DB
	pdstID, err := GetID(tx, "PRIMARY_DS_TYPES", "primary_ds_type_id", "primary_ds_type", pdst)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to look-up primary_ds_type_id for", pdst, "error", err, "will insert...")
		}
		// insert PrimaryDSType record
		err = trec.Insert(tx)
		if err != nil {
			return Error(err, InsertErrorCode, "", "dbs.primarydatasets.InsertPrimaryDatasets")
		}
		pdstID = trec.PRIMARY_DS_TYPE_ID
	}

	// init all foreign Id's in output config record
	prec.PRIMARY_DS_TYPE_ID = pdstID
	err = prec.Insert(tx)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.primarydatasets.InsertPrimaryDatasets")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to insert primarydatasets", err)
		return Error(err, CommitErrorCode, "", "dbs.primarydatasets.InsertPrimaryDatasets")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
