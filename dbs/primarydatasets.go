package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// PrimaryDatasets DBS API
func (API) PrimaryDatasets(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("primary_ds_name", "P.PRIMARY_DS_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("primarydatasets")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// PrimaryDatasets
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
	stm := getSQL("insert_primary_datasets")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_primary_datasets_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert PrimaryDatasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PRIMARY_DS_ID, r.PRIMARY_DS_NAME, r.PRIMARY_DS_TYPE_ID, r.CREATION_DATE, r.CREATE_BY)
	return err
}

// Validate implementation of PrimaryDatasets
func (r *PrimaryDatasets) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
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

// PrimaryDatasetRecord
type PrimaryDatasetRecord struct {
	PRIMARY_DS_NAME string `json:"primary_ds_name"`
	PRIMARY_DS_TYPE string `json:"primary_ds_type"`
	CREATION_DATE   int64  `json:"creation_date"`
	CREATE_BY       string `json:"create_by"`
}

// InsertPrimaryDatasets DBS API
func (API) InsertPrimaryDatasets(r io.Reader, cby string) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSPrimaryDataset.py
	// intput values: primary_ds_name, primary_ds_type, creation_date, create_by
	// insert primary_ds_type and get primary_ds_type_id
	// businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
	// insert primary_ds_name, creation_date, create_by, primary_ds_id

	// read given input
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := PrimaryDatasetRecord{CREATE_BY: cby}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	trec := PrimaryDSTypes{PRIMARY_DS_TYPE: rec.PRIMARY_DS_TYPE}
	prec := PrimaryDatasets{PRIMARY_DS_NAME: rec.PRIMARY_DS_NAME, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	err = trec.Insert(tx)
	if err != nil {
		return err
	}

	// init all foreign Id's in output config record
	prec.PRIMARY_DS_TYPE_ID = trec.PRIMARY_DS_TYPE_ID
	err = prec.Insert(tx)
	if err != nil {
		return err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("faile to insert_outputconfigs_sqlite", err)
		return err
	}
	return err
}
