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

// InsertPrimaryDatasets DBS API
func (api API) InsertPrimaryDatasets(values Record) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSPrimaryDataset.py
	// intput values: primary_ds_name, primary_ds_type, creation_date, create_by
	// insert primary_ds_type and get primary_ds_type_id
	// businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
	// insert primary_ds_name, creation_date, create_by, primary_ds_id

	params := []string{"primary_ds_name", "primary_ds_type", "creation_date", "create_by"}
	if err := checkParams(values, params); err != nil {
		return err
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// if primary_ds_id is not given we will insert primary ds type first
	if _, ok := values["primary_ds_type_id"]; !ok {
		rec := make(Record)
		rec["primary_ds_type"] = values["primary_ds_type"]
		api.InsertPrimaryDSTypes(rec)
		pid, err := LastInsertId(tx, "primary_ds_types", "primary_ds_type_id")
		if err != nil {
			tx.Rollback()
			return err
		}
		values["primary_ds_type_id"] = pid + 1
	}
	delete(values, "primary_ds_type")
	if _, ok := values["primary_ds_id"]; !ok {
		sid, err := IncrementSequence(tx, "SEQ_PDS")
		if err != nil {
			tx.Rollback()
			return err
		}
		values["primary_ds_id"] = sid + 1
	}
	res := InsertValuesTxt(tx, "insert_primary_datasets", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return res
}

// PrimaryDatasets
type PrimaryDatasets struct {
	PRIMARY_DS_ID      int64  `json:"primary_ds_id"`
	PRIMARY_DS_NAME    string `json:"primary_ds_name"`
	PRIMARY_DS_TYPE_ID int64  `json:"primary_ds_type_id"`
	CREATION_DATE      int64  `json:"creation_date"`
	CREATE_BY          string `json:"create_by"`
}

// Insert implementation of PrimaryDatasets
func (r *PrimaryDatasets) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PRIMARY_DS_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "PRIMARY_DATASETS", "primary_ds_id")
			r.PRIMARY_DS_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PDS")
			r.PRIMARY_DS_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_primarydatastes")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_primarydatasets_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert PrimaryDatasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PRIMARY_DS_ID, r.PRIMARY_DS_NAME, r.PRIMARY_DS_TYPE_ID, r.CREATION_DATE, r.CREATE_BY)
	return err
}

// Validate implementation of PrimaryDatasets
func (r *PrimaryDatasets) Validate() error {
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	if r.PRIMARY_DS_NAME == "" {
		return errors.New("missing primary_ds_name")
	}
	if r.CREATION_DATE == 0 {
		return errors.New("missing creation_date")
	}
	if r.CREATE_BY == "" {
		return errors.New("missing create_by")
	}
	return nil
}

// Decode implementation for PrimaryDatasets
func (r *PrimaryDatasets) Decode(reader io.Reader) (int64, error) {
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

// PrimaryDatasetRecord
type PrimaryDatasetRecord struct {
	PRIMARY_DS_NAME string `json:"primary_ds_name"`
	PRIMARY_DS_TYPE string `json:"primary_ds_type"`
	CREATION_DATE   int64  `json:"creation_date"`
	CREATE_BY       string `json:"create_by"`
}

// PostPrimaryDatasets DBS API
func (API) PostPrimaryDatasets(r io.Reader) (int64, error) {
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
		return 0, err
	}
	size := int64(len(data))
	var rec PrimaryDatasetRecord
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return 0, err
	}
	trec := PrimaryDSTypes{PRIMARY_DS_TYPE: rec.PRIMARY_DS_TYPE}
	prec := PrimaryDatasets{PRIMARY_DS_NAME: rec.PRIMARY_DS_NAME, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()
	err = trec.Insert(tx)
	if err != nil {
		return 0, err
	}

	// init all foreign Id's in output config record
	prec.PRIMARY_DS_TYPE_ID = trec.PRIMARY_DS_TYPE_ID
	err = prec.Insert(tx)
	if err != nil {
		return 0, err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("faile to insert_outputconfigs_sqlite", err)
		return 0, err
	}
	return size, err
}
