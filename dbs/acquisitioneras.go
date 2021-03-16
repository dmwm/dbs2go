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

// AcquisitionEras DBS API
func (API) AcquisitionEras(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// UpdateAcquisitionEras DBS API
func (API) UpdateAcquisitionEras(values Record) error {
	// TODO: implement the following logic
	// input values: acquisition_era_name ="", end_date=0
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)
	return nil
}

// AcquisitionEras
type AcquisitionEras struct {
	ACQUISITION_ERA_ID   int64  `json:"acquisition_era_id"`
	ACQUISITION_ERA_NAME string `json:"acquisition_era_name" validate:"required"`
	START_DATE           int64  `json:"start_date" validate:"required,number"`
	END_DATE             int64  `json:"end_date"`
	CREATION_DATE        int64  `json:"creation_date" validate:"required,number"`
	CREATE_BY            string `json:"create_by" validate:"required"`
	DESCRIPTION          string `json:"description"`
}

// Insert implementation of AcquisitionEras
func (r *AcquisitionEras) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.ACQUISITION_ERA_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "ACQUISITION_ERAS", "acquisition_era_id")
			r.ACQUISITION_ERA_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_AQE")
			r.ACQUISITION_ERA_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_acquisition_eras")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_acquisition_eras_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert AcquisitionEras\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.ACQUISITION_ERA_ID, r.ACQUISITION_ERA_NAME, r.START_DATE, r.END_DATE, r.CREATION_DATE, r.CREATE_BY, r.DESCRIPTION)
	return err
}

// Validate implementation of AcquisitionEras
func (r *AcquisitionEras) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	return nil
}

// SetDefaults implements set defaults for AcquisitionEras
func (r *AcquisitionEras) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
	if r.START_DATE == 0 {
		r.START_DATE = Date()
	}
	if r.END_DATE == 0 {
		r.END_DATE = Date()
	}
}

// Decode implementation for AcquisitionEras
func (r *AcquisitionEras) Decode(reader io.Reader) (int64, error) {
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

// InsertAcquisitionEras DBS API
func (API) InsertAcquisitionEras(r io.Reader, cby string) (int64, error) {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSAcquisitionEra.py
	// input values: acquisition_era_name, creation_date, start_date, end_date, create_by
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)

	//     return InsertValues("insert_acquisition_eras", values)
	return insertRecord(&AcquisitionEras{CREATE_BY: cby}, r)
}
