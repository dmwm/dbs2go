package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/vkuznet/dbs2go/utils"
)

// AcquisitionEras DBS API
func (a API) AcquisitionEras() error {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
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
			tid, err = LastInsertID(tx, "ACQUISITION_ERAS", "acquisition_era_id")
			r.ACQUISITION_ERA_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_AQE")
			r.ACQUISITION_ERA_ID = tid
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
	stm := getSQL("insert_acquisition_eras")
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
func (r *AcquisitionEras) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
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

// InsertAcquisitionEras DBS API
func (a API) InsertAcquisitionEras() error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSAcquisitionEra.py
	// input values: acquisition_era_name, creation_date, start_date, end_date, create_by
	// businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)

	//     return InsertValues("insert_acquisition_eras", values)
	return insertRecord(&AcquisitionEras{CREATE_BY: a.CreateBy}, a.Reader)
}

// UpdateAcquisitionEras DBS API
func (a API) UpdateAcquisitionEras() error {

	var aera string
	var endDate int
	if v, ok := a.Params["end_date"]; ok {
		val, err := strconv.Atoi(v.(string))
		if err != nil {
			log.Println("invalid input parameter", err)
		}
		endDate = val
	}
	if v, ok := a.Params["acquisition_era_name"]; ok {
		aera = v.(string)
	}

	// validate input params
	if endDate == 0 {
		return errors.New("invalid end_date parameter")
	}
	if aera == "" {
		return errors.New("invalid end_date parameter")
	}

	// get SQL statement from static area
	stm := getSQL("update_acquisition_eras")
	if utils.VERBOSE > 0 {
		log.Printf("update AcquisitionEras\n%s\n%+v", stm, a.Params)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(stm, endDate, aera)
	if err != nil {
		log.Printf("unable to update %v", err)
		return err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return err
	}
	return err
}
