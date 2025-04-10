package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/dmwm/dbs2go/utils"
)

// AcquisitionEras DBS API
func (a *API) AcquisitionEras() error {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("acquisitioneras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to execute acquisition era query", "dbs.acquisitioners.AcquisitionEras")
	}
	return nil
}

// AcquisitionEras represents Acquisition Eras DBS DB table
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

	// check if our data already exist in DB
	if IfExist(
		tx,
		"ACQUISITION_ERAS",
		"acquisition_era_id",
		"acquisition_era_name",
		r.ACQUISITION_ERA_NAME) {
		return nil
	}

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
			return Error(err, LastInsertErrorCode, "unable to increment AcquisitionEras sequence id", "dbs.acquisitioneras.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		msg := "unable to validate acquisition era record"
		log.Println(msg, err)
		return Error(err, ValidateErrorCode, msg, "dbs.acquisitioneras.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_acquisition_eras")
	if utils.VERBOSE > 0 {
		log.Printf("Insert AcquisitionEras\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.ACQUISITION_ERA_ID,
		r.ACQUISITION_ERA_NAME,
		r.START_DATE,
		r.END_DATE,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.DESCRIPTION)
	if utils.VERBOSE > 0 {
		log.Printf("unable to insert AcquisitionEras %s error %+v", stm, err)
	}
	if err != nil {
		return Error(err, InsertAcquisitionEraErrorCode, "unable to insert Acquisition Era record", "dbs.acquisitioneras.Insert")
	}
	return nil
}

// Validate implementation of AcquisitionEras
func (r *AcquisitionEras) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.acquisitioneras.Validate")
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
		return Error(err, ReaderErrorCode, "unable to read acquisition eras record", "dbs.acquisitioneras.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode acquisition eras record", "dbs.acquisitioneras.Decode")
	}
	return nil
}

// InsertAcquisitionEras DBS API
func (a *API) InsertAcquisitionEras() error {
	err := insertRecord(&AcquisitionEras{CREATE_BY: a.CreateBy}, a.Reader)
	if err != nil {
		return Error(err, InsertAcquisitionEraErrorCode, "unable to insert Acquisition Era record", "dbs.acquisitioneras.InsertAcquisitionEras")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// UpdateAcquisitionEras DBS API
func (a *API) UpdateAcquisitionEras() error {

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
		msg := "invalid end_date parameter"
		e := Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.UpdateAckquisitionEras")
		return e
	}
	if aera == "" {
		msg := "invalid ackquisition_era_name parameter"
		e := Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.UpdateAckquisitionEras")
		return e
	}

	// get SQL statement from static area
	stm := getSQL("update_acquisition_eras")
	if utils.VERBOSE > 0 {
		log.Printf("update AcquisitionEras\n%s\n%+v", stm, a.Params)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		e := Error(err, TransactionErrorCode, "transaction error", "dbs.UpdateAckquisitionEras")
		log.Println(e)
		return e
	}
	defer tx.Rollback()

	_, err = tx.Exec(stm, endDate, aera)
	if err != nil {
		e := Error(err, UpdateAcquisitionEraErrorCode, "unable to update acquisition era record", "dbs.UpdateAckquisitionEras")
		log.Println(e)
		return e
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		e := Error(err, UpdateAcquisitionEraErrorCode, "unable to commit update of acquisition era record", "dbs.UpdateAckquisitionEras")
		log.Println(e)
		return e
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
