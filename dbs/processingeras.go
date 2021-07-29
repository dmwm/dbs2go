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

// ProcessingEras DBS API
func (API) ProcessingEras(params Record, w http.ResponseWriter) error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("processing_version", "PE.PROCESSING_VERSION", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("processingeras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// ProcessingEras
type ProcessingEras struct {
	PROCESSING_ERA_ID  int64  `json:"processing_era_id"`
	PROCESSING_VERSION int64  `json:"processing_version" validate:"required,number,gt=0"`
	CREATION_DATE      int64  `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY          string `json:"create_by validate:"required""`
	DESCRIPTION        string `json:"description"`
}

// Insert implementation of ProcessingEras
func (r *ProcessingEras) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PROCESSING_ERA_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "PROCESSING_ERAS", "processing_era_id")
			r.PROCESSING_ERA_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PE")
			r.PROCESSING_ERA_ID = tid
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
	stm := getSQL("insert_processing_eras")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ProcessingEras\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PROCESSING_ERA_ID, r.PROCESSING_VERSION, r.CREATION_DATE, r.CREATE_BY, r.DESCRIPTION)
	return err
}

// Validate implementation of ProcessingEras
func (r *ProcessingEras) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	return nil
}

// SetDefaults implements set defaults for ProcessingEras
func (r *ProcessingEras) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
}

// Decode implementation for ProcessingEras
func (r *ProcessingEras) Decode(reader io.Reader) error {
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

// InsertProcessingEras DBS API
func (API) InsertProcessingEras(r io.Reader, cby string) error {
	return insertRecord(&ProcessingEras{CREATE_BY: cby}, r)
}
