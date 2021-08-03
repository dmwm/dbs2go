package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// DataTiers DBS API
func (API) DataTiers(params Record, sep string, w http.ResponseWriter) error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("data_tier_name", "DT.DATA_TIER_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("tiers")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, sep, stm, args...)
}

// DataTiers
type DataTiers struct {
	DATA_TIER_ID   int64  `json:"data_tier_id"`
	DATA_TIER_NAME string `json:"data_tier_name" validate:"required,uppercase"`
	CREATION_DATE  int64  `json:"creation_date" validate:"required,number"`
	CREATE_BY      string `json:"create_by" validate:"required"`
}

// Insert implementation of DataTiers
func (r *DataTiers) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DATA_TIER_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "DATA_TIERS", "data_tier_id")
			r.DATA_TIER_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DT")
			r.DATA_TIER_ID = tid
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
	stm := getSQL("insert_tiers")
	if utils.VERBOSE > 0 {
		log.Printf("Insert DataTiers\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATA_TIER_ID, r.DATA_TIER_NAME, r.CREATION_DATE, r.CREATE_BY)
	return err
}

// Validate implementation of DataTiers
func (r *DataTiers) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("data_tier_name", r.DATA_TIER_NAME); err != nil {
		return err
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for creation date")
	}
	return nil
}

// SetDefaults implements set defaults for DataTiers
func (r *DataTiers) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
}

// Decode implementation for DataTiers
func (r *DataTiers) Decode(reader io.Reader) error {
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

// InsertDataTiers DBS API
func (API) InsertDataTiers(r io.Reader, cby string) error {
	return insertRecord(&DataTiers{CREATE_BY: cby}, r)
}
