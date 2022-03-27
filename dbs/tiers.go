package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// DataTiers DBS API
func (a *API) DataTiers() error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("data_tier_name", "DT.DATA_TIER_NAME", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("tiers")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.tiers.DataTiers")
	}
	return nil
}

// DataTiers represents data tiers DBS DB table
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
			return Error(err, LastInsertErrorCode, "", "dbs.tiers.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.tiers.Insert")
	}
	// check if our data already exist in DB
	if IfExist(tx, "DATA_TIERS", "data_tier_id", "data_tier_name", r.DATA_TIER_NAME) {
		return nil
	}

	// get SQL statement from static area
	stm := getSQL("insert_tiers")
	if utils.VERBOSE > 0 {
		log.Printf("Insert DataTiers\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATA_TIER_ID, r.DATA_TIER_NAME, r.CREATION_DATE, r.CREATE_BY)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.tiers.Insert")
	}
	return nil
}

// Validate implementation of DataTiers
func (r *DataTiers) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("data_tier_name", r.DATA_TIER_NAME); err != nil {
		return Error(err, PatternErrorCode, "", "dbs.tiers.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.tiers.Validate")
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
		return Error(err, ReaderErrorCode, "", "dbs.tiers.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.tiers.Decode")
	}
	return nil
}

// InsertDataTiers DBS API
func (a *API) InsertDataTiers() error {
	err := insertRecord(&DataTiers{CREATE_BY: a.CreateBy}, a.Reader)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.tiers.InsertDataTiers")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
