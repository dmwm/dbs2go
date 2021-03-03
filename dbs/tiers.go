package dbs

import (
	"database/sql"
	"io"
	"net/http"
	"strings"
)

// DataTiers DBS API
func (API) DataTiers(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("data_tier_name", "DT.DATA_TIER_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("tiers")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertDataTiers DBS API
func (API) InsertDataTiers(values Record) error {
	// implement the following
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSDataTier.py
	// input values: data_tier_name, creation_date, create_by

	params := []string{"data_tier_name", "creation_date", "create_by"}
	if err := checkParams(values, params); err != nil {
		return err
	}
	if v, ok := values["data_tier_name"]; ok {
		values["data_tier_name"] = strings.ToUpper(v.(string))
	}
	err := insertWithId("SEQ_DT", "data_tier_id", "insert_tiers", values)
	return err
}

// DataTiers
type DataTiers struct {
	DATA_TIER_ID   int64  `json:"primary_ds_type_id"`
	DATA_TIER_NAME string `json:"primary_ds_type"`
	CREATION_DATE  int64  `json:"creation_date"`
	CREATE_BY      string `json:"create_by"`
}

// Insert implementation of DataTiers
func (r DataTiers) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DATA_TIER_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "DATA_TIERS", "data_tier_id")
			r.DATA_TIER_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DT")
			r.DATA_TIER_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_tiers")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_tiers_sqlite")
	}
	_, err = tx.Exec(stm, r.DATA_TIER_ID, r.DATA_TIER_NAME, r.CREATION_DATE, r.CREATE_BY)
	return err
}

// PostDataTiers DBS API
func (API) PostDataTiers(r io.Reader) (int64, error) {
	return insertRecord(DataTiers{}, r)
}
