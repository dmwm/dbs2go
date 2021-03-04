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
	"strings"
	"unsafe"

	"github.com/vkuznet/dbs2go/utils"
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
	DATA_TIER_ID   int64  `json:"data_tier_id"`
	DATA_TIER_NAME string `json:"data_tier_name"`
	CREATION_DATE  int64  `json:"creation_date"`
	CREATE_BY      string `json:"create_by"`
}

// Insert implementation of DataTiers
func (r *DataTiers) Insert(tx *sql.Tx) error {
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
	if utils.VERBOSE > 0 {
		log.Printf("Insert DataTiers\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATA_TIER_ID, r.DATA_TIER_NAME, r.CREATION_DATE, r.CREATE_BY)
	return err
}

// Validate implementation of DataTiers
func (r *DataTiers) Validate() error {
	if matched := tierPattern.MatchString(r.DATA_TIER_NAME); !matched {
		log.Println("validate DataTiers", r)
		return errors.New("invalid pattern for data tier")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	if r.DATA_TIER_NAME == "" {
		return errors.New("missing data_tier_name")
	}
	if r.CREATION_DATE == 0 {
		return errors.New("missing creation_date")
	}
	if r.CREATE_BY == "" {
		return errors.New("missing create_by")
	}
	return nil
}

// Decode implementation for DataTiers
func (r *DataTiers) Decode(reader io.Reader) (int64, error) {
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

// Size implementation for DataTiers
func (r *DataTiers) Size() int64 {
	size := int64(unsafe.Sizeof(*r))
	size += int64(len(r.DATA_TIER_NAME))
	size += int64(len(r.CREATE_BY))
	return size
}

// PostDataTiers DBS API
func (API) PostDataTiers(r io.Reader) (int64, error) {
	return insertRecord(&DataTiers{}, r)
}
