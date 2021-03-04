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
func (API) ProcessingEras(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	conds, args = AddParam("processing_version", "PE.PROCESSING_VERSION", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("processingeras")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertProcessingEras DBS API
func (API) InsertProcessingEras(values Record) error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSProcessingEra.py
	// input values: processing_version, creation_date,  create_by, description
	// businput["processing_era_id"] = self.sm.increment(conn, "SEQ_PE", tran)
	params := []string{"processing_version", "creation_date", "create_by", "description"}
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

	if _, ok := values["processing_era_id"]; !ok {
		sid, err := IncrementSequence(tx, "SEQ_PE")
		if err != nil {
			tx.Rollback()
			return err
		}
		values["processing_era_id"] = sid + 1
	}
	res := InsertValues("insert_processing_eras", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return res
}

// ProcessingEras
type ProcessingEras struct {
	PROCESSING_ERA_ID  int64  `json:"processing_era_id"`
	PROCESSING_VERSION string `json:"processing_version"`
	CREATION_DATE      int64  `json:"creation_date"`
	CREATE_BY          string `json:"create_by"`
	DESCRIPTION        string `json:"description"`
}

// Insert implementation of ProcessingEras
func (r *ProcessingEras) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PROCESSING_ERA_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "PROCESSING_ERAS", "processing_era_id")
			r.PROCESSING_ERA_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PE")
			r.PROCESSING_ERA_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_processingeras")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_processingeras_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert ProcessingEras\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PROCESSING_ERA_ID, r.PROCESSING_VERSION, r.CREATION_DATE, r.CREATE_BY, r.DESCRIPTION)
	return err
}

// Validate implementation of ProcessingEras
func (r *ProcessingEras) Validate() error {
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	if r.PROCESSING_VERSION == "" {
		return errors.New("missing processing_version")
	}
	if r.CREATION_DATE == 0 {
		return errors.New("missing creation_date")
	}
	if r.CREATE_BY == "" {
		return errors.New("missing create_by")
	}
	return nil
}

// Decode implementation for ProcessingEras
func (r *ProcessingEras) Decode(reader io.Reader) (int64, error) {
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

// PostProcessingEras DBS API
func (API) PostProcessingEras(r io.Reader) (int64, error) {
	return insertRecord(&ProcessingEras{}, r)
}
