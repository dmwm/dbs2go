package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// ReleaseVersions DBS API
func (API) ReleaseVersions(params Record, w http.ResponseWriter) error {
	var args []interface{}
	var conds []string

	// parse dataset argument
	releaseversions := getValues(params, "release_version")
	if len(releaseversions) > 1 {
		msg := "The releaseversions API does not support list of releaseversions"
		return errors.New(msg)
	} else if len(releaseversions) == 1 {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("releaseversions")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// ReleaseVersions
type ReleaseVersions struct {
	RELEASE_VERSION_ID int64  `json:"release_version_id"`
	RELEASE_VERSION    string `json:"release_version" validate:"required"`
}

// Insert implementation of ReleaseVersions
func (r *ReleaseVersions) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.RELEASE_VERSION_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "RELEASE_VERSIONS", "release_version_id")
			r.RELEASE_VERSION_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_RV")
			r.RELEASE_VERSION_ID = tid
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
	stm := getSQL("insert_release_versions")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ReleaseVersions\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.RELEASE_VERSION_ID, r.RELEASE_VERSION)
	return err
}

// Validate implementation of ReleaseVersions
func (r *ReleaseVersions) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for ReleaseVersions
func (r *ReleaseVersions) SetDefaults() {
}

// Decode implementation for ReleaseVersions
func (r *ReleaseVersions) Decode(reader io.Reader) error {
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

// InsertReleaseVersions DBS API
func (API) InsertReleaseVersions(r io.Reader, cby string) error {
	return insertRecord(&ReleaseVersions{}, r)
}
