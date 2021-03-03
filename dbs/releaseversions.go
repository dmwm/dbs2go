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
	"unsafe"

	"github.com/vkuznet/dbs2go/utils"
)

// ReleaseVersions DBS API
func (API) ReleaseVersions(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	releaseversions := getValues(params, "release_version")
	if len(releaseversions) > 1 {
		msg := "The releaseversions API does not support list of releaseversions"
		return 0, errors.New(msg)
	} else if len(releaseversions) == 1 {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("releaseversions")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertReleaseVersions DBS API
func (API) InsertReleaseVersions(values Record) error {
	params := []string{"release_version"}
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

	// get last inserted id
	pid, err := LastInsertId(tx, "RELEASE_VERSIONS", "release_version_id")
	if err != nil {
		return err
	}
	values["release_version_id"] = pid + 1
	res := InsertValuesTxt(tx, "insert_release_versions", values)

	// commit transaction
	err = tx.Commit()
	if err != nil {
		return err
	}
	return res
}

// ReleaseVersions
type ReleaseVersions struct {
	RELEASE_VERSION_ID int64  `json:"release_version_id"`
	RELEASE_VERSION    string `json:"release_version"`
}

// Insert implementation of ReleaseVersions
func (r *ReleaseVersions) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.RELEASE_VERSION_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "RELEASE_VERSIONS", "release_version_id")
			r.RELEASE_VERSION_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_RV")
			r.RELEASE_VERSION_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_relase_versions")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_relase_versions_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert ReleaseVersions\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.RELEASE_VERSION_ID, r.RELEASE_VERSION)
	return err
}

// Validate implementation of ReleaseVersions
func (r *ReleaseVersions) Validate() error {
	if r.RELEASE_VERSION == "" {
		return errors.New("missing release_version")
	}
	return nil
}

// Decode implementation for ReleaseVersions
func (r *ReleaseVersions) Decode(reader io.Reader) error {
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

// Size implementation for ReleaseVersions
func (r *ReleaseVersions) Size() int64 {
	size := int64(unsafe.Sizeof(*r))
	size += int64(len(r.RELEASE_VERSION))
	return size
}
