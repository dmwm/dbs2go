package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// ReleaseVersions DBS API
func (a *API) ReleaseVersions() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Dataset"] = false
	tmpl["Lfn"] = false

	// parse dataset argument
	releaseversions := getValues(a.Params, "release_version")
	if len(releaseversions) > 1 {
		msg := "The releaseversions API does not support list of releaseversions"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.releaseversions.ReleaseVersions")
	} else if len(releaseversions) == 1 {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", a.Params, conds, args)
	}
	if _, err := getSingleValue(a.Params, "dataset"); err == nil {
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)
	}
	if _, err := getSingleValue(a.Params, "logical_file_name"); err == nil {
		tmpl["Lfn"] = true
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("releaseversions", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "unable to load releaseversions sql template", "dbs.releaseversions.ReleaseVersions")
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query release version", "dbs.releaseversions.ReleaseVersions")
	}
	return nil
}

// ReleaseVersions represents Relases Versions DBS DB table
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
			return Error(err, LastInsertErrorCode, "unable to increment release version sequence number", "dbs.releaseversions.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "fail to validate release version record", "dbs.releaseversions.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_release_versions")
	if utils.VERBOSE > 0 {
		log.Printf("Insert ReleaseVersions\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.RELEASE_VERSION_ID, r.RELEASE_VERSION)
	if err != nil {
		return Error(err, InsertReleaseVersionErrorCode, "unable to insert release version record", "dbs.releaseversions.Insert")
	}
	return nil
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
		return Error(err, ReaderErrorCode, "unable to read release version record", "dbs.releaseversions.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode release version record", "dbs.releaseversions.Decode")
	}
	return nil
}

// InsertReleaseVersions DBS API
func (a *API) InsertReleaseVersions() error {
	err := insertRecord(&ReleaseVersions{}, a.Reader)
	if err != nil {
		return Error(err, InsertReleaseVersionErrorCode, "unable to insert release version record", "dbs.releaseversions.InsertReleaseVersions")
	}
	return nil
}
