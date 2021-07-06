package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

/*
DBS Migration APIs, see Python counterpart here:
Server/Python/src/dbs/web/DBSMigrateModel.py
Server/Python/src/dbs/business/DBSMigrate.py
and various bisuness dao, e.g.
Server/Python/src/dbs/dao/Oracle/MigrationBlock

Submit should submit migration request
(see insertMigrationRequest python API)

Status checks migration request
(see listMigrationRequests python API)

Remove removes migration request
(see removeMigrationRequest API)
*/

// MigrationReport represents migration report returned by the migration API
type MigrationReport struct {
	Report  string `json:"report"`
	Status  int    `json:"status"`
	Details string `json:"details"`
}

// Submit DBS API
func (API) Submit(r io.Reader, cby string) error {
	// read given input
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := MigrationRequests{CREATE_BY: cby, LAST_MODIFIED_BY: cby}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}

	// set migration record
	mrec := MigrationRequests{MIGRATION_URL: rec.MIGRATION_URL, MIGRATION_INPUT: rec.MIGRATION_INPUT, MIGRATION_STATUS: rec.MIGRATION_STATUS, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY, LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE, LAST_MODIFIED_BY: rec.LAST_MODIFIED_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	err = mrec.Insert(tx)
	if err != nil {
		return err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	return err
}

// Remove DBS API
func (API) Remove(r io.Reader, cby string) error {
	return nil
}

// Status DBS API
func (API) Status(params Record, w http.ResponseWriter) (int64, error) {
	return 0, nil
}
