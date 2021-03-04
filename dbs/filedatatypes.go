package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// FileDataTypes DBS API
func (API) FileDataTypes(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("file_data_types")

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileDataTypes DBS API
func (API) InsertFileDataTypes(values Record) error {
	return InsertValues("insert_file_data_types", values)
}

// FileTypes
type FileTypes struct {
	FILE_TYPE_ID int64  `json:"file_type_id"`
	FILE_TYPE    string `json:"file_type"`
}

// Insert implementation of FileTypes
func (r *FileTypes) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.FILE_TYPE_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "FILE_TYPES", "file_type_id")
			r.FILE_TYPE_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FT")
			r.FILE_TYPE_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_filetypes")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_filetypes_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileTypes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.FILE_TYPE_ID, r.FILE_TYPE)
	return err
}

// Validate implementation of FileTypes
func (r *FileTypes) Validate() error {
	if r.FILE_TYPE == "" {
		return errors.New("missing file_type")
	}
	return nil
}

// Decode implementation for FileTypes
func (r *FileTypes) Decode(reader io.Reader) (int64, error) {
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
