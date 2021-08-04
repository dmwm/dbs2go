package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// FileDataTypes DBS API
func (a API) FileDataTypes() error {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("file_data_types")

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// FileDataTypes
type FileDataTypes struct {
	FILE_TYPE_ID int64  `json:"file_type_id"`
	FILE_TYPE    string `json:"file_type" validate:"required"`
}

// Insert implementation of FileDataTypes
func (r *FileDataTypes) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.FILE_TYPE_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "FILE_DATA_TYPES", "file_type_id")
			r.FILE_TYPE_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FT")
			r.FILE_TYPE_ID = tid
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
	stm := getSQL("insert_file_data_types")
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileDataTypes\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.FILE_TYPE_ID, r.FILE_TYPE)
	return err
}

// Validate implementation of FileDataTypes
func (r *FileDataTypes) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for FileDataTypes
func (r *FileDataTypes) SetDefaults() {
}

// Decode implementation for FileDataTypes
func (r *FileDataTypes) Decode(reader io.Reader) error {
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

// InsertFileDataTypes DBS API
func (a API) InsertFileDataTypes() error {
	return insertRecord(&FileDataTypes{}, a.Reader)
}
