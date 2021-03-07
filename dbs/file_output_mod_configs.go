package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
)

// FileOutputModConfigs DBS API
func (API) FileOutputModConfigs(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}

	// get SQL statement from static area
	stm := getSQL("file_output_mod_configs")

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// FileOutputModConfigs
type FileOutputModConfigs struct {
	FILE_OUTPUT_CONFIG_ID int64 `json:"file_output_config_id"`
	FILE_ID               int64 `json:"file_id"`
	OUTPUT_MOD_CONFIG_ID  int64 `json:"output_mod_config_id"`
}

// Insert implementation of FileOutputModConfigs
func (r *FileOutputModConfigs) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.FILE_OUTPUT_CONFIG_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "FILE_OUTPUT_MOD_CONFIGS", "file_output_config_id")
			r.FILE_OUTPUT_CONFIG_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FC")
			r.FILE_OUTPUT_CONFIG_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_file_output_mod_configs")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_file_output_mod_configs_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileOutputModConfigs\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.FILE_OUTPUT_CONFIG_ID, r.FILE_ID, r.OUTPUT_MOD_CONFIG_ID)
	return err
}

// Validate implementation of FileOutputModConfigs
func (r *FileOutputModConfigs) Validate() error {
	return nil
}

// Decode implementation for FileOutputModConfigs
func (r *FileOutputModConfigs) Decode(reader io.Reader) (int64, error) {
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

// InsertFileOutputModConfigs DBS API
func (API) InsertFileOutputModConfigs(r io.Reader) (int64, error) {
	return insertRecord(&FileOutputModConfigs{}, r)
}
