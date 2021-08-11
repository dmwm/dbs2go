package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
)

// BlockDumpRecord represent input recor for blockdump API
type BlockDumpRecord struct {
	BLOCK_NAME string `json:"block_name"`
}

// TODO: see dumpBlock function in
// ../../Server/Python/src/dbs/business/DBSBlock.py

// BlockDump DBS API
func (a *API) BlockDump() error {

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := BlockDumpRecord{}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	return err
}
