package dbs

import (
	"io"
	"log"
	"net/http"
)

// DatasetList DBS API
func (api API) DatasetList(params Record, sep string, w http.ResponseWriter) error {
	// perform some data preprocessing on given record
	log.Printf("DatasetList data %+v", params)
	return api.Datasets(params, sep, w)
}

// InsertDatasetList DBS API
func (API) InsertDatasetList(r io.Reader, cby string) error {
	//     return InsertValues("insert_dataset_list", values)
	return nil
}
