package dbs

import (
	"io"
	"log"
	"net/http"
)

// DatasetList DBS API
func (api API) DatasetList(params Record, w http.ResponseWriter) (int64, error) {
	// perform some data preprocessing on given record
	log.Printf("DatasetList data %+v", params)
	return api.Datasets(params, w)
}

// InsertDatasetList DBS API
func (API) InsertDatasetList(r io.Reader, cby string) (int64, error) {
	//     return InsertValues("insert_dataset_list", values)
	return 0, nil
}
