package dbs

import (
	"log"
)

// DatasetList DBS API
func (a *API) DatasetList() error {
	// perform some data preprocessing on given record
	log.Printf("DatasetList data %+v", a.Params)
	return a.Datasets()
}

// InsertDatasetList DBS API
func (a *API) InsertDatasetList() error {
	//     return InsertValues("insert_dataset_list", values)
	return nil
}
