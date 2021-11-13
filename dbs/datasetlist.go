package dbs

import (
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// DatasetList DBS API
func (a *API) DatasetList() error {
	// perform some data preprocessing on given record
	if utils.VERBOSE > 0 {
		log.Printf("DatasetList data %+v", a.Params)
	}
	return a.Datasets()
}

// InsertDatasetList DBS API
func (a *API) InsertDatasetList() error {
	//     return InsertValues("insert_dataset_list", values)
	return nil
}
