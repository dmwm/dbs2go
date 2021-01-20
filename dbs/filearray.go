package dbs

import (
	"log"
	"net/http"
)

// helper function to flat out given lumis
func flatLumis(lumis interface{}) []string {
	var out []string
	log.Println("flatLumis", lumis)
	return out
}

// FileArray DBS API
func (api API) FileArray(params Record, w http.ResponseWriter) (int64, error) {
	// perform some data preprocessing on given record
	log.Printf("FileArray data %+v", params)
	// flat out lumi_list
	if lumis, ok := params["lumi_list"]; ok {
		params["lumi_list"] = flatLumis(lumis)
	}
	if len(params) == 0 {
		return 0, nil
	}
	return api.Files(params, w)
}

// InsertFileArray DBS API
func (API) InsertFileArray(values Record) error {
	return InsertValues("insert_file_array", values)
}
