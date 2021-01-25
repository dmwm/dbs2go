package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
)

// FlatLumis perform flat operation for given lumis lists
func FlatLumis(val interface{}) ([]string, error) {
	// expand input [[1, 20], [30, 40], [50, 60]]
	// to 1,2,3..,20,30,31,..40,...
	var out []string
	switch lumis := val.(type) {
	case string:
		var r []int
		err := json.Unmarshal([]byte(lumis), r)
		if err != nil {
			var r [][]int
			err := json.Unmarshal([]byte(lumis), r)
			if err == nil {
				for _, v := range r {
					if len(v) != 2 {
						return out, errors.New("invalid range of lumis")
					}
					for i := v[0]; i < v[1]; i++ {
						out = append(out, fmt.Sprintf("%d", i))
					}
				}
			}
		} else {
			for _, v := range r {
				out = append(out, fmt.Sprintf("%d", v))
			}
		}
	}
	return out, nil
}

// FileArray DBS API
func (api API) FileArray(params Record, w http.ResponseWriter) (int64, error) {
	// perform some data preprocessing on given record
	log.Printf("FileArray data %+v", params)
	// flat out lumi_list
	if lumis, ok := params["lumi_list"]; ok {
		lumiList, err := FlatLumis(lumis)
		if err != nil {
			return 0, err
		}
		params["lumi_list"] = lumiList
	}
	if len(params) == 0 {
		msg := "filearray api requires input parameers"
		return dbsError(w, msg)
	}
	// When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle
	if _, ok := params["sumOverLumi"]; ok {
		if runs, ok := params["run_num"]; ok {
			vals := fmt.Sprintf("%v", runs)
			if strings.Contains(vals, "[") {
				msg := "When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle"
				return dbsError(w, msg)
			}
		}
		if _, ok := params["lumi_list"]; ok {
			msg := "When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle"
			return dbsError(w, msg)
		}
	}
	return api.Files(params, w)
}

// InsertFileArray DBS API
func (API) InsertFileArray(values Record) error {
	return InsertValues("insert_file_array", values)
}
