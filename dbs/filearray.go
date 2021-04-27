package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// FlatLumis perform flat operation for given lumis lists
func FlatLumis(val interface{}) ([]string, error) {
	// expand input [[1, 20], [30, 40], [50, 60]]
	// to 1,2,3..,20,30,31,..40,...
	lumis := fmt.Sprintf("%v", val)
	if strings.Contains(lumis, "+") {
		// input like %5B%5B1%2C+20%5D%2C+%5B30%2C+40%5D%2C+%5B50%2C+60%5D%5D
		// [[1,+20],+[30,+40],+[50,+60]]
		lumis = strings.Replace(lumis, "+", " ", -1)
	}
	if strings.Contains(lumis, " ") && !strings.Contains(lumis, ",") {
		// input like [[1 20] [30 40]]
		lumis = strings.Replace(lumis, " ", ",", -1)
	}
	var out []string
	var r []int
	err := json.Unmarshal([]byte(lumis), &r)
	if err == nil {
		for _, v := range r {
			out = append(out, fmt.Sprintf("%d", v))
		}
	} else {
		var r [][]int
		err := json.Unmarshal([]byte(lumis), &r)
		if err != nil {
			return out, err
		}
		for _, v := range r {
			if len(v) == 2 {
				for i := v[0]; i <= v[1]; i++ {
					out = append(out, fmt.Sprintf("%d", i))
				}
			} else {
				for _, x := range v {
					out = append(out, fmt.Sprintf("%d", x))
				}
			}
		}
	}
	return out, nil
}

// FileArray DBS API
func (api API) FileArray(params Record, w http.ResponseWriter) (int64, error) {
	// perform some data preprocessing on given record
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
		return 0, errors.New(msg)
	}
	// When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle
	if _, ok := params["sumOverLumi"]; ok {
		if runs, ok := params["run_num"]; ok {
			vals := fmt.Sprintf("%v", runs)
			if strings.Contains(vals, "[") {
				msg := "When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle"
				return 0, errors.New(msg)
			}
		}
		if _, ok := params["lumi_list"]; ok {
			msg := "When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle"
			return 0, errors.New(msg)
		}
	}
	return api.Files(params, w)
}

// InsertFileArray DBS API
func (API) InsertFileArray(r io.Reader, cby string) error {
	//     return InsertValues("insert_file_array", values)
	return nil
}
