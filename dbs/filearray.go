package dbs

import (
	"encoding/json"
	"fmt"
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
	if strings.HasPrefix(lumis, "[[[") {
		lumis = strings.Replace(lumis, "[[[", "[[", -1)
		lumis = strings.Replace(lumis, "]]]", "]]", -1)
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
			return out, Error(err, UnmarshalErrorCode, "", "dbs.filearray.FlatLumis")
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
func (a *API) FileArray() error {
	// perform some data preprocessing on given record
	// flat out lumi_list
	if lumis, ok := a.Params["lumi_list"]; ok {
		lumiList, err := FlatLumis(lumis)
		if err != nil {
			return Error(err, InvalidParameterErrorCode, "invalid lumi_list parameter", "dbs.filearray.FileArray")
		}
		a.Params["lumi_list"] = lumiList
	}
	if len(a.Params) == 0 {
		msg := "filearray api requires input parameters"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.filearray.FileArray")
	}
	return a.Files()
}

// InsertFileArray DBS API
func (a *API) InsertFileArray() error {
	//     return InsertValues("insert_file_array", values)
	return nil
}
