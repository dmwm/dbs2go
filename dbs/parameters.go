package dbs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/dmwm/dbs2go/utils"
)

// ApiParameterFile represents API parameter file
var ApiParametersFile string

// ApiParameters rerepresents a API parameters record
type ApiParameters struct {
	Api        string
	Parameters []string
}

// ApiParametersMap represents data type of api parameters
type ApiParametersMap map[string][]string

// ApiParamMap an object which holds API parameter records
var ApiParamMap ApiParametersMap

// LoadApiParameters loads Api parameters and constructs ApiParameters map
func LoadApiParameters(fname string) (ApiParametersMap, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Printf("Unable to read, file '%s', error: %v\n", fname, err)
		return nil, Error(err, ReaderErrorCode, "", "dbs.parameters.LoadParameters")
	}
	var records []ApiParameters
	err = json.Unmarshal(data, &records)
	if err != nil {
		log.Printf("Unable to parse, file '%s', error: %v\n", fname, err)
		return nil, Error(err, UnmarshalErrorCode, "", "dbs.parameters.LoadParameters")
	}
	pmap := make(ApiParametersMap)
	for _, rec := range records {
		pmap[rec.Api] = rec.Parameters
	}
	return pmap, nil
}

// CheckQueryParameters checks query parameters against API parameters map
func CheckQueryParameters(r *http.Request, api string) error {
	var err error
	if ApiParamMap == nil {
		log.Println("loading", ApiParametersFile)
		ApiParamMap, err = LoadApiParameters(ApiParametersFile)
		if err != nil {
			return Error(GenericErr, LoadErrorCode, "", "dbs.parameters.CheckQueryParameters")
		}
	}
	for k, _ := range r.URL.Query() {
		if params, ok := ApiParamMap[api]; ok {
			if !utils.InList(k, params) {
				msg := fmt.Sprintf("parameter '%s' is not accepted by '%s' API", k, api)
				return Error(
					InvalidParamErr,
					ParametersErrorCode,
					msg,
					"dbs.parameters.CheckQueryParameters")
			}
		} else {
			log.Printf("DBS %s API is not presented in ApiParamMap", api)
		}
	}
	return nil
}
