package dbs

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/dmwm/das2go/utils"
	"golang.org/x/exp/errors"
)

var datasetPattern = regexp.MustCompile(`^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$`)
var cdatePattern = regexp.MustCompile(`\d`)

// helper function to validate string parameters
func strType(key string, val interface{}) error {
	var v string
	switch vvv := val.(type) {
	case string:
		v = vvv
	default:
		return errors.New(fmt.Sprintf("invalid type of input parameter '%s' for value '%+v'", key, val))
	}
	// to be implemented
	if key == "dataset" {
		if matched := datasetPattern.MatchString(v); !matched {
			return errors.New(fmt.Sprintf("unable to match '%s' value '%+v'", key, val))
		}
	}
	return nil
	//     return errors.New(fmt.Sprintf("Invalid type of %s, should be string", k))
}

// helper function to validate int parameters
func intType(k string, v interface{}) error {
	// to be implemented
	return nil
}

// helper function to validate mix parameters
func mixType(k string, v interface{}) error {
	// to be implemented
	return nil
}

// Validate provides validation of all input parameters of HTTP request
func Validate(r *http.Request) error {
	strParameters := []string{"dataset", "parent_dataset", "release_version", "pset_hash", "app_name", "output_module_label", "global_tag", "processing_version", "acquisition_era_name", "physics_group_name", "logical_file_name", "primary_ds_name", "primary_ds_type", "processed_ds_name", "data_tier_name", "dataset_access_type", "create_by", "last_modified_by"}
	intParameters := []string{"cdate", "ldate", "min_cdate", "max_cdate", "min_ldate", "max_ldate", "datset_id", "prep_id"}
	mixParameters := []string{"run_num"} // can be different type
	if r.Method == "GET" {
		for k, vvv := range r.URL.Query() {
			// vvv here is []string{} type since all HTTP parameters are treated
			// as list of strings
			for _, v := range vvv {
				if utils.InList(k, strParameters) {
					if err := strType(k, v); err != nil {
						return err
					}
				}
				if utils.InList(k, intParameters) {
					if err := intType(k, v); err != nil {
						return err
					}
				}
				if utils.InList(k, mixParameters) {
					if err := mixType(k, v); err != nil {
						return err
					}
				}
			}
			log.Printf("query parameter key=%s values=%+v\n", k, vvv)
		}
	} else if r.Method == "POST" {
		var rec Record
		var unmarshalErr *json.UnmarshalTypeError
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&rec)
		if err != nil {
			if errors.As(err, &unmarshalErr) {
				return errors.New("Bad Request. Wrong Type provided for field " + unmarshalErr.Field)
			} else {
				return errors.New("Bad Request " + err.Error())
			}
			return err
		}
		// validate post form
		return validatePostRequest(rec)
	}
	return nil
}

// helper function to validate POST request
func validatePostRequest(rec Record) error {
	return nil
}
