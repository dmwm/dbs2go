package dbs

import (
	"encoding/json"
	"log"
	"net/http"

	"golang.org/x/exp/errors"
)

// Validate provides validation of all input parameters of HTTP request
func Validate(r *http.Request) error {
	if r.Method == "GET" {
		for k, v := range r.URL.Query() {
			log.Printf("query parameter key=%s values=%+v\n", k, v)
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
