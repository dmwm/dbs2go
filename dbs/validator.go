package dbs

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/vkuznet/dbs2go/utils"
	"golang.org/x/exp/errors"
)

var datasetPattern = regexp.MustCompile(`^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$`)
var datasetLen = 400
var blockPattern = regexp.MustCompile(`^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}#[a-zA-Z0-9\.\-_]+`)
var blockLen = 400
var primDSPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9\-_]+[*]?$|^[*]$`)
var primDSLen = 99
var procDSPattern = regexp.MustCompile(`[a-zA-Z0-9\.\-_]+`)
var procDSLen = 199
var tierPattern = regexp.MustCompile(`[A-Z\-_]+`)
var tierLen = 99
var eraPattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var eraLen = 99
var releasePattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var releaseLen = 99
var appPattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var appLen = 99
var filePattern = regexp.MustCompile(`/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root`)
var lfnPattern = regexp.MustCompile(`/[a-zA-Z0-9_-]+.*/([a-zA-Z0-9\-_]+).root$`)
var lfnLen = 499

var unixTimePattern = regexp.MustCompile(`^[1-9][0-9]{9}$`)
var intPattern = regexp.MustCompile(`^\d+$`)
var runRangePattern = regexp.MustCompile(`^\d+-\d+$`)

// ObjectPattern represents interface to check different objects
type ObjectPattern interface {
	Check(k string, v interface{}) error
}

// StrPattern represents string object pattern
type StrPattern struct {
	Patterns []*regexp.Regexp
	Len      int
}

// Check implements ObjectPattern interface for StrPattern objects
func (o StrPattern) Check(key string, val interface{}) error {
	var v string
	switch vvv := val.(type) {
	case string:
		v = vvv
	default:
		return errors.New(fmt.Sprintf("invalid type of input parameter '%s' for value '%+v' type '%T'", key, val, val))
	}
	if len(v) > o.Len {
		return errors.New(fmt.Sprintf("length of %s exceed %d charactoers", v, o.Len))
	}
	for _, pat := range o.Patterns {
		if matched := pat.MatchString(v); matched {
			// if at least one pattern matched we'll return
			return nil
		}
	}
	msg := fmt.Sprintf("unable to match '%s' value '%s'", key, val)
	return errors.New(msg)
}

// helper function to validate string parameters
func strType(key string, val interface{}) error {
	var v string
	switch vvv := val.(type) {
	case string:
		v = vvv
	default:
		return errors.New(fmt.Sprintf("invalid type of input parameter '%s' for value '%+v' type '%T'", key, val, val))
	}
	var patterns []*regexp.Regexp
	var length int
	if key == "dataset" {
		patterns = append(patterns, datasetPattern)
		length = datasetLen
	}
	if key == "block_name" {
		patterns = append(patterns, blockPattern)
		length = blockLen
	}
	if key == "logical_file_name" {
		if strings.Contains(v, "[") {
			if strings.Contains(v, "'") { // Python bad json, e.g. ['bla']
				v = strings.Replace(v, "'", "\"", -1)
			}
			var records []string
			err := json.Unmarshal([]byte(v), &records)
			if err != nil {
				return err
			}
			for _, r := range records {
				patterns = append(patterns, filePattern)
				patterns = append(patterns, lfnPattern)
				length = lfnLen
				err := StrPattern{Patterns: patterns, Len: length}.Check(key, r)
				if err != nil {
					return err
				}
			}
		}
		patterns = append(patterns, filePattern)
		patterns = append(patterns, lfnPattern)
		length = lfnLen
	}
	if key == "primary_ds_name" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		patterns = append(patterns, primDSPattern)
		length = primDSLen
	}
	if key == "processed_ds_name" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		patterns = append(patterns, procDSPattern)
		length = procDSLen
	}
	if key == "app_name" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		patterns = append(patterns, appPattern)
		length = appLen
	}
	if key == "release_version" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		patterns = append(patterns, releasePattern)
		length = releaseLen
	}
	return StrPattern{Patterns: patterns, Len: length}.Check(key, val)
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
	}
	return nil
}

// ValidatePostPayload function to validate POST request
func ValidatePostPayload(rec Record) error {
	for key, val := range rec {
		errMsg := fmt.Sprintf("unable to match '%s' value '%+v'", key, val)
		if key == "data_tier_name" {
			v, err := utils.CastString(val)
			if err != nil {
				return errors.New(errMsg)
			} else if matched := tierPattern.MatchString(v); !matched {
				return errors.New(errMsg)
			}
		} else if key == "creation_date" || key == "last_modification_date" {
			v, err := utils.CastInt(val)
			if err != nil {
				return errors.New(errMsg)
			} else if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", v)); !matched {
				return errors.New(errMsg)
			}
		}
	}
	return nil
}
