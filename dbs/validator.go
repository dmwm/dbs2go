package dbs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/vkuznet/dbs2go/utils"
)

// DBS string parameters
var strParameters = []string{
	"dataset",
	"block_name",
	"parent_dataset",
	"release_version",
	"pset_hash",
	"app_name",
	"output_module_label",
	"global_tag",
	"processing_version",
	"acquisition_era_name",
	"physics_group_name",
	"logical_file_name",
	"primary_ds_name",
	"primary_ds_type",
	"processed_ds_name",
	"data_tier_name",
	"dataset_access_type",
	"create_by",
	"user",
	"last_modified_by",
}

// DBS integer parameters
var intParameters = []string{
	"cdate",
	"ldate",
	"min_cdate",
	"max_cdate",
	"min_ldate",
	"max_ldate",
	"datset_id",
	"prep_id",
}

// DBS mix type parameters
var mixParameters = []string{"run_num"}

// Lexicon represents single lexicon pattern structure
type Lexicon struct {
	Name     string   `json:"name"`
	Patterns []string `json:"patterns"`
	Length   int      `json:"length"`
}

func (r *Lexicon) String() string {
	data, err := json.MarshalIndent(r, "", "  ")
	if err == nil {
		return string(data)
	}
	return fmt.Sprintf("Lexicon: name=%s patters=%v length=%d", r.Name, r.Patterns, r.Length)
}

// LexiconPattern represents single lexicon compiled pattern structure
type LexiconPattern struct {
	Lexicon  Lexicon
	Patterns []*regexp.Regexp
}

// LexiconPatterns represents CMS Lexicon patterns
var LexiconPatterns map[string]LexiconPattern

// LoadPatterns loads CMS Lexion patterns from given file
// the format of the file is a list of the following dicts:
// [ {"name": <name>, "patterns": [list of patterns], "length": int},...]
func LoadPatterns(fname string) (map[string]LexiconPattern, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Printf("Unable to read, file '%s', error: %v\n", fname, err)
		return nil, Error(err, ReaderErrorCode, "", "dbs.validator.LoadPatterns")
	}
	var records []Lexicon
	err = json.Unmarshal(data, &records)
	if err != nil {
		log.Printf("Unable to parse, file '%s', error: %v\n", fname, err)
		return nil, Error(err, UnmarshalErrorCode, "", "dbs.validator.LoadPatterns")
	}
	// fetch and compile all patterns
	pmap := make(map[string]LexiconPattern)
	for _, rec := range records {
		var patterns []*regexp.Regexp
		for _, pat := range rec.Patterns {
			patterns = append(patterns, regexp.MustCompile(pat))
		}
		lex := LexiconPattern{Lexicon: rec, Patterns: patterns}
		key := rec.Name
		pmap[key] = lex
		if utils.VERBOSE > 1 {
			log.Printf("regexp pattern\n%s", rec.String())
		}
	}
	return pmap, nil
}

// aux patterns
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
	if utils.VERBOSE > 0 {
		log.Printf("StrPatern check key=%s val=%v", key, val)
		log.Printf("patterns %v max length %v", o.Patterns, o.Len)
	}
	var v string
	switch vvv := val.(type) {
	case string:
		v = vvv
	default:
		msg := fmt.Sprintf("invalid type of input parameter '%s' for value '%+v' type '%T'", key, val, val)
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.validator.Check")
	}
	if len(o.Patterns) == 0 {
		// nothing to match in patterns
		if utils.VERBOSE > 0 {
			log.Println("nothing to match since we do not have patterns")
		}
		return nil
	}
	if o.Len > 0 && len(v) > o.Len {
		if utils.VERBOSE > 0 {
			log.Println("lexicon str pattern", o)
		}
		msg := fmt.Sprintf("length of %s exceed %d characters", v, o.Len)
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.validator.Check")
	}
	msg := fmt.Sprintf("unable to match '%s' value '%s'", key, val)
	for _, pat := range o.Patterns {
		if matched := pat.MatchString(v); matched {
			// if at least one pattern matched we'll return
			return nil
		}
	}
	return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.validator.Check")
}

//gocyclo:ignore
// helper function to validate string parameters
func strType(key string, val interface{}) error {
	var v string
	switch vvv := val.(type) {
	case string:
		v = vvv
	default:
		msg := fmt.Sprintf("invalid type of input parameter '%s' for value '%+v' type '%T'", key, val, val)
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.validator.strType")
	}
	mapKeys := make(map[string]string)
	mapKeys["dataset"] = "dataset"
	mapKeys["block_name"] = "block_name"
	mapKeys["logical_file_name"] = "logical_file_name"
	mapKeys["create_by"] = "user"
	mapKeys["last_modified_by"] = "user"
	mapKeys["primary_ds_name"] = "primary_dataset"
	mapKeys["processed_ds_name"] = "processed_dataset"
	mapKeys["processing_version"] = "processing_version"
	mapKeys["app_name"] = "application"
	mapKeys["data_tier_name"] = "data_tier_name"
	mapKeys["dataset"] = "dataset"
	mapKeys["release_version"] = "cmssw_version"
	var allowedWildCardKeys = []string{
		"primary_ds_name",
		"processed_ds_name",
		"processing_version",
		"app_name",
		"data_tier_name",
		"release_version",
	}

	var patterns []*regexp.Regexp
	var length int

	for k, lkey := range mapKeys {
		if key == k {
			if utils.InList(k, allowedWildCardKeys) {
				if v == "" && val == "*" { // when someone passed wildcard
					return nil
				}
			}
			if p, ok := LexiconPatterns[lkey]; ok {
				patterns = p.Patterns
				length = p.Lexicon.Length
			}
		}
		if key == "logical_file_name" {
			if strings.Contains(v, "[") {
				if strings.Contains(v, "'") { // Python bad json, e.g. ['bla']
					v = strings.Replace(v, "'", "\"", -1)
				}
				var records []string
				err := json.Unmarshal([]byte(v), &records)
				if err != nil {
					return Error(err, UnmarshalErrorCode, "", "dbs.validator.strType")
				}
				for _, r := range records {
					err := StrPattern{Patterns: patterns, Len: length}.Check(key, r)
					if err != nil {
						return Error(err, PatternErrorCode, "", "dbs.validator.strType")
					}
				}
			}
		}
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
	if r.Method == "GET" {
		for k, vvv := range r.URL.Query() {
			// vvv here is []string{} type since all HTTP parameters are treated
			// as list of strings
			for _, v := range vvv {
				if utils.InList(k, strParameters) {
					if err := strType(k, v); err != nil {
						return Error(err, ValidateErrorCode, "not str type", "dbs.Validate")
					}
				}
				if utils.InList(k, intParameters) {
					if err := intType(k, v); err != nil {
						return Error(err, ValidateErrorCode, "not int type", "dbs.Validate")
					}
				}
				if utils.InList(k, mixParameters) {
					if err := mixType(k, v); err != nil {
						return Error(err, ValidateErrorCode, "not mix type", "dbs.Validate")
					}
				}
			}
			if utils.VERBOSE > 0 {
				log.Printf("query parameter key=%s values=%+v\n", k, vvv)
			}
		}
	}
	return nil
}

// CheckPattern is a generic functino to check given key value within Lexicon map
func CheckPattern(key, value string) error {
	if p, ok := LexiconPatterns[key]; ok {
		for _, pat := range p.Patterns {
			if matched := pat.MatchString(value); matched {
				if utils.VERBOSE > 0 {
					log.Printf("CheckPattern key=%s value='%s' found match %s", key, value, pat)
				}
				return nil
			}
			if utils.VERBOSE > 0 {
				log.Printf("CheckPattern key=%s value='%s' does not match %s", key, value, pat)
			}
		}
		msg := fmt.Sprintf("invalid pattern for key=%s", key)
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.CheckPattern")
	}
	return nil
}

// ValidatePostPayload function to validate POST request
func ValidatePostPayload(rec Record) error {
	for key, val := range rec {
		errMsg := fmt.Sprintf("unable to match '%s' value '%+v'", key, val)
		if key == "data_tier_name" {
			if vvv, ok := val.(string); ok {
				if err := CheckPattern("data_tier_name", vvv); err != nil {
					return Error(err, PatternErrorCode, "wrong data_tier_name pattern", "dbs.ValidaatePostPayload")
				}
			}
		} else if key == "creation_date" || key == "last_modification_date" {
			v, err := utils.CastInt(val)
			if err != nil {
				return Error(err, PatternErrorCode, errMsg, "dbs.ValidaatePostPayload")
			} else if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", v)); !matched {
				return Error(InvalidParamErr, PatternErrorCode, errMsg, "dbs.ValidaatePostPayload")
			}
		}
	}
	return nil
}
