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
	"gitlab.com/tymonx/go-formatter/formatter"
	"golang.org/x/exp/errors"
)

// Parts define multiple patterns
type Parts struct {
	Era          string
	PrimDS       string
	Tier         string
	Version      string
	ProcDS       string
	Counter      string
	Root         string
	HnName       string
	Subdir       string
	File         string
	Workflow     string
	PhysicsGroup string
}

// LexiconPatterns represents CMS Lexicon patterns
var LexiconPatterns map[string][]*regexp.Regexp

// LoadPatterns loads CMS Lexion patterns from given file
func LoadPatterns(fname string) (map[string][]*regexp.Regexp, error) {
	var rec map[string][]string
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Printf("Unable to read, file '%s', error: %v\n", fname, err)
		return nil, err
	}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("Unable to parse, file '%s', error: %v\n", fname, err)
		return nil, err
	}
	// fetch and compile all patterns
	pmap := make(map[string][]*regexp.Regexp)
	for key, pats := range rec {
		var patterns []*regexp.Regexp
		for _, pat := range pats {
			patterns = append(patterns, regexp.MustCompile(pat))
		}
		pmap[key] = patterns
	}
	return pmap, nil
}

var parts = Parts{
	Era:          `([a-zA-Z0-9\-_]+)`,
	PrimDS:       `([a-zA-Z][a-zA-Z0-9\-_]*)`,
	Tier:         `([A-Z\-_]+)`,
	Version:      `([a-zA-Z0-9\-_]+)`,
	ProcDS:       `([a-zA-Z0-9\-_]+)`,
	Counter:      `([0-9]+)`,
	Root:         `([a-zA-Z0-9\-_]+).root`,
	HnName:       `([a-zA-Z0-9\.]+)`,
	Subdir:       `([a-zA-Z0-9\-_]+)`,
	File:         `([a-zA-Z0-9\-\._]+)`,
	Workflow:     `([a-zA-Z0-9\-_]+)`,
	PhysicsGroup: `([a-zA-Z0-9\-_]+)`,
}

// dataset patterns
var datasetPattern = regexp.MustCompile(`^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$`)
var datasetLen = 400

// block patterns
var blockPattern = regexp.MustCompile(`^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,199})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}#[a-zA-Z0-9\.\-_]+`)
var blockLen = 400

// primary dataset patterns
var primDSPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9\-_]+[*]?$|^[*]$`)
var primDSLen = 99

// processed dataset patterns
var procDSPattern = regexp.MustCompile(`[a-zA-Z0-9\.\-_]+`)
var procDSLen = 199

// data tier patterns
var tierPattern = regexp.MustCompile(`[A-Z\-_]+`)
var tierLen = 99

// acquisision era patterns
var eraPattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var eraLen = 99

// release patterns
var releasePattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var releaseLen = 99

// application patterns
var appPattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var appLen = 99

// physics group patterns
var physGroupPattern = regexp.MustCompile(`([a-zA-Z0-9\-_]+)`)
var physGroupLen = 30

// helper function to get user patterns
func userPatterns() ([]*regexp.Regexp, error) {
	var out []*regexp.Regexp
	pat1 := `^/[a-zA-Z][a-zA-Z0-9/\=\s()\']*\=[a-zA-Z0-9/\=\.\-_/#:\s\']*$`
	pat2 := `^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_\']*$`
	pat3 := `^[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*@[a-zA-Z0-9/][a-zA-Z0-9/\.\-_]*$`
	out = append(out, regexp.MustCompile(pat1))
	out = append(out, regexp.MustCompile(pat2))
	out = append(out, regexp.MustCompile(pat3))
	return out, nil
}

var userLen = 30

// helper function to get file, lfn patterns
func lfnPatterns() ([]*regexp.Regexp, error) {
	var out []*regexp.Regexp
	regexp1, err := formatter.Format(`/([a-z]+)/([a-z0-9]+)/({.Era})/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root'`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(regexp1))
	regexp2 := `/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root`
	out = append(out, regexp.MustCompile(regexp2))
	regexp3, err := formatter.Format(`/store/(temp/)*(user|group)/({.HnName}|{.PhysicsGroup})/{.PrimDS}/{.ProcDS}/{.Version}/{.Counter}/{.Root}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(regexp3))
	regexp4, err := formatter.Format(`/store/(temp/)*(user|group)/({.HnName}|{.PhysicsGroup})/{.PrimDS/({.Subdir}/)+{.Root}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(regexp4))
	oldStyleTier0LFN, err := formatter.Format(`/store/data/{.Era}/{.PrimDS}/{.Tier}/{.Version}/{.Counter}/{.Counter}/{.Counter}/{.Root}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(oldStyleTier0LFN))
	tier0LFN, err := formatter.Format(`/store/(backfill/[0-9]/){0,1}(t0temp/|unmerged/){0,1}(data|express|hidata)/{.Era}/{.PrimDS}/{.Tier}/{.Version}/{.Counter}/{.Counter}/{.Counter}(/{.Counter})?/{.Root}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(tier0LFN))
	storeMcLFN, err := formatter.Format(`/store/mc/({.Era})/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)(/([a-zA-Z0-9\-_]+))*/([a-zA-Z0-9\-_]+).root`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(storeMcLFN))
	storeResults2LFN, err := formatter.Format(`/store/results/{.PhysicsGroup}/{.PrimDS}/{.ProcDS}/{.PrimDS}/{.Tier}/{.ProcDS}/{.Counter}/{.Root}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(storeResults2LFN))
	storeResultsLFN, err := formatter.Format(`/store/results/{.PhysicsGroup}/{.Era}/{.PrimDS}/{.Tier}/{.ProcDS}/{.Counter}/{.Root}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(storeResultsLFN))
	lheLFN1 := `/store/lhe/([0-9]+)/([a-zA-Z0-9\-_]+).lhe(.xz){0,1}`
	out = append(out, regexp.MustCompile(lheLFN1))
	lheLFN2, err := formatter.Format(`/store/lhe/{.Era}/{.PrimDS}/([0-9]+)/([a-zA-Z0-9\-_]+).lhe(.xz){0,1}`, parts)
	if err != nil {
		return out, err
	}
	out = append(out, regexp.MustCompile(lheLFN2))
	return out, nil
}

var lfnLen = 499

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
		if pats, ok := LexiconPatterns["dataset"]; ok {
			patterns = pats
		} else {
			patterns = append(patterns, datasetPattern)
		}
		length = datasetLen
	}
	if key == "block_name" {
		if pats, ok := LexiconPatterns["block"]; ok {
			patterns = pats
		} else {
			patterns = append(patterns, blockPattern)
		}
		length = blockLen
	}
	if key == "create_by" || key == "last_modified_by" {
		if pats, ok := LexiconPatterns["user"]; ok {
			patterns = pats
		} else {
			uPatterns, err := userPatterns()
			if err != nil {
				return err
			}
			patterns = uPatterns
		}
		length = userLen
	}
	if key == "logical_file_name" {
		length = lfnLen
		if pats, ok := LexiconPatterns["lfn"]; ok {
			patterns = pats
		} else {
			filePatterns, err := lfnPatterns()
			if err != nil {
				return err
			}
			patterns = filePatterns
		}
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
				err := StrPattern{Patterns: patterns, Len: length}.Check(key, r)
				if err != nil {
					return err
				}
			}
		}
	}
	if key == "primary_ds_name" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		if pats, ok := LexiconPatterns["primary_dataset"]; ok {
			patterns = pats
		} else {
			patterns = append(patterns, primDSPattern)
		}
		length = primDSLen
	}
	if key == "processed_ds_name" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		if pats, ok := LexiconPatterns["processed_dataset"]; ok {
			patterns = pats
		} else {
			patterns = append(patterns, procDSPattern)
		}
		length = procDSLen
	}
	if key == "app_name" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		if pats, ok := LexiconPatterns["application"]; ok {
			patterns = pats
		} else {
			patterns = append(patterns, appPattern)
		}
		length = appLen
	}
	if key == "release_version" {
		if v == "" && val == "*" { // when someone passed wildcard
			return nil
		}
		if pats, ok := LexiconPatterns["cmssw_version"]; ok {
			patterns = pats
		} else {
			patterns = append(patterns, releasePattern)
		}
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
