package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	validator "github.com/go-playground/validator/v10"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestValidator
func TestValidator(t *testing.T) {
	// set DBS lexicon patterns
	lexiconFile := os.Getenv("DBS_LEXICON_FILE")
	if lexiconFile == "" {
		t.Error(errors.New("Please setup DBS_LEXICON_FILE env"))
	}
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	dbs.LexiconPatterns = lexPatterns

	var req *http.Request
	host := "http://localhost:8111/dbs2go"
	rurl := host + "/primarydatasets?primary_ds_name=*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
	rurl = host + "/primarydatasets?primary_ds_name=bla*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
	rurl = host + "/datasets?dataset=/unittest_web_primary_ds_name*/*/*"
	req, _ = http.NewRequest("GET", rurl, nil)
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
	//     rurl = host + "/datasets?dataset=/*/*/*"
	//     req, _ = http.NewRequest("GET", rurl, nil)
	//     err = dbs.Validate(req)
	//     if err != nil {
	//         t.Error(err)
	//     }
}

// TestValidatePostPayload
func TestValidatePostPayload(t *testing.T) {
	var req *http.Request
	var err error
	host := "http://localhost:8111/dbs2go"
	rurl := host + "/datatiers?data_tier_name=*"
	rec := make(map[string]string)
	rec["data_tier_name"] = "RAW"
	data, err := json.Marshal(rec)
	if err != nil {
		t.Error(err)
	}
	req, _ = http.NewRequest("POST", rurl, bytes.NewBuffer(data))
	err = dbs.Validate(req)
	if err != nil {
		t.Error(err)
	}
}

// helper function to test validation success
func validationSuccess(t *testing.T, rec dbs.DBRecord) {
	log.Printf("Validate %+v", rec)
	err := rec.Validate()
	if err == nil {
		log.Println("Validation is successfull")
	} else {
		t.Error(err)
	}
}

// helper function to test validation failure
func validationFailure(t *testing.T, rec dbs.DBRecord) {
	log.Printf("Validate %+v", rec)
	err := rec.Validate()
	if err == nil {
		t.Error("No error is raised for invalid record")
	} else {
		log.Println("Validator error", err)
	}
}

// TestValidatorDataTier
func TestValidatorDataTier(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	ts := time.Now().Unix()
	cby := "test"
	tier := "raw"
	log.Println("validate lower case data tier")
	rec = &dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: ts, CREATE_BY: cby}
	validationFailure(t, rec)
	log.Println("validate no create_by")
	rec = &dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: ts}
	validationFailure(t, rec)
	log.Println("validate creation_date")
	rec = &dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: 123, CREATE_BY: cby}
	validationFailure(t, rec)
	tier = "RAW"
	log.Println("validate correct record")
	rec = &dbs.DataTiers{DATA_TIER_NAME: tier, CREATION_DATE: ts, CREATE_BY: cby}
	validationSuccess(t, rec)
}

// TestValidatorDatasetAccessType
func TestValidatorDatasetAccessType(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate without dataset access type")
	rec = &dbs.DatasetAccessTypes{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.DatasetAccessTypes{DATASET_ACCESS_TYPE: "test"}
	validationSuccess(t, rec)
}

/*
ApplicationExecutables
PrimaryDatasets
ProcessingEras
AcquisitionEras
DataTiers
PhysicsGroups
DatasetAccessTypes
ProcessedDatasets
Datasets
Blocks
Files
FileDataTypes
FileLumis
FileOutputModConfigs
FileParents
DatasetParents
*/

// TestValidatorPrimaryDataset
func TestValidatorPrimaryDataset(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate without dataset access type")
	rec = &dbs.DatasetAccessTypes{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.DatasetAccessTypes{DATASET_ACCESS_TYPE: "test"}
	validationSuccess(t, rec)
}
