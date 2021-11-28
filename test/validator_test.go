package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	validator "github.com/go-playground/validator/v10"
	"github.com/vkuznet/dbs2go/dbs"
)

// TestLexiconPositive
func TestLexiconPositive(t *testing.T) {
	testLexicon(t, "positive")
}

// TestLexiconNegative
func TestLexiconNegative(t *testing.T) {
	testLexicon(t, "negative")
}

// testLexicon
//gocyclo:ignore
func testLexicon(t *testing.T, test string) {
	// set DBS lexicon patterns
	lexiconFile := os.Getenv("DBS_LEXICON_FILE")
	if lexiconFile == "" {
		t.Fatal(errors.New("Please setup DBS_LEXICON_FILE env"))
	}
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	if err != nil {
		t.Fatal(err)
	}
	sampleFile := os.Getenv("DBS_LEXICON_SAMPLE_FILE")
	if sampleFile == "" {
		t.Fatal(errors.New("Please setup DBS_LEXICON_SAMPLE_FILE env"))
	}
	data, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Fatal(err)
	}
	records := make(map[string][]string)
	err = json.Unmarshal(data, &records)
	if err != nil {
		t.Fatal(err)
	}
	cMap := make(map[string]bool)
	for key, values := range records {
		//         log.Println("check", key, "values", values)
		if rec, ok := lexPatterns[key]; ok {
			for _, pat := range rec.Patterns {
				for _, v := range values {
					if matched := pat.MatchString(v); matched {
						cMap[key] = true
						log.Printf("%s=%s matched with %v", key, v, pat)
						break
					}
				}
			}
		} else {
			log.Printf("attribute %s is not present in lexicon records", key)
		}
	}
	log.Printf("performed %s lexicon test against %s", test, sampleFile)
	if test == "positive" {
		for k, v := range cMap {
			if !v {
				t.Errorf("key %s did not match any pattern", k)
			}
		}
	} else if test == "negative" {
		for k, v := range cMap {
			if v {
				t.Errorf("key %s matched some pattern while it should not", k)
			}
		}
	}

}

// TestValidator
func TestValidator(t *testing.T) {
	// set DBS lexicon patterns
	lexiconFile := os.Getenv("DBS_LEXICON_FILE")
	if lexiconFile == "" {
		t.Error(errors.New("Please setup DBS_LEXICON_FILE env"))
	}
	lexPatterns, err := dbs.LoadPatterns(lexiconFile)
	if err != nil {
		t.Fatal(err)
	}
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
		log.Println("Validation is successful")
	} else {
		t.Fatal(err)
	}
}

// helper function to test validation failure
func validationFailure(t *testing.T, rec dbs.DBRecord) {
	log.Printf("Validate %+v", rec)
	err := rec.Validate()
	if err == nil {
		t.Fatal("No error is raised for invalid record")
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
	rec = &dbs.DataTiers{
		DATA_TIER_NAME: tier,
		CREATION_DATE:  ts,
		CREATE_BY:      cby,
	}
	validationFailure(t, rec)
	log.Println("validate no create_by")
	rec = &dbs.DataTiers{
		DATA_TIER_NAME: tier,
		CREATION_DATE:  ts,
	}
	validationFailure(t, rec)
	log.Println("validate creation_date")
	rec = &dbs.DataTiers{DATA_TIER_NAME: tier,
		CREATION_DATE: 123,
		CREATE_BY:     cby,
	}
	validationFailure(t, rec)
	tier = "RAW"
	log.Println("validate correct record")
	rec = &dbs.DataTiers{
		DATA_TIER_NAME: tier,
		CREATION_DATE:  ts,
		CREATE_BY:      cby,
	}
	validationSuccess(t, rec)
}

// TestValidatorDatasetAccessTypes
func TestValidatorDatasetAccessTypes(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate without dataset access type")
	rec = &dbs.DatasetAccessTypes{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.DatasetAccessTypes{
		DATASET_ACCESS_TYPE: "test",
	}
	validationSuccess(t, rec)
}

// TestValidatorPrimaryDatasets
func TestValidatorPrimaryDatasets(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.PrimaryDatasets{}
	validationFailure(t, rec)
	rec = &dbs.PrimaryDatasets{
		PRIMARY_DS_NAME: "test",
	}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	validationFailure(t, rec)
	rec = &dbs.PrimaryDatasets{
		PRIMARY_DS_NAME:    "test",
		PRIMARY_DS_TYPE_ID: 1,
	}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	validationFailure(t, rec)
	rec = &dbs.PrimaryDatasets{
		PRIMARY_DS_NAME:    "test",
		PRIMARY_DS_TYPE_ID: 1,
		CREATION_DATE:      ts,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.PrimaryDatasets{
		PRIMARY_DS_NAME:    "test",
		PRIMARY_DS_TYPE_ID: 1,
		CREATION_DATE:      ts,
		CREATE_BY:          "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorApplicationExecutables
func TestValidatorApplicationExecutables(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.ApplicationExecutables{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.ApplicationExecutables{
		APP_NAME: "test",
	}
	validationSuccess(t, rec)
}

// TestValidatorProcessingEras
func TestValidatorProcessingEras(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.ProcessingEras{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.ProcessingEras{
		PROCESSING_VERSION: 1,
	}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.ProcessingEras{
		PROCESSING_VERSION: 1,
		CREATION_DATE:      ts,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.ProcessingEras{
		PROCESSING_VERSION: 1,
		CREATION_DATE:      ts,
		CREATE_BY:          "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorAcquisitionEras
func TestValidatorAcquisitionEras(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.AcquisitionEras{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: "era",
	}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: "era",
		CREATION_DATE:        ts,
	}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: "era",
		CREATION_DATE:        ts,
		START_DATE:           ts,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: "era",
		CREATION_DATE:        ts,
		START_DATE:           ts,
		CREATE_BY:            "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorDataTiers
func TestValidatorDataTiers(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.DataTiers{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.DataTiers{
		DATA_TIER_NAME: "RAW",
	}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.DataTiers{
		DATA_TIER_NAME: "RAW",
		CREATION_DATE:  ts,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.DataTiers{
		DATA_TIER_NAME: "RAW",
		CREATION_DATE:  ts,
		CREATE_BY:      "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorPhysicsGroups
func TestValidatorPhysicsGroups(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.PhysicsGroups{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.PhysicsGroups{
		PHYSICS_GROUP_NAME: "Physics",
	}
	validationSuccess(t, rec)
}

// TestValidatorProcessedDatasets
func TestValidatorProcessedDatasets(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.ProcessedDatasets{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.ProcessedDatasets{
		PROCESSED_DS_NAME: "Processed",
	}
	validationSuccess(t, rec)
}

// TestValidatorDatasets
func TestValidatorDatasets(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	dataset := "/unittest_web_primary_ds_name_207/acq_era_207-v207/GEN-SIM-RAW"
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.Datasets{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.Datasets{
		DATASET:                dataset,
		IS_DATASET_VALID:       1,
		PRIMARY_DS_ID:          1,
		PROCESSED_DS_ID:        1,
		DATA_TIER_ID:           1,
		DATASET_ACCESS_TYPE_ID: 1,
		ACQUISITION_ERA_ID:     1,
		PROCESSING_ERA_ID:      1,
		PHYSICS_GROUP_ID:       1,
		XTCROSSSECTION:         1.1,
		CREATION_DATE:          ts,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorBlocks
func TestValidatorBlocks(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	block := "/unittest_web_primary_ds_name_207/acq_era_207-v207/GEN-SIM-RAW#123-lskdfjl-123"
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.Blocks{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.Blocks{
		BLOCK_NAME:             block,
		DATASET_ID:             1,
		OPEN_FOR_WRITING:       1,
		ORIGIN_SITE_NAME:       "site",
		BLOCK_SIZE:             123,
		FILE_COUNT:             123,
		CREATION_DATE:          ts,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorFiles
func TestValidatorFiles(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	ts := time.Now().Unix()
	lfn := "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/207/0.root"
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.Files{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.Files{
		LOGICAL_FILE_NAME:      lfn,
		IS_FILE_VALID:          1,
		DATASET_ID:             1,
		BLOCK_ID:               1,
		FILE_TYPE_ID:           1,
		CHECK_SUM:              "sum",
		FILE_SIZE:              123,
		EVENT_COUNT:            123,
		ADLER32:                "adler",
		CREATION_DATE:          123,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       "tester",
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.Files{
		LOGICAL_FILE_NAME:      lfn,
		IS_FILE_VALID:          1,
		DATASET_ID:             1,
		BLOCK_ID:               1,
		FILE_TYPE_ID:           1,
		CHECK_SUM:              "sum",
		FILE_SIZE:              123,
		EVENT_COUNT:            123,
		ADLER32:                "adler",
		CREATION_DATE:          ts,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: ts,
		LAST_MODIFIED_BY:       "tester",
	}
	validationSuccess(t, rec)
}

// TestValidatorFileDataTypes
func TestValidatorFileDataTypes(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.FileDataTypes{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.FileDataTypes{
		FILE_TYPE: "type",
	}
	validationSuccess(t, rec)
}

// TestValidatorFileLumis
func TestValidatorFileLumis(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.FileLumis{}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.FileLumis{
		FILE_ID:          1,
		LUMI_SECTION_NUM: 123,
		RUN_NUM:          123,
	}
	validationSuccess(t, rec)
}

// TestValidatorFileOutputModConfigs
func TestValidatorFileOutputModConfigs(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.FileOutputModConfigs{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.FileOutputModConfigs{
		FILE_ID:              0,
		OUTPUT_MOD_CONFIG_ID: 0,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.FileOutputModConfigs{
		FILE_ID:              1,
		OUTPUT_MOD_CONFIG_ID: 1,
	}
	validationSuccess(t, rec)
}

// TestValidatorFileParents
func TestValidatorFileParents(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.FileParents{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.FileParents{
		THIS_FILE_ID:   0,
		PARENT_FILE_ID: 0,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.FileParents{
		THIS_FILE_ID:   1,
		PARENT_FILE_ID: 1,
	}
	validationSuccess(t, rec)
}

// TestValidatorDatasetParents
func TestValidatorDatasetParents(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.DatasetParents{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.DatasetParents{
		THIS_DATASET_ID:   0,
		PARENT_DATASET_ID: 0,
	}
	validationFailure(t, rec)
	log.Println("validate correct record")
	rec = &dbs.DatasetParents{
		THIS_DATASET_ID:   1,
		PARENT_DATASET_ID: 1,
	}
	validationSuccess(t, rec)
}

// TestValidatorMigrationRequest
func TestValidatorMigrationRequest(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.MigrationRequest{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.MigrationRequest{
		MIGRATION_REQUEST_ID:   1,
		MIGRATION_URL:          "url",
		MIGRATION_INPUT:        "input",
		MIGRATION_STATUS:       0,
		CREATE_BY:              "me",
		CREATION_DATE:          123456789,
		LAST_MODIFIED_BY:       "me",
		LAST_MODIFICATION_DATE: 123456789,
		RETRY_COUNT:            0,
	}
	validationSuccess(t, rec)
}

// TestValidatorMigrationBlocks
func TestValidatorMigrationBlocs(t *testing.T) {
	if dbs.RecordValidator == nil {
		dbs.RecordValidator = validator.New()
	}
	var rec dbs.DBRecord
	log.Println("validate incorrect record")
	rec = &dbs.MigrationBlocks{}
	validationFailure(t, rec)
	log.Println("validate incorrect record")
	rec = &dbs.MigrationBlocks{
		MIGRATION_BLOCK_ID:     1,
		MIGRATION_REQUEST_ID:   1,
		MIGRATION_BLOCK_NAME:   "block",
		MIGRATION_ORDER:        1,
		MIGRATION_STATUS:       1,
		CREATE_BY:              "me",
		CREATION_DATE:          123456789,
		LAST_MODIFIED_BY:       "me",
		LAST_MODIFICATION_DATE: 123456789,
	}
	validationSuccess(t, rec)
}
