package main

// This file contains common types for test cases. It also contains a function to generate initial data, if the json file does not exist
// the initial data is also loaded into the TestData struct for the test-driven tables defined in  int_*.go

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
)

// Response represents an expected HTTP response body
type Response interface{}

// RequestBody represents an expected HTTP request body
type RequestBody interface{}

// BadRequest represents a request with no good fields
type BadRequest struct {
	BAD_FIELD string
}

// basic elements to define a test case
type testCase struct {
	description string     // test case description
	serverType  string     // DBSWriter, DBSReader, DBSMigrate
	method      string     // http method
	endpoint    string     // url endpoint
	params      url.Values // url parameters
	handler     func(http.ResponseWriter, *http.Request)
	input       RequestBody // POST record
	output      []Response  // expected response
	respCode    int         // expected HTTP response code
}

// initialData struct for test data generation
type initialData struct {
	PrimaryDSName          string   `json:"primary_ds_name"`
	ProcDataset            string   `json:"procdataset"`
	Tier                   string   `json:"tier"`
	Dataset                string   `json:"dataset"`
	ParentDataset          string   `json:"parent_dataset"`
	ParentProcDataset      string   `json:"parent_procdataset"`
	PrimaryDSName2         string   `json:"primary_ds_name2"`
	Dataset2               string   `json:"dataset2"`
	AppName                string   `json:"app_name"`
	OutputModuleLabel      string   `json:"output_module_label"`
	GlobalTag              string   `json:"global_tag"`
	PsetHash               string   `json:"pset_hash"`
	PsetName               string   `json:"pset_name"`
	ReleaseVersion         string   `json:"release_version"`
	Site                   string   `json:"site"`
	Block                  string   `json:"block"`
	ParentBlock            string   `json:"parent_block"`
	Files                  []string `json:"files"`
	ParentFiles            []string `json:"parent_files"`
	Runs                   []int    `json:"runs"`
	AcquisitionEra         string   `json:"acquisition_era"`
	ProcessingVersion      int64    `json:"processing_version"`
	StepPrimaryDSName      string   `json:"step_primary_ds_name"`
	StepchainDataset       string   `json:"stepchain_dataset"`
	StepchainBlock         string   `json:"stepchain_block"`
	ParentStepchainDataset string   `json:"parent_stepchain_dataset"`
	ParentStepchainBlock   string   `json:"parent_stepchain_block"`
	StepchainFiles         []string `json:"stepchain_files"`
	ParentStepchainFiles   []string `json:"parent_stepchain_files"`
}

// TestData contains the generated data
var TestData initialData

// defines a testcase for an endpoint
type EndpointTestCase struct {
	description     string
	defaultHandler  func(http.ResponseWriter, *http.Request)
	defaultEndpoint string
	testCases       []testCase
}

// get a UUID time_mid as an int
func getUUID(t *testing.T) int64 {
	uuid, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	time_mid := uuid[4:6]
	uid := hex.EncodeToString(time_mid)
	uidInt, err := strconv.ParseInt(uid, 16, 64)
	if err != nil {
		t.Fatal(err)
	}

	return uidInt
}

// generate test data based on dmwm/DBSClient tests/dbsclient_t/unittests/DBSClientWriter_t.py
func generateBaseData(t *testing.T, filepath string) {
	uid := getUUID(t)
	fmt.Printf("****uid=%v*****\n", uid)

	PrimaryDSName := fmt.Sprintf("unittest_web_primary_ds_name_%v", uid)
	processing_version := uid % 9999
	if uid < 9999 {
		processing_version = uid
	}
	acquisition_era_name := fmt.Sprintf("acq_era_%v", uid)
	ProcDataset := fmt.Sprintf("%s-v%v", acquisition_era_name, processing_version)
	parent_procdataset := fmt.Sprintf("%s-ptsr-v%v", acquisition_era_name, processing_version)
	Tier := "GEN-SIM-RAW"
	dataset := fmt.Sprintf("/%s/%s/%s", PrimaryDSName, ProcDataset, Tier)
	primary_ds_name2 := fmt.Sprintf("%s_2", PrimaryDSName)
	dataset2 := fmt.Sprintf("/%s/%s/%s", primary_ds_name2, ProcDataset, Tier)
	app_name := "cmsRun"
	output_module_label := "merged"
	global_tag := fmt.Sprintf("my-cms-gtag_%v", uid)
	pset_hash := "76e303993a1c2f842159dbfeeed9a0dd"
	pset_name := "UnittestPsetName"
	release_version := "CMSSW_1_2_3"
	site := "cmssrm.fnal.gov"
	block := fmt.Sprintf("%s#%v", dataset, uid)
	parent_dataset := fmt.Sprintf("/%s/%s/%s", PrimaryDSName, parent_procdataset, Tier)
	parent_block := fmt.Sprintf("%s#%v", parent_dataset, uid)

	step_primary_ds_name := fmt.Sprintf("%s_stepchain", PrimaryDSName)
	stepchain_dataset := fmt.Sprintf("/%s/%s/%s", step_primary_ds_name, ProcDataset, Tier)
	stepchain_block := fmt.Sprintf("%s#%v", stepchain_dataset, uid)
	parent_stepchain_dataset := fmt.Sprintf("/%s/%s/%s", step_primary_ds_name, parent_procdataset, Tier)
	parent_stepchain_block := fmt.Sprintf("%s#%v", parent_stepchain_dataset, uid)
	stepchain_files := []string{}
	parent_stepchain_files := []string{}

	TestData.PrimaryDSName = PrimaryDSName
	TestData.ProcDataset = ProcDataset
	TestData.Tier = Tier
	TestData.Dataset = dataset
	TestData.ParentDataset = parent_dataset
	TestData.ParentProcDataset = parent_procdataset
	TestData.PrimaryDSName2 = primary_ds_name2
	TestData.Dataset2 = dataset2
	TestData.AppName = app_name
	TestData.OutputModuleLabel = output_module_label
	TestData.GlobalTag = global_tag
	TestData.PsetHash = pset_hash
	TestData.PsetName = pset_name
	TestData.ReleaseVersion = release_version
	TestData.Site = site
	TestData.Block = block
	TestData.ParentBlock = parent_block
	TestData.Files = []string{}
	TestData.ParentFiles = []string{}
	TestData.Runs = []int{97, 98, 99}
	TestData.AcquisitionEra = acquisition_era_name
	TestData.ProcessingVersion = processing_version
	TestData.StepPrimaryDSName = step_primary_ds_name
	TestData.StepchainDataset = stepchain_dataset
	TestData.StepchainBlock = stepchain_block
	TestData.ParentStepchainDataset = parent_stepchain_dataset
	TestData.ParentStepchainBlock = parent_stepchain_block
	TestData.StepchainFiles = stepchain_files
	TestData.ParentStepchainFiles = parent_stepchain_files

	// fmt.Println(TestData)
	file, _ := json.MarshalIndent(TestData, "", "  ")
	_ = ioutil.WriteFile(filepath, file, os.ModePerm)
}

// reads a json file and load into TestData
func readJsonFile(t *testing.T, filename string) {
	var data []byte
	var err error
	// var testData map[string]interface{}
	data, err = os.ReadFile(filename)
	if err != nil {
		log.Printf("ERROR: unable to read %s error %v", filename, err.Error())
		t.Fatal(err.Error())
	}
	err = json.Unmarshal(data, &TestData)
	if err != nil {
		log.Println("unable to unmarshal received data")
		t.Fatal(err.Error())
	}
}

// LoadTestCases loads the InitialData from a json file
func LoadTestCases(t *testing.T, filepath string) []EndpointTestCase {
	if _, err := os.Stat(filepath); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating data")
		generateBaseData(t, filepath)
	}
	readJsonFile(t, filepath)

	primaryDatasetAndTypesTestCase := getPrimaryDatasetTestTable(t)
	outputConfigTestCase := getOutputConfigTestTable(t)
	acquisitionErasTestCase := getAcquisitionErasTestTable(t)
	processingErasTestCase := getProcessingErasTestTable(t)
	datatiersTestCase := getDatatiersTestTable(t)
	datasetAccessTypesTestCase := getDatasetAccessTypesTestTable(t)
	physicsGroupsTestCase := getPhysicsGroupsTestTable(t)
	datasetsTestCase := getDatasetsTestTable(t)

	return []EndpointTestCase{
		primaryDatasetAndTypesTestCase,
		outputConfigTestCase,
		acquisitionErasTestCase,
		processingErasTestCase,
		datatiersTestCase,
		datasetAccessTypesTestCase,
		physicsGroupsTestCase,
		datasetsTestCase,
	}
}
