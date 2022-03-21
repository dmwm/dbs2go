package main

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
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/web"
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
	serverType  string     //DBSWriter, DBSReader, DBSMigrate
	method      string     // http method
	endpoint    string     // url endpoint
	params      url.Values // url parameters
	handler     func(http.ResponseWriter, *http.Request)
	input       RequestBody //kjdPOST record
	output      []Response  // expected response
	respCode    int         // expected HTTP response code
}

// InitialData struct for test data generation
type InitialData struct {
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
var TestData InitialData

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

// generate test data
func generateBaseData(t *testing.T) {
	uid := getUUID(t)
	fmt.Printf("****uid=%v*****\n", uid)

	PrimaryDSName := fmt.Sprintf("unittest_web_primary_ds_name_%v", uid)
	processing_version := uid % 9999
	if uid < 9999 {
		processing_version = uid
	}
	acquisition_era_name := fmt.Sprintf("acq_era_%v", uid)
	ProcDataset := fmt.Sprintf("%s-pstr-v%v", acquisition_era_name, processing_version)
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
	_ = ioutil.WriteFile("./data/integration/integration_data.json", file, os.ModePerm)
}

// primarydataset and primarydstype endpoints tests
func getPrimaryDatasetTestTable(t *testing.T) EndpointTestCase {
	// primaryDSTypesResponse is the expected primarydstypes GET response
	type primaryDSTypesResponse struct {
		DATA_TYPE          string `json:"data_type"`
		PRIMARY_DS_TYPE_ID int64  `json:"primary_ds_type_id"`
	}

	// create data structs for expected requests and responses
	primaryDSReq := dbs.PrimaryDatasetRecord{
		PRIMARY_DS_NAME: TestData.PrimaryDSName,
		PRIMARY_DS_TYPE: "test",
		CREATE_BY:       "tester",
	}
	primaryDSResp := dbs.PrimaryDataset{
		PrimaryDSId:   1.0,
		PrimaryDSType: "test",
		PrimaryDSName: TestData.PrimaryDSName,
		CreationDate:  0,
		CreateBy:      "tester",
	}
	primaryDSTypeResp := primaryDSTypesResponse{
		PRIMARY_DS_TYPE_ID: 1.0,
		DATA_TYPE:          "test",
	}
	primaryDSReq2 := dbs.PrimaryDatasetRecord{
		PRIMARY_DS_NAME: TestData.PrimaryDSName2,
		PRIMARY_DS_TYPE: "test",
		CREATE_BY:       "tester",
	}
	primaryDSResp2 := dbs.PrimaryDataset{
		PrimaryDSId:   2.0,
		PrimaryDSType: "test",
		PrimaryDSName: TestData.PrimaryDSName2,
		CreateBy:      "tester",
		CreationDate:  0,
	}
	dbsError1 := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Message:  "unable to match 'dataset' value 'fnal'",
		Code:     dbs.PatternErrorCode,
		Function: "dbs.validator.Check",
	}
	dbsError := dbs.DBSError{
		Function: "dbs.Validate",
		Code:     dbs.ValidateErrorCode,
		Reason:   dbsError1.Error(),
		Message:  "not str type",
	}
	hrec := web.HTTPError{
		Method:    "GET",
		Timestamp: "",
		HTTPCode:  http.StatusBadRequest,
		Path:      "/dbs/primarydstypes?dataset=fnal",
		UserAgent: "Go-http-client/1.1",
	}
	errorResp := web.ServerError{
		HTTPError: hrec,
		DBSError:  &dbsError,
		Exception: http.StatusBadRequest,
		Type:      "HTTPError",
		Message:   dbsError.Error(),
	}
	return EndpointTestCase{
		description:     "Test primarydataset",
		defaultHandler:  web.PrimaryDatasetsHandler,
		defaultEndpoint: "/dbs/primarydatasets",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				serverType:  "DBSReader",
				method:      "GET",
				params:      nil,
				input:       nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydstypes GET with no Data",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				params:      nil,
				handler:     web.PrimaryDSTypesHandler,
				input:       nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets bad POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: BadRequest{
					BAD_FIELD: "Bad",
				},
				params:   nil,
				output:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test primarydatasets POST",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSReq,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydstypes GET",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params:   nil,
				respCode: http.StatusOK,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydstypes GET w primary_ds_type param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params: url.Values{
					"primary_ds_type": []string{"test"},
				},
				respCode: http.StatusOK,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydstypes GET w primary_ds_type wildcard param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params: url.Values{
					"primary_ds_type": []string{"t*"},
				},
				handler:  web.PrimaryDSTypesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydstypes GET w dataset param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					primaryDSTypeResp,
				},
				params: url.Values{
					"dataset": []string{"unittest"},
				},
				handler:  web.PrimaryDSTypesHandler,
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydstypes GET w bad dataset param",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output: []Response{
					errorResp,
				},
				params: url.Values{
					"dataset": []string{"fnal"},
				},
				respCode: http.StatusBadRequest,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydstypes GET w different params",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/primarydstypes",
				input:       nil,
				output:      []Response{},
				params: url.Values{
					"primary_ds_type": []string{"A*"},
				},
				respCode: http.StatusOK,
				handler:  web.PrimaryDSTypesHandler,
			},
			{
				description: "Test primarydataset POST duplicate",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSReq,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after duplicate POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test primarydataset second POST",
				method:      "POST",
				serverType:  "DBSWriter",
				params:      nil,
				input:       primaryDSReq2,
				output:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test primarydatasets GET after second POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					primaryDSResp,
					primaryDSResp2,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// outputconfigs endpoint tests
// TODO: Rest of test cases
func getOutputConfigTestTable(t *testing.T) EndpointTestCase {
	// outputConfigResponse is the expected outputconfigs GET response
	type outputConfigResponse struct {
		APP_NAME            string `json:"app_name"`
		RELEASE_VERSION     string `json:"release_version"`
		PSET_HASH           string `json:"pset_hash"`
		PSET_NAME           string `json:"pset_name"`
		GLOBAL_TAG          string `json:"global_tag"`
		OUTPUT_MODULE_LABEL string `json:"output_module_label"`
		CREATION_DATE       int64  `json:"creation_date"`
		CREATE_BY           string `json:"create_by"`
	}

	outputConfigReq := dbs.OutputConfigRecord{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           "tester",
		SCENARIO:            "note",
	}
	outputConfigResp := outputConfigResponse{
		APP_NAME:            TestData.AppName,
		RELEASE_VERSION:     TestData.ReleaseVersion,
		PSET_HASH:           TestData.PsetHash,
		GLOBAL_TAG:          TestData.GlobalTag,
		OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
		CREATE_BY:           "tester",
		CREATION_DATE:       0,
	}
	return EndpointTestCase{
		description:     "Test outputconfigs",
		defaultHandler:  web.OutputConfigsHandler,
		defaultEndpoint: "/dbs/outputconfigs",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				input:       nil,
				params:      nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test bad POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input: BadRequest{
					BAD_FIELD: "bad",
				},
				params:   nil,
				output:   nil,
				respCode: http.StatusBadRequest,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigReq,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test duplicate POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       outputConfigReq,
				output:      nil,
				params:      nil,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					outputConfigResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
		},
	}
}

// acquisitioneras endpoint tests
// TODO: Rest of test cases
func getAcquisitionErasTestTable(t *testing.T) EndpointTestCase {
	acqEraReq := dbs.AcquisitionEras{
		ACQUISITION_ERA_NAME: TestData.AcquisitionEra,
		DESCRIPTION:          "note",
		CREATE_BY:            "tester",
	}
	acqEraResp := dbs.AcquisitionEra{
		AcquisitionEraName: TestData.AcquisitionEra,
		StartDate:          0,
		EndDate:            0,
		CreationDate:       0,
		CreateBy:           "tester",
		Description:        "note",
	}
	return EndpointTestCase{
		description:     "Test acquisitioneras",
		defaultHandler:  web.AcquisitionErasHandler,
		defaultEndpoint: "/dbs/acquisitioneras",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       acqEraReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					acqEraResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// processingeras endpoint tests
// TODO: Rest of test cases
func getProcessingErasTestTable(t *testing.T) EndpointTestCase {
	procErasReq := dbs.ProcessingEras{
		PROCESSING_VERSION: int64(TestData.ProcessingVersion),
		DESCRIPTION:        "this_is_a_test",
		CREATE_BY:          "tester",
	}
	procErasResp := dbs.ProcessingEra{
		ProcessingVersion: int64(TestData.ProcessingVersion),
		CreateBy:          "tester",
		Description:       "this_is_a_test",
		CreationDate:      0,
	}
	return EndpointTestCase{
		description:     "Test processingeras",
		defaultHandler:  web.ProcessingErasHandler,
		defaultEndpoint: "/dbs/processingeras",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				input:       nil,
				params:      nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       procErasReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					procErasResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// datatiers endpoint tests
func getDatatiersTestTable(t *testing.T) EndpointTestCase {
	tiersReq := dbs.DataTiers{
		DATA_TIER_NAME: TestData.Tier,
		CREATE_BY:      "tester",
	}
	tiersResp := dbs.DataTiers{
		DATA_TIER_ID:   1,
		DATA_TIER_NAME: TestData.Tier,
		CREATE_BY:      "tester",
		CREATION_DATE:  0,
	}
	badReq := BadRequest{
		BAD_FIELD: "BAD",
	}
	return EndpointTestCase{
		description:     "Test datatiers",
		defaultHandler:  web.DatatiersHandler,
		defaultEndpoint: "/dbs/datatiers",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				input:       nil,
				params:      nil,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test bad POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       badReq,
				params:      nil,
				respCode:    http.StatusBadRequest,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       tiersReq,
				output: []Response{
					tiersResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					tiersResp,
				},
				params:   nil,
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with parameters",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"data_tier_name": []string{
						TestData.Tier,
					},
				},
				output: []Response{
					tiersResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with regex parameter",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"data_tier_name": []string{"G*"},
				},
				output: []Response{
					tiersResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with non-existing parameter value",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				params: url.Values{
					"data_tier_name": []string{"A*"},
				},
				respCode: http.StatusOK,
			},
		},
	}

}

// datasetaccesstypes endpoint tests
func getDatasetAccessTypesTestTable(t *testing.T) EndpointTestCase {
	type datasetAccessTypeResponse struct {
		DATASET_ACCESS_TYPE string `json:"dataset_access_type"`
	}

	dataATreq := dbs.DatasetAccessTypes{
		DATASET_ACCESS_TYPE: "PRODUCTION",
	}
	dataATresp := datasetAccessTypeResponse{
		DATASET_ACCESS_TYPE: "PRODUCTION",
	}
	return EndpointTestCase{
		description:     "Test datasetaccesstypes",
		defaultHandler:  web.DatasetAccessTypesHandler,
		defaultEndpoint: "/dbs/datasetaccesstypes",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       dataATreq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				method:      "GET",
				serverType:  "DBSReader",
				output: []Response{
					dataATresp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// contains tests for the physicsgroups endpoint
func getPhysicsGroupsTestTable(t *testing.T) EndpointTestCase {
	type physicsGroupsResponse struct {
		PHYSICS_GROUP_NAME string `json:"physics_group_name"`
	}
	physGroupReq := dbs.PhysicsGroups{
		PHYSICS_GROUP_NAME: "Tracker",
	}
	physGroupResp := physicsGroupsResponse{
		PHYSICS_GROUP_NAME: "Tracker",
	}
	return EndpointTestCase{
		description:     "Test physicsgroups",
		defaultHandler:  web.PhysicsGroupsHandler,
		defaultEndpoint: "/dbs/physicsgroups",
		testCases: []testCase{
			{
				description: "Test GET with no data",
				serverType:  "DBSReader",
				method:      "GET",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       physGroupReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				serverType:  "DBSReader",
				method:      "GET",
				output: []Response{
					physGroupResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// datasets endpoint tests
//* Note: depends on above tests for their *_id
// TODO: include prep_id in POST tests
func getDatasetsTestTable(t *testing.T) EndpointTestCase {
	type datasetRequest struct {
		DATASET                string                   `json:"dataset" validate:"required"`
		PRIMARY_DS_NAME        string                   `json:"primary_ds_name" validate:"required"`
		PRIMARY_DS_TYPE        string                   `json:"primary_ds_type" validate:"required"`
		PROCESSED_DS_NAME      string                   `json:"processed_ds_name" validate:"required"`
		DATA_TIER_NAME         string                   `json:"data_tier_name" validate:"required"`
		ACQUISITION_ERA_NAME   string                   `json:"acquisition_era_name" validate:"required"`
		DATASET_ACCESS_TYPE    string                   `json:"dataset_access_type" validate:"required"`
		PROCESSING_VERSION     int64                    `json:"processing_version" validate:"required,number,gt=0"`
		OUTPUT_CONFIGS         []dbs.OutputConfigRecord `json:"output_configs"`
		PHYSICS_GROUP_NAME     string                   `json:"physics_group_name" validate:"required"`
		XTCROSSSECTION         float64                  `json:"xtcrosssection" validate:"required,number"`
		CREATION_DATE          int64                    `json:"creation_date" validate:"required,number,gt=0"`
		CREATE_BY              string                   `json:"create_by" validate:"required"`
		LAST_MODIFICATION_DATE int64                    `json:"last_modification_date" validate:"required,number,gt=0"`
		LAST_MODIFIED_BY       string                   `json:"last_modified_by" validate:"required"`
	}
	type datasetsResponse struct {
		DATASET string `json:"dataset"`
	}
	type datasetsDetailResponse struct {
		DATASET_ID             int64  `json:"dataset_id"`
		PHYSICS_GROUP_NAME     string `json:"physics_group_name"`
		DATASET                string `json:"dataset"`
		DATASET_ACCESS_TYPE    string `json:"dataset_access_type"`
		PROCESSED_DS_NAME      string `json:"processed_ds_name"`
		PREP_ID                string `json:"prep_id"`
		PRIMARY_DS_NAME        string `json:"primary_ds_name"`
		XTCROSSSECTION         int64  `json:"xtcrosssection"`
		DATA_TIER_NAME         string `json:"data_tier_name"`
		PRIMARY_DS_TYPE        string `json:"primary_ds_type"`
		CREATION_DATE          int64  `json:"creation_date"`
		CREATE_BY              string `json:"create_by"`
		LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
		LAST_MODIFIED_BY       string `json:"last_modified_by"`
		PROCESSING_VERSION     int64  `json:"processing_version"`
		ACQUISITION_ERA_NAME   string `json:"acquisition_era_name"`
	}
	outputConfs := []dbs.OutputConfigRecord{
		{
			RELEASE_VERSION:     TestData.ReleaseVersion,
			PSET_HASH:           TestData.PsetHash,
			APP_NAME:            TestData.AppName,
			OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
			GLOBAL_TAG:          TestData.GlobalTag,
		},
	}
	datasetReq := datasetRequest{
		PHYSICS_GROUP_NAME:     "Tracker",
		DATASET:                TestData.Dataset,
		DATASET_ACCESS_TYPE:    "PRODUCTION",
		PROCESSED_DS_NAME:      TestData.ProcDataset,
		PRIMARY_DS_NAME:        TestData.PrimaryDSName,
		XTCROSSSECTION:         123,
		DATA_TIER_NAME:         TestData.Tier,
		PRIMARY_DS_TYPE:        "test",
		OUTPUT_CONFIGS:         outputConfs,
		CREATION_DATE:          1635177605,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: 1635177605,
		LAST_MODIFIED_BY:       "testuser",
		PROCESSING_VERSION:     TestData.ProcessingVersion,
		ACQUISITION_ERA_NAME:   TestData.AcquisitionEra,
	}
	parentDatasetReq := datasetRequest{
		PHYSICS_GROUP_NAME:     "Tracker",
		DATASET:                TestData.ParentDataset,
		DATASET_ACCESS_TYPE:    "PRODUCTION",
		PROCESSED_DS_NAME:      TestData.ParentProcDataset,
		PRIMARY_DS_NAME:        TestData.PrimaryDSName,
		XTCROSSSECTION:         123,
		DATA_TIER_NAME:         TestData.Tier,
		PRIMARY_DS_TYPE:        "test",
		OUTPUT_CONFIGS:         outputConfs,
		CREATION_DATE:          1635177605,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: 1635177605,
		LAST_MODIFIED_BY:       "testuser",
		PROCESSING_VERSION:     TestData.ProcessingVersion,
		ACQUISITION_ERA_NAME:   TestData.AcquisitionEra,
	}
	datasetResp := datasetsResponse{
		DATASET: TestData.Dataset,
	}
	datasetDetailResp := datasetsDetailResponse{
		DATASET_ID:             1.0,
		PHYSICS_GROUP_NAME:     "Tracker",
		DATASET:                TestData.Dataset,
		DATASET_ACCESS_TYPE:    "PRODUCTION",
		PROCESSED_DS_NAME:      TestData.ProcDataset,
		PREP_ID:                "",
		PRIMARY_DS_NAME:        TestData.PrimaryDSName,
		XTCROSSSECTION:         123,
		DATA_TIER_NAME:         TestData.Tier,
		PRIMARY_DS_TYPE:        "test",
		CREATION_DATE:          1635177605,
		CREATE_BY:              "tester",
		LAST_MODIFICATION_DATE: 1635177605,
		LAST_MODIFIED_BY:       "testuser",
		PROCESSING_VERSION:     TestData.ProcessingVersion,
		ACQUISITION_ERA_NAME:   TestData.AcquisitionEra,
	}
	return EndpointTestCase{
		description:     "Test datasets",
		defaultHandler:  web.DatasetsHandler,
		defaultEndpoint: "/dbs/datasets",
		testCases: []testCase{
			{
				description: "Test empty GET",
				method:      "GET",
				serverType:  "DBSReader",
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test POST",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       datasetReq,
				output: []Response{
					datasetResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST parent dataset",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       parentDatasetReq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET after POST",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{TestData.Dataset},
					"dataset_access_type": []string{"PRODUCTION"},
				},
				output: []Response{
					datasetResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST with detail",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{TestData.Dataset},
					"dataset_access_type": []string{"PRODUCTION"},
					"detail":              []string{"true"},
				},
				output: []Response{
					datasetDetailResp,
				},
				respCode: http.StatusOK,
			},
		},
	}

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
		generateBaseData(t)
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
