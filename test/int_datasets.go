package main

// this file contains logic for datasets API
// the HTTP requests body is defined by dbs.DatasetRecord struct defined in dbs/datasets.go
// the HTTP response body is defined by datasetsResponse struct defined in this file
// the HTTP response body for the `detail` query is defined by datasetsDetailResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// struct for datasets GET response
type datasetsResponse struct {
	DATASET string `json:"dataset"`
}

// struct for datasets GET response with parent_dataset parameter
type datasetsWithParentsResponse struct {
	DATASET        string `json:"dataset"`
	PARENT_DATASET string `json:"parent_dataset"`
}

// struct for datasets GET response with detail=true query parameter
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

// struct for datasets GET response with detail and when tmpl["Version"] is true
type datasetsDetailVersionResponse struct {
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
	OUTPUT_MODULE_LABEL    string `json:"output_module_label"`
	GLOBAL_TAG             string `json:"global_tag"`
	RELEASE_VERSION        string `json:"release_version"`
	PSET_HASH              string `json:"pset_hash"`
	APP_NAME               string `json:"app_name"`
}

// creates a dataset request
func createDSRequest(dataset string, procdataset string, dsType string, physGroup string, outputConfs []dbs.OutputConfigRecord) dbs.DatasetRecord {
	return dbs.DatasetRecord{
		PHYSICS_GROUP_NAME:  physGroup,
		DATASET:             dataset,
		DATASET_ACCESS_TYPE: dsType,
		PROCESSED_DS_NAME:   procdataset,
		PRIMARY_DS_NAME:     TestData.PrimaryDSName,
		XTCROSSSECTION:      123,
		DATA_TIER_NAME:      TestData.Tier,
		// PRIMARY_DS_TYPE:        TestData.PrimaryDSType,
		OUTPUT_CONFIGS:         outputConfs,
		CREATION_DATE:          1635177605,
		CREATE_BY:              TestData.CreateBy,
		LAST_MODIFICATION_DATE: 1635177605,
		LAST_MODIFIED_BY:       TestData.CreateBy,
		PROCESSING_VERSION:     TestData.ProcessingVersion,
		ACQUISITION_ERA_NAME:   TestData.AcquisitionEra,
	}
}

// creates a basic datasets response
func createDSResponse(dataset string) datasetsResponse {
	return datasetsResponse{
		DATASET: dataset,
	}
}

// creates a detailed datasets response
func createDetailDSResponse(datasetID int64, dataset string, procdataset string, dsType string, physicsGroupName string) datasetsDetailResponse {
	return datasetsDetailResponse{
		DATASET_ID:             datasetID,
		PHYSICS_GROUP_NAME:     physicsGroupName,
		DATASET:                dataset,
		DATASET_ACCESS_TYPE:    dsType,
		PROCESSED_DS_NAME:      procdataset,
		PREP_ID:                "",
		PRIMARY_DS_NAME:        TestData.PrimaryDSName,
		XTCROSSSECTION:         123,
		DATA_TIER_NAME:         TestData.Tier,
		PRIMARY_DS_TYPE:        TestData.PrimaryDSType,
		CREATION_DATE:          1635177605,
		CREATE_BY:              TestData.CreateBy,
		LAST_MODIFICATION_DATE: 1635177605,
		LAST_MODIFIED_BY:       TestData.CreateBy,
		PROCESSING_VERSION:     TestData.ProcessingVersion,
		ACQUISITION_ERA_NAME:   TestData.AcquisitionEra,
	}
}

// creates a detailed datasets response for params using output_mod_config values
func createDetailVersionDSResponse(datasetID int64, dataset string, procdataset string, dsType string) datasetsDetailVersionResponse {
	return datasetsDetailVersionResponse{
		DATASET_ID:             datasetID,
		PHYSICS_GROUP_NAME:     TestData.PhysicsGroupName,
		DATASET:                dataset,
		DATASET_ACCESS_TYPE:    dsType,
		PROCESSED_DS_NAME:      procdataset,
		PREP_ID:                "",
		PRIMARY_DS_NAME:        TestData.PrimaryDSName,
		XTCROSSSECTION:         123,
		DATA_TIER_NAME:         TestData.Tier,
		PRIMARY_DS_TYPE:        TestData.PrimaryDSType,
		CREATION_DATE:          1635177605,
		CREATE_BY:              TestData.CreateBy,
		LAST_MODIFICATION_DATE: 1635177605,
		LAST_MODIFIED_BY:       TestData.CreateBy,
		PROCESSING_VERSION:     TestData.ProcessingVersion,
		ACQUISITION_ERA_NAME:   TestData.AcquisitionEra,
		OUTPUT_MODULE_LABEL:    TestData.OutputModuleLabel,
		GLOBAL_TAG:             TestData.GlobalTag,
		RELEASE_VERSION:        TestData.ReleaseVersion,
		PSET_HASH:              TestData.PsetHash,
		APP_NAME:               TestData.AppName,
	}
}

// datasets endpoint tests
// * Note: depends on above tests for their *_id
// TODO: include prep_id in POST tests
// TODO: DBSClientWriter_t.test11
func getDatasetsTestTable(t *testing.T) EndpointTestCase {
	outputConfs := []dbs.OutputConfigRecord{
		{
			RELEASE_VERSION:     TestData.ReleaseVersion,
			PSET_HASH:           TestData.PsetHash,
			APP_NAME:            TestData.AppName,
			OUTPUT_MODULE_LABEL: TestData.OutputModuleLabel,
			GLOBAL_TAG:          TestData.GlobalTag,
		},
	}
	dsReq := createDSRequest(TestData.Dataset, TestData.ProcDataset, TestData.DatasetAccessType, TestData.PhysicsGroupName, outputConfs)
	dsParentReq := createDSRequest(TestData.ParentDataset, TestData.ParentProcDataset, TestData.DatasetAccessType, TestData.PhysicsGroupName, outputConfs)

	// record without output_configs
	noOMCReq := createDSRequest(TestData.Dataset, TestData.ProcDataset, TestData.DatasetAccessType, TestData.PhysicsGroupName, []dbs.OutputConfigRecord{})

	// alternative access type request
	dsAccessTypeReq := createDSRequest(TestData.Dataset2, TestData.ProcDataset, "PRODUCTION", TestData.PhysicsGroupName, outputConfs)

	// basic responses
	dsResp := createDSResponse(TestData.Dataset)
	dsParentResp := createDSResponse(TestData.ParentDataset)
	dsAccessTypeResp := createDSResponse(TestData.Dataset2)

	// detail responses
	dsDetailResp := createDetailDSResponse(1, TestData.Dataset, TestData.ProcDataset, TestData.DatasetAccessType, TestData.PhysicsGroupName)

	// detail responses for output_config parameters
	dsDetailVersResp := createDetailVersionDSResponse(1, TestData.Dataset, TestData.ProcDataset, TestData.DatasetAccessType)
	dsParentDetailVersResp := createDetailVersionDSResponse(2, TestData.ParentDataset, TestData.ParentProcDataset, TestData.DatasetAccessType)
	a := strings.Split(TestData.Dataset, "/")
	dsQuery := fmt.Sprintf("/*%s/%s/%s", a[1], a[2], a[3])
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
				description: "Test POST", // DBSClientWriter_t.test08
				method:      "POST",
				serverType:  "DBSWriter",
				input:       dsReq,
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST parent dataset", // DBSClientWriter_t.test08
				method:      "POST",
				serverType:  "DBSWriter",
				input:       dsParentReq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Test GET with no params", // DBSClientReader_t.test005
				serverType:  "DBSReader",
				method:      "GET",
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET initial dataset", // DBSClientReader_t.test006a
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST with detail", // DBSClientReader_t.test006b
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
					"detail":  []string{"true"},
				},
				output: []Response{
					dsDetailResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST with detail and ds access type wildcard", // DBSClientReader_t.test006b.2
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_id":          []string{"1"},
					"detail":              []string{"true"},
					"dataset_access_type": []string{"*"},
				},
				output: []Response{
					dsDetailResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET after POST ds access type wildcard ", // DBSClientReader_t.test006c
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_id":          []string{"1"},
					"dataset_access_type": []string{"*"},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET parent dataset",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.ParentDataset},
				},
				output: []Response{
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset wildcard", // DBSClientReader_t.test007
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset + "*"},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset all wildcard", // DBSClientReader_t.test007a
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{"/*/*/*"},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset first wildcard", // DBSClientReader_t.test007b
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{dsQuery},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset list",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset, TestData.ParentDataset},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test duplicate POST", // DBSClientWriter_t.test09
				method:      "POST",
				serverType:  "DBSWriter",
				input:       dsReq,
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with release_version param", // DBSClientReader_t.test008
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"release_version": []string{TestData.ReleaseVersion},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with pset_hash param", // DBSClientReader_t.test009
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"pset_hash": []string{TestData.PsetHash},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with app_name param", // DBSClientReader_t.test010
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"app_name": []string{TestData.AppName},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with output_module_label param", // DBSClientReader_t.test011
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with multiple params", // DBSClientReader_t.test012
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"release_version":     []string{TestData.ReleaseVersion},
					"pset_hash":           []string{TestData.PsetHash},
					"app_name":            []string{TestData.AppName},
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with multiple params including dataset", // DBSClientReader_t.test013
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{TestData.Dataset},
					"release_version":     []string{TestData.ReleaseVersion},
					"pset_hash":           []string{TestData.PsetHash},
					"app_name":            []string{TestData.AppName},
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with dataset and release_version", // DBSClientReader_t.test014
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":         []string{TestData.Dataset},
					"release_version": []string{TestData.ReleaseVersion},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with create_by", // DBSClientReader_t.test014a
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"create_by": []string{TestData.CreateBy},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with last_modified_by", // DBSClientReader_t.test014b
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"last_modified_by": []string{TestData.CreateBy},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with version and detail parameter",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"detail":              []string{"true"},
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					dsDetailVersResp,
					dsParentDetailVersResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST with no output_config", // DBSClientWriter_t.test11
				serverType:  "DBSWriter",
				method:      "POST",
				input:       noOMCReq,
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST with different dataset_access_type",
				serverType:  "DBSWriter",
				method:      "POST",
				input:       dsAccessTypeReq,
				output: []Response{
					dsResp, // change with actual response
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET for new dataset_access_type",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_access_type": []string{TestData.DatasetAccessType2},
				},
				output: []Response{
					dsAccessTypeResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET ensure only VALID dstype",
				serverType:  "DBSReader",
				method:      "GET",
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test is_dataset_valid true",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"is_dataset_valid": []string{"1"},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test is_dataset_valid false",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"is_dataset_valid": []string{"0"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test is_dataset_valid none",
				serverType:  "DBSReader",
				method:      "GET",
				params:      url.Values{},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

// second datasets endpoint tests for after files integration tests
func getDatasetsTestTable2(t *testing.T) EndpointTestCase {
	dsChildrenResp := datasetsWithParentsResponse{
		DATASET:        TestData.Dataset,
		PARENT_DATASET: TestData.ParentDataset,
	}
	dsResp := createDSResponse(TestData.Dataset)
	dsParentResp := createDSResponse(TestData.ParentDataset)
	dsDetailResp := createDetailDSResponse(1, TestData.Dataset, TestData.ProcDataset, TestData.DatasetAccessType, TestData.PhysicsGroupName)
	dsDetailParentResp := createDetailDSResponse(2, TestData.ParentDataset, TestData.ParentProcDataset, TestData.DatasetAccessType, TestData.PhysicsGroupName)
	runs := strings.ReplaceAll(fmt.Sprint(TestData.Runs), " ", ",")

	datasetsParamErr := dbs.CreateInvalidParamError("fnal", "datasets")
	hrec := createHTTPError("GET", "/dbs/datasets?fnal=cern")
	errorResp := createServerErrorResponse(hrec, datasetsParamErr)

	return EndpointTestCase{
		description:     "Test datasets 2",
		defaultHandler:  web.DatasetsHandler,
		defaultEndpoint: "/dbs/datasets",
		testCases: []testCase{
			{
				description: "Test GET with dataset runs and detail", // DBSClientReader_t.test014c
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
					"run_num": []string{runs},
					"detail":  []string{"true"},
				},
				output: []Response{
					dsDetailResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with primary_ds_name with * and detail", // DBSClientReader_t.test014d
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"primary_ds_name": []string{TestData.PrimaryDSName + "*"},
					"detail":          []string{"true"},
				},
				output: []Response{
					dsDetailResp,
					dsDetailParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with primary_ds_name and detail", // DBSClientReader_t.test014e
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"primary_ds_name": []string{TestData.PrimaryDSName},
					"detail":          []string{"true"},
				},
				output: []Response{
					dsDetailResp,
					dsDetailParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with primary_ds_name with *", // DBSClientReader_t.test014f
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"primary_ds_name": []string{TestData.PrimaryDSName + "*"},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with primary_ds_name", // DBSClientReader_t.test014g
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"primary_ds_name": []string{TestData.PrimaryDSName},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with processed_ds_name with *", // DBSClientReader_t.test014h
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"processed_ds_name": []string{TestData.ProcDataset + "*"},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with processed_ds_name", // DBSClientReader_t.test014i
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"processed_ds_name": []string{TestData.ProcDataset},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with processed_ds_name with *, detail true", // DBSClientReader_t.test014j
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"processed_ds_name": []string{TestData.ProcDataset + "*"},
					"detail":            []string{"true"},
				},
				output: []Response{
					dsDetailResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with processed_ds_name, detail true", // DBSClientReader_t.test014k
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"processed_ds_name": []string{TestData.ProcDataset},
					"detail":            []string{"true"},
				},
				output: []Response{
					dsDetailResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with data_tier_name with *", // DBSClientReader_t.test014n
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"data_tier_name": []string{TestData.Tier + "*"},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with data_tier_name", // DBSClientReader_t.test014o
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"data_tier_name": []string{TestData.Tier},
				},
				output: []Response{
					dsResp,
					dsParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with data_tier_name with *, detail true", // DBSClientReader_t.test014l
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"data_tier_name": []string{TestData.Tier + "*"},
					"detail":         []string{"true"},
				},
				output: []Response{
					dsDetailResp,
					dsDetailParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with data_tier_name, detail true", // DBSClientReader_t.test014m
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"data_tier_name": []string{TestData.Tier},
					"detail":         []string{"true"},
				},
				output: []Response{
					dsDetailResp,
					dsDetailParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET dataset children", // DBSClientReader_t.test072
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"parent_dataset": []string{TestData.ParentDataset},
				},
				output: []Response{
					dsChildrenResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET dataset invalid parameter key",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"fnal": []string{"cern"},
				},
				output: []Response{
					errorResp,
				},
				respCode: http.StatusBadRequest,
			},
		},
	}
}

// struct for a datasets update request body
type datasetsUpdateRequest struct {
	DATASET             string `json:"dataset"`
	DATASET_ACCESS_TYPE string `json:"dataset_access_type"`
}

// struct for datasets update request physics_group
type datasetsPhysicsGroupUpdateRequest struct {
	DATASET            string `json:"dataset"`
	PHYSICS_GROUP_NAME string `json:"physics_group_name"`
}

// third datasets endpoint tests for update datasets
func getDatasetsTestTable3(t *testing.T) EndpointTestCase {
	// basic responses
	dsResp := createDSResponse(TestData.Dataset)
	// dsParentResp := createDSResponse(TestData.ParentDataset)

	// detail responses
	// dsDetailResp := createDetailDSResponse(1, TestData.Dataset, TestData.ProcDataset, TestData.DatasetAccessType)

	// setting dsResp to PRODUCTION
	dsUpdateReq := datasetsUpdateRequest{
		DATASET:             TestData.Dataset,
		DATASET_ACCESS_TYPE: TestData.DatasetAccessType2,
	}
	return EndpointTestCase{
		description:     "Test datasets update",
		defaultHandler:  web.DatasetsHandler,
		defaultEndpoint: "/dbs/datasets",
		testCases: []testCase{
			{
				description: "Check dataset to be updated",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test PUT update dataset type", // DBSClientWriter_t.test20
				serverType:  "DBSWriter",
				method:      "PUT",
				input:       dsUpdateReq,
				output:      []Response{},
				respCode:    http.StatusOK,
			},
			{
				description: "Ensure update removes dataset valid",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Check dataset access type is PRODUCTION",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"is_dataset_valid":    []string{"0"},
					"dataset_access_type": []string{"PRODUCTION"},
				},
				output: []Response{
					dsResp,
				},
				respCode: http.StatusOK,
			},
		},
	}
}

type datasetParentResponse struct {
	PARENT_DATASET string `json:"parent_dataset"`
	PARENT_DS_ID   int    `json:"parent_dataset_id"`
	THIS_DATASET   string `json:"this_dataset"`
}

// datasetparents test cases
func getDatasetParentsTestTable(t *testing.T) EndpointTestCase {
	dsParentsResp := datasetParentResponse{
		PARENT_DATASET: TestData.ParentDataset,
		PARENT_DS_ID:   2,
		THIS_DATASET:   TestData.Dataset,
	}
	datasetsParamErr := dbs.CreateInvalidParamError("fnal", "datasetparents")
	hrec := createHTTPError("GET", "/dbs/datasetparents?fnal=cern")
	errorResp := createServerErrorResponse(hrec, datasetsParamErr)

	return EndpointTestCase{
		description:     "Test datasetparents",
		defaultHandler:  web.DatasetParentsHandler,
		defaultEndpoint: "/dbs/datasetparents",
		testCases: []testCase{
			{
				description: "Test GET", // DBSClientReader_t.test030
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{TestData.Dataset},
				},
				output:   []Response{dsParentsResp},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET for non-existing", // DBSClientReader_t.test031
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{"/does/not/EXIST"},
				},
				output:   []Response{},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET invalid parameter key",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"fnal": []string{"cern"},
				},
				output: []Response{
					errorResp,
				},
				respCode: http.StatusBadRequest,
			},
		},
	}
}

// test updating physics group name
func getDatasetPhysicsGroupUpdateTestTable(t *testing.T) EndpointTestCase {
	physicsGroup1 := dbs.PhysicsGroups{
		PHYSICS_GROUP_NAME: "PleaseChangeMe",
	}
	physicsGroup2 := dbs.PhysicsGroups{
		PHYSICS_GROUP_NAME: "PickMe",
	}
	physicsGroup1Resp := physicsGroupsResponse{
		PHYSICS_GROUP_NAME: "PleaseChangeMe",
	}
	physicsGroup2Resp := physicsGroupsResponse{
		PHYSICS_GROUP_NAME: "PickMe",
	}
	datasetName := "/unittest_web_primary_ds_name_8268/acq_era_8268-v8268/GEN-SIM-UPDATE-RAW"
	dsReq := createDSRequest(datasetName, TestData.ProcDataset, TestData.DatasetAccessType, physicsGroup1.PHYSICS_GROUP_NAME, []dbs.OutputConfigRecord{})
	dsUpdateReq := datasetsPhysicsGroupUpdateRequest{
		DATASET:            datasetName,
		PHYSICS_GROUP_NAME: "BadN@me",
	}
	dsUpdateReq2 := datasetsPhysicsGroupUpdateRequest{
		DATASET:            datasetName,
		PHYSICS_GROUP_NAME: physicsGroup2.PHYSICS_GROUP_NAME,
	}
	dsResp1 := createDetailDSResponse(9, datasetName, TestData.ProcDataset, TestData.DatasetAccessType, physicsGroup1.PHYSICS_GROUP_NAME)
	dsResp2 := createDetailDSResponse(9, datasetName, TestData.ProcDataset, TestData.DatasetAccessType, physicsGroup2.PHYSICS_GROUP_NAME)
	dsResp2.LAST_MODIFIED_BY = "DBS-workflow"
	return EndpointTestCase{
		description:     "Test dataset with physics group renaming update",
		defaultHandler:  web.DatasetsHandler,
		defaultEndpoint: "/dbs/datasets",
		testCases: []testCase{
			{
				description: "Add physics group",
				method:      "POST",
				serverType:  "DBSWriter",
				endpoint:    "/dbs/physicsgroups",
				handler:     web.PhysicsGroupsHandler,
				input:       physicsGroup1,
				respCode:    http.StatusOK,
			},
			{
				description: "Check if physics group was added",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/physicsgroups",
				handler:     web.PhysicsGroupsHandler,
				params: url.Values{
					"physics_group_name": []string{physicsGroup1.PHYSICS_GROUP_NAME},
				},
				output: []Response{
					physicsGroup1Resp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Add dataset",
				method:      "POST",
				serverType:  "DBSWriter",
				input:       dsReq,
				respCode:    http.StatusOK,
			},
			{
				description: "Verify dataset",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{datasetName},
					"detail":  []string{"true"},
				},
				output: []Response{
					dsResp1,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Update dataset without adding physics_group",
				method:      "PUT",
				serverType:  "DBSWriter",
				input:       dsUpdateReq,
				respCode:    http.StatusBadRequest,
			},
			{
				description: "Add second physics group",
				method:      "POST",
				serverType:  "DBSWriter",
				endpoint:    "/dbs/physicsgroups",
				handler:     web.PhysicsGroupsHandler,
				input:       physicsGroup2,
				respCode:    http.StatusOK,
			},
			{
				description: "Check if physics group was added",
				method:      "GET",
				serverType:  "DBSReader",
				endpoint:    "/dbs/physicsgroups",
				handler:     web.PhysicsGroupsHandler,
				params: url.Values{
					"physics_group_name": []string{physicsGroup2.PHYSICS_GROUP_NAME},
				},
				output: []Response{
					physicsGroup2Resp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Update dataset with an invalid physics_group",
				method:      "PUT",
				serverType:  "DBSWriter",
				input:       dsUpdateReq,
				respCode:    http.StatusBadRequest,
			},
			{
				description: "Update dataset with new physics_group",
				method:      "PUT",
				serverType:  "DBSWriter",
				input:       dsUpdateReq2,
				respCode:    http.StatusOK,
			},
			{
				description: "Verify updated dataset",
				method:      "GET",
				serverType:  "DBSReader",
				params: url.Values{
					"dataset": []string{datasetName},
					"detail":  []string{"true"},
				},
				output: []Response{
					dsResp2,
				},
				respCode: http.StatusOK,
			},
		},
	}
}
