package main

// this file contains logic for datasets API
// the HTTP requests body is defined by datasetsRequest struct defined in this file
// the HTTP response body is defined by datasetsResponse struct defined in this file
// the HTTP response body for the `detail` query is defined by datasetsDetailResponse struct defined in this file
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// struct for datasets POST request body
type datasetsRequest struct {
	DATASET                string                   `json:"dataset" validate:"required"`
	PRIMARY_DS_NAME        string                   `json:"primary_ds_name" validate:"required"`
	PRIMARY_DS_TYPE        string                   `json:"primary_ds_type" validate:"required"`
	PROCESSED_DS_NAME      string                   `json:"processed_ds_name" validate:"required"`
	DATA_TIER_NAME         string                   `json:"data_tier_name" validate:"required"`
	ACQUISITION_ERA_NAME   string                   `json:"acquisition_era_name" validate:"required"`
	DATASET_ACCESS_TYPE    string                   `json:"dataset_access_type" validate:"required"`
	PROCESSING_VERSION     int64                    `json:"processing_version" validate:"required,number,gt=0"`
	PHYSICS_GROUP_NAME     string                   `json:"physics_group_name" validate:"required"`
	XTCROSSSECTION         float64                  `json:"xtcrosssection" validate:"required,number"`
	CREATION_DATE          int64                    `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY              string                   `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64                    `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string                   `json:"last_modified_by" validate:"required"`
	OUTPUT_CONFIGS         []dbs.OutputConfigRecord `json:"output_configs"`
}

// struct for datasets GET response
type datasetsResponse struct {
	DATASET string `json:"dataset"`
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

// datasets endpoint tests
//* Note: depends on above tests for their *_id
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
	datasetReq := dbs.DatasetRecord{
		PHYSICS_GROUP_NAME:  TestData.PhysicsGroupName,
		DATASET:             TestData.Dataset,
		DATASET_ACCESS_TYPE: TestData.DatasetAccessType,
		PROCESSED_DS_NAME:   TestData.ProcDataset,
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
	parentDatasetReq := dbs.DatasetRecord{
		PHYSICS_GROUP_NAME:  TestData.PhysicsGroupName,
		DATASET:             TestData.ParentDataset,
		DATASET_ACCESS_TYPE: TestData.DatasetAccessType,
		PROCESSED_DS_NAME:   TestData.ParentProcDataset,
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
	datasetResp := datasetsResponse{
		DATASET: TestData.Dataset,
	}
	datasetParentResp := datasetsResponse{
		DATASET: TestData.ParentDataset,
	}
	datasetDetailResp := datasetsDetailResponse{
		DATASET_ID:             1.0,
		PHYSICS_GROUP_NAME:     TestData.PhysicsGroupName,
		DATASET:                TestData.Dataset,
		DATASET_ACCESS_TYPE:    TestData.DatasetAccessType,
		PROCESSED_DS_NAME:      TestData.ProcDataset,
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
	/*
		datasetDetailVersResp := datasetsDetailVersionResponse{
			DATASET_ID:             1.0,
			PHYSICS_GROUP_NAME:     TestData.PhysicsGroupName,
			DATASET:                TestData.Dataset,
			DATASET_ACCESS_TYPE:    TestData.DatasetAccessType,
			PROCESSED_DS_NAME:      TestData.ProcDataset,
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
	*/
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
				input:       datasetReq,
				output: []Response{
					datasetResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test POST parent dataset", // DBSClientWriter_t.test08
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
				description: "Test GET to confirm parent dataset",
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset":             []string{TestData.Dataset, TestData.ParentDataset},
					"dataset_access_type": []string{"PRODUCTION"},
				},
				output: []Response{
					datasetResp,
					datasetParentResp,
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
			{
				description: "Test duplicate POST", // DBSClientWriter_t.test09
				method:      "POST",
				serverType:  "DBSWriter",
				input:       datasetReq,
				output: []Response{
					datasetResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with release_version param", // DBSClientReader_t.test008
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_access_type": []string{TestData.DatasetAccessType},
					"release_version":     []string{TestData.ReleaseVersion},
				},
				output: []Response{
					datasetResp,
					datasetParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with pset_hash param", // DBSClientReader_t.test009
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_access_type": []string{TestData.DatasetAccessType},
					"pset_hash":           []string{TestData.PsetHash},
				},
				output: []Response{
					datasetResp,
					datasetParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with app_name param", // DBSClientReader_t.test010
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_access_type": []string{TestData.DatasetAccessType},
					"app_name":            []string{TestData.AppName},
				},
				output: []Response{
					datasetResp,
					datasetParentResp,
				},
				respCode: http.StatusOK,
			},
			{
				description: "Test GET with output_module_label param", // DBSClientReader_t.test011
				serverType:  "DBSReader",
				method:      "GET",
				params: url.Values{
					"dataset_access_type": []string{"PRODUCTION"},
					"output_module_label": []string{TestData.OutputModuleLabel},
				},
				output: []Response{
					datasetResp,
					datasetParentResp,
				},
				respCode: http.StatusOK,
			},
		},
	}

}
