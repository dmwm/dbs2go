package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/web"
)

// this file contains logic for fileArray API
//
// the HTTP POST request body is defined by multiple structs defined in this file
// each of those structs are named based on the POST fields
//
// the basic HTTP response body is defined by fileResponse struct in test/int_files.go
// the detailed HTTP response body is defined by fileDetailResponse struct in test/int_files.go
// the HTTP response body for run_num param is defined by fileRunResponse struct in test/int_files.go
// the HTTP handlers and endpoints are defined in the EndpointTestCase struct defined in test/integration_cases.go

// fileArray request with dataset parameter
type fileArrayDatasetRequest struct {
	Dataset string `json:"dataset"`
}

// fileArray request with dataset and validFileOnly parameter
type fileArrayDatasetValidFileRequest struct {
	Dataset       string `json:"dataset"`
	ValidFileOnly string `json:"validFileOnly"`
}

// fileArray request with dataset, validFileOnly, detail and sumOverLumi parameter
type fileArrayDatasetValidFileDetailSumOverLumiRequest struct {
	Dataset       string `json:"dataset"`
	ValidFileOnly string `json:"validFileOnly"`
	Detail        string `json:"detail"`
	SumOverLumi   string `json:"sumOverLumi"`
}

// fileArray request with block_name parameter
type fileArrayBlockNameRequest struct {
	BlockName string `json:"block_name"`
}

// fileArray request with block_name and detail parameter
type fileArrayBlockNameDetailRequest struct {
	BlockName string `json:"block_name"`
	Detail    string `json:"detail"`
}

// fileArray request with block_name and validFileOnly parameter
type fileArrayBlockNameValidFileRequest struct {
	BlockName     string `json:"block_name"`
	ValidFileOnly string `json:"validFileOnly"`
}

// fileArray request with block_name and validFileOnly parameter
type fileArrayBlockNameDetailValidFileRequest struct {
	BlockName     string `json:"block_name"`
	Detail        string `json:"detail"`
	ValidFileOnly string `json:"validFileOnly"`
}

// fileArray request with block_name, run_num, and lumi_list parameter
type fileArrayBlockNameRunNumLumiListRequest struct {
	BlockName string `json:"block_name"`
	RunNum    string `json:"run_num"`
	LumiList  string `json:"lumi_list"`
}

// fileArray request with block_name, run_num, lumi_list, and detail parameter
type fileArrayBlockNameRunNumLumiListDetailRequest struct {
	BlockName string `json:"block_name"`
	RunNum    string `json:"run_num"`
	LumiList  string `json:"lumi_list"`
	Detail    string `json:"detail"`
}

// fileArray request with block_name, run_num, lumi_list, and validFileOnly parameter
type fileArrayBlockNameRunNumLumiListValidFileRequest struct {
	BlockName     string `json:"block_name"`
	RunNum        string `json:"run_num"`
	LumiList      string `json:"lumi_list"`
	ValidFileOnly string `json:"validFileOnly"`
}

// fileArray request with block_name, run_num, lumi_list, detail, and validFileOnly parameter
type fileArrayBlockNameRunNumLumiListValidFileDetailRequest struct {
	BlockName     string `json:"block_name"`
	RunNum        string `json:"run_num"`
	LumiList      string `json:"lumi_list"`
	ValidFileOnly string `json:"validFileOnly"`
	Detail        string `json:"detail"`
}

// fileArray request with block_name, run_num, lumi_list and sumOverLumi parameter
type fileArrayBlockNameRunNumLumiListSumOverLumiRequest struct {
	BlockName   string `json:"block_name"`
	RunNum      string `json:"run_num"`
	LumiList    string `json:"lumi_list"`
	SumOverLumi string `json:"sumOverLumi"`
}

// fileArray request with block_name, run_num, lumi_list and sumOverLumi parameter
type fileArrayBlockNameRunNumLumiListSumOverLumiDetailRequest struct {
	BlockName   string `json:"block_name"`
	RunNum      string `json:"run_num"`
	LumiList    string `json:"lumi_list"`
	SumOverLumi string `json:"sumOverLumi"`
	Detail      string `json:"detail"`
}

// fileArray request with block_name, run_num, sumOverLumi, detail parameter
type fileArrayBlockNameRunNumSumOverLumiDetailRequest struct {
	BlockName   string `json:"block_name"`
	RunNum      string `json:"run_num"`
	SumOverLumi string `json:"sumOverLumi"`
	Detail      string `json:"detail"`
}

// fileArray request with block_name, run_num, sumOverLumi parameter
type fileArrayBlockNameRunNumSumOverLumiRequest struct {
	BlockName   string `json:"block_name"`
	RunNum      string `json:"run_num"`
	SumOverLumi string `json:"sumOverLumi"`
}

// logical_file_name structs

// fileArray request with logical_file_name
type fileArrayLFNRequest struct {
	LogicalFileName string `json:"logical_file_name"`
}

// fileArray request with logical_file_name, validFileOnly
type fileArrayLFNValidFileRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	ValidFileOnly   string `json:"validFileOnly"`
}

// fileArray request with logical_file_name, run_num, lumi_list
type fileArrayLFNRunNumLumiListRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
	LumiList        string `json:"lumi_list"`
}

// fileArray request with logical_file_name, run_num, lumi_list, detail
type fileArrayLFNRunNumLumiListDetailRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
	LumiList        string `json:"lumi_list"`
	Detail          string `json:"detail"`
}

// fileArray request with logical_file_name, run_num, lumi_list, validFileOnly
type fileArrayLFNRunNumLumiListValidFileRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
	LumiList        string `json:"lumi_list"`
	ValidFileOnly   string `json:"validFileOnly"`
}

// fileArray request with logical_file_name, run_num, lumi_list, detail
type fileArrayLFNRunNumLumiListValidFileDetailRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
	LumiList        string `json:"lumi_list"`
	ValidFileOnly   string `json:"validFileOnly"`
	Detail          string `json:"detail"`
}

// fileArray request with logical_file_name, run_num, lumi_list, sumOverLumi
type fileArrayLFNRunNumLumiListSumOverLumiRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
	LumiList        string `json:"lumi_list"`
	SumOverLumi     string `json:"sumOverLumi"`
}

// fileArray request with logical_file_name, run_num, sumOverLumi, detail
type fileArrayLFNRunNumSumOverLumiDetailRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
	SumOverLumi     string `json:"sumOverLumi"`
	Detail          string `json:"detail"`
}

// fileArray request with dataset, release_version
type fileArrayDatasetReleaseRequest struct {
	Dataset        string `json:"dataset"`
	ReleaseVersion string `json:"release_version"`
}

// fileArray request with dataset, release_version, validFileOnly
type fileArrayDatasetReleaseValidFileRequest struct {
	Dataset        string `json:"dataset"`
	ReleaseVersion string `json:"release_version"`
	ValidFileOnly  string `json:"validFileOnly"`
}

// fileArray request with dataset, output_module_config fields
type fileArrayDatasetOutputModRequest struct {
	Dataset           string `json:"dataset"`
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
}

// fileArray request with lfn, output_module_config fields
type fileArrayLFNOutputModRequest struct {
	LogicalFileName   string `json:"logical_file_name"`
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
}

// fileArray request with lfn, output_module_config fields, validFileOnly
type fileArrayLFNOutputModValidFileRequest struct {
	LogicalFileName   string `json:"logical_file_name"`
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
	ValidFileOnly     string `json:"validFileOnly"`
}

// fileArray request with dataset, run_num, lumi_list fields
type fileArrayDatasetRunLumiRequest struct {
	Dataset  string `json:"dataset"`
	RunNum   string `json:"run_num"`
	LumiList string `json:"lumi_list"`
}

// fileArray request with dataset, run_num, lumi_list, detail fields
type fileArrayDatasetRunLumiDetailRequest struct {
	Dataset  string `json:"dataset"`
	RunNum   string `json:"run_num"`
	LumiList string `json:"lumi_list"`
	Detail   string `json:"detail"`
}

// fileArray request with dataset, run_num, lumi_list, validFileOnly fields
type fileArrayDatasetRunLumiValidRequest struct {
	Dataset       string `json:"dataset"`
	RunNum        string `json:"run_num"`
	LumiList      string `json:"lumi_list"`
	ValidFileOnly string `json:"validFileOnly"`
}

// fileArray request with dataset, run_num, lumi_list, detail fields
type fileArrayDatasetRunLumiValidDetailRequest struct {
	Dataset       string `json:"dataset"`
	RunNum        string `json:"run_num"`
	LumiList      string `json:"lumi_list"`
	Detail        string `json:"detail"`
	ValidFileOnly string `json:"validFileOnly"`
}

// fileArray request with dataset, run_num
type fileArrayDatasetRunRequest struct {
	Dataset string `json:"dataset"`
	RunNum  string `json:"run_num"`
}

// fileArray request with block_name, run_num
type fileArrayBlockNameRunNumRequest struct {
	BlockName string `json:"block_name"`
	RunNum    string `json:"run_num"`
}

// fileArray request with lfn, run_num
type fileArrayLFNRunNumRequest struct {
	LogicalFileName string `json:"logical_file_name"`
	RunNum          string `json:"run_num"`
}

// fileArray request with origin_site_name, dataset
type fileArrayOriginDatasetRequest struct {
	OriginSiteName string `json:"origin_site_name"`
	Dataset        string `json:"dataset"`
}

// fileArray request wih logical_file_name, validFileOnly
type fileArrayLFNValidFileOnlyRequest struct {
	LogicalFileName []string `json:"logical_file_name"`
	ValidFileOnly   string   `json:"validFileOnly"`
}

// fileArray request wih logical_file_name, detail
type fileArrayLFNDetailRequest struct {
	LogicalFileName []string `json:"logical_file_name"`
	Detail          string   `json:"detail"`
}

// fileArray request wih logical_file_name, validFileOnly, detail
type fileArrayLFNValidFileOnlyDetailRequest struct {
	LogicalFileName []string `json:"logical_file_name"`
	ValidFileOnly   string   `json:"validFileOnly"`
	Detail          string   `json:"detail"`
}

// fileArray request with logical_file_name, detail, run_num
type fileArrayLFNRunNumDetailRequest struct {
	LogicalFileName []string `json:"logical_file_name"`
	RunNum          string   `json:"run_num"`
	Detail          string   `json:"detail"`
}

// fileArray request with logical_file_name, detail, sumOverLumi
type fileArrayLFNSumOverLumiDetailRequest struct {
	LogicalFileName []string `json:"logical_file_name"`
	SumOverLumi     string   `json:"sumOverLumi"`
	Detail          string   `json:"detail"`
}

// fileArray request with run_num
type fileArrayRunNumReqest struct {
	RunNum string `json:"run_num"`
}

// fileArray request with run_num, dataset
type fileArrayRunNumDatasetRequest struct {
	RunNum  string `json:"run_num"`
	Dataset string `json:"dataset"`
}

// fileArray request with run_num, block_name
type fileArrayRunNumBlockNameRequest struct {
	RunNum    string `json:"run_num"`
	BlockName string `json:"block_name"`
}

// test fileArray
func getFileArrayTestTable(t *testing.T) []EndpointTestCase {
	fileLumiList := []dbs.FileLumi{
		{LumiSectionNumber: 27414, RunNumber: 97},
		{LumiSectionNumber: 26422, RunNumber: 98},
		{LumiSectionNumber: 29838, RunNumber: 99},
	}

	var lfns []Response
	var lfnsRuns []Response
	var detailResp []Response
	var detailRunResp []Response
	var detailRunSumLumiResp []Response
	for i := 1; i <= 10; i++ {
		lfn := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%v/%v.root", TestData.UID, i)
		lfns = append(lfns, fileResponse{LOGICAL_FILE_NAME: lfn})
		fileParentLFN := fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%v/%v.root", TestData.UID, i)
		fileParentList := []dbs.FileParentLFNRecord{
			{
				FILE_PARENT_LFN: fileParentLFN,
			},
		}
		fileRecord := createFileRecord(i, TestData.Dataset, TestData.Block, fileLumiList, lfn, fileParentList)
		r := createFileDetailedResponse(i+10, 1, 1, fileRecord)
		for j := 97; j < 100; j++ {
			fileRunResp := fileRunResponse{
				LOGICAL_FILE_NAME: lfn,
				RUN_NUM:           int64(j),
			}
			lfnsRuns = append(lfnsRuns, fileRunResp)
		}

		if i == 1 {
			r.LAST_MODIFIED_BY = "DBS-workflow"
			r.IS_FILE_VALID = 0
		}
		detailResp = append(detailResp, r)
		var detailRun fileDetailRunEventResponse
		d, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err.Error())
		}
		err = json.Unmarshal(d, &detailRun)
		if err != nil {
			t.Fatal(err.Error())
		}
		detailRun.RUN_NUM = 97
		detailRun.EventCount = 1619
		detailRunResp = append(detailRunResp, detailRun)

		var detailRunResp fileDetailRunResponse
		d, err = json.Marshal(detailRun)
		if err != nil {
			t.Fatal(err.Error())
		}
		err = json.Unmarshal(d, &detailRunResp)
		if err != nil {
			t.Fatal(err.Error())
		}
		if i == 1 {
			for j := 97; j < 100; j++ {
				for i := 0; i < 3; i++ {
					detailRunResp.RUN_NUM = int64(j)
					detailRunSumLumiResp = append(detailRunSumLumiResp, detailRunResp)
				}
			}
		}
	}

	var lfnsRun97 []Response
	var lfnsRun99 []Response
	for _, u := range lfnsRuns {
		if u.(fileRunResponse).RUN_NUM == 99 {
			lfnsRun99 = append(lfnsRun99, u)
		}
		if u.(fileRunResponse).RUN_NUM == 97 {
			lfnsRun97 = append(lfnsRun97, u)
		}
	}

	dbsError := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Code:     dbs.ParametersErrorCode,
		Message:  "cannot supply more than one list (lfn, run_num or lumi) at one query",
		Function: "dbs.files.Files",
	}
	hrec := createHTTPError("POST", "/dbs/fileArray")
	errorResp := createServerErrorResponse(hrec, &dbsError)

	dbsError2 := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Code:     dbs.ParametersErrorCode,
		Message:  "When sumOverLumi=1, no run_num list is allowed",
		Function: "dbs.files.Files",
	}
	errorResp2 := createServerErrorResponse(hrec, &dbsError2)

	var largeFileResp []Response
	err := readJsonFile(t, "./data/integration/files_response_data.json", &largeFileResp)
	if err != nil {
		t.Fatal(err.Error())
	}

	childBulk := BulkBlocksData.ConcurrentChildData
	var siteDSResp []Response
	for _, f := range childBulk.Files {
		fr := fileResponse{
			LOGICAL_FILE_NAME: f.LogicalFileName,
		}
		siteDSResp = append(siteDSResp, fr)
	}

	dbsError3 := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Code:     dbs.ParametersErrorCode,
		Message:  "When sumOverLumi=1, no lfn list is allowed",
		Function: "dbs.files.Files",
	}
	errorResp3 := createServerErrorResponse(hrec, &dbsError3)

	dbsError4 := dbs.DBSError{
		Reason:   dbs.InvalidParamErr.Error(),
		Code:     dbs.ParametersErrorCode,
		Message:  "files API does not support run_num=1 when no lumi and lfns list provided",
		Function: "dbs.files.Files",
	}
	errorResp4 := createServerErrorResponse(hrec, &dbsError4)

	return []EndpointTestCase{
		{
			description:     "Test fileArray API with dataset parameter",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with datasets", // DBSClientReader.test03200
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRequest{
						Dataset: TestData.Dataset,
					},
					output:   lfns,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with datasets, validFileOnly true", // DBSClientReader.test03200a
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetValidFileRequest{
						Dataset:       TestData.Dataset,
						ValidFileOnly: "1",
					},
					output:   lfns[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with datasets, validFileOnly false", // DBSClientReader.test03200b
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetValidFileRequest{
						Dataset:       TestData.Dataset,
						ValidFileOnly: "0",
					},
					output:   lfns,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with datasets, validFileOnly true, detail, sumOverLumi", // DBSClientReader.test03200c
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetValidFileDetailSumOverLumiRequest{
						Dataset:       TestData.Dataset,
						ValidFileOnly: "1",
						Detail:        "1",
						SumOverLumi:   "1",
					},
					output:   detailResp[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with datasets, validFileOnly false, detail, sumOverLumi", // DBSClientReader.test03200d
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetValidFileDetailSumOverLumiRequest{
						Dataset:       TestData.Dataset,
						ValidFileOnly: "0",
						Detail:        "1",
						SumOverLumi:   "1",
					},
					output:   detailResp,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray API with block_name parameter",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with block_name", // DBSClientReader.test03300a
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRequest{
						BlockName: TestData.Block,
					},
					output:   lfns,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name and detail", // DBSClientReader.test03300b
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameDetailRequest{
						BlockName: TestData.Block,
						Detail:    "1",
					},
					output:   detailResp,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name and validFileOnly true", // DBSClientReader.test03300c
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameValidFileRequest{
						BlockName:     TestData.Block,
						ValidFileOnly: "1",
					},
					output:   lfns[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, detail, validFileOnly true", // DBSClientReader.test03300d
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameDetailValidFileRequest{
						BlockName:     TestData.Block,
						Detail:        "1",
						ValidFileOnly: "1",
					},
					output:   detailResp[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name and validFileOnly 0", // DBSClientReader.test03300e
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameValidFileRequest{
						BlockName:     TestData.Block,
						ValidFileOnly: "0",
					},
					output:   lfns,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, detail, validFileOnly 0", // DBSClientReader.test03300f
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameDetailValidFileRequest{
						BlockName:     TestData.Block,
						Detail:        "1",
						ValidFileOnly: "0",
					},
					output:   detailResp,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, lumi_list", // DBSClientReader.test03300g
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListRequest{
						BlockName: TestData.Block,
						RunNum:    "97",
						LumiList:  "[27414,26422,29838]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, nested lumi_list", // DBSClientReader.test03300h
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListRequest{
						BlockName: TestData.Block,
						RunNum:    "97",
						LumiList:  "[[27414 27418] [26422 26426] [29838 29842]]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, lumi_list, detail", // DBSClientReader.test03300i
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListDetailRequest{
						BlockName: TestData.Block,
						RunNum:    "97",
						LumiList:  "[27414,26422,29838]",
						Detail:    "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, nested lumi_list, detail", // DBSClientReader.test03300j
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListDetailRequest{
						BlockName: TestData.Block,
						RunNum:    "97",
						LumiList:  "[[27414 27418] [26422 26426] [29838 29842]]",
						Detail:    "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, lumi_list, validFileOnly 1", // DBSClientReader.test03300k
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListValidFileRequest{
						BlockName:     TestData.Block,
						RunNum:        "97",
						LumiList:      "[27414,26422,29838]",
						ValidFileOnly: "1",
					},
					output:   lfnsRun97[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, nested lumi_list, validFileOnly 1", // DBSClientReader.test03300l
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListValidFileRequest{
						BlockName:     TestData.Block,
						RunNum:        "97",
						LumiList:      "[[27414 27418] [26422 26426] [29838 29842]]",
						ValidFileOnly: "1",
					},
					output:   lfnsRun97[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, lumi_list, detail, validFileOnly 1", // DBSClientReader.test03300m
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListValidFileDetailRequest{
						BlockName:     TestData.Block,
						RunNum:        "97",
						LumiList:      "[27414,26422,29838]",
						ValidFileOnly: "1",
						Detail:        "1",
					},
					output:   detailRunResp[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, run_num, nested lumi_list, detail, validFileOnly 1", // DBSClientReader.test03300n
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListValidFileDetailRequest{
						BlockName:     TestData.Block,
						RunNum:        "97",
						LumiList:      "[[27414 27418] [26422 26426] [29838 29842]]",
						ValidFileOnly: "1",
						Detail:        "1",
					},
					output:   detailRunResp[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, list run_num, lumi_list", // DBSClientReader.test03300o
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListRequest{
						BlockName: TestData.Block,
						RunNum:    "[97]",
						LumiList:  "[27414,26422,29838]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with block_name, list run_num, lumi_list, sumOverLumi", // DBSClientReader.test03300p
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListSumOverLumiRequest{
						BlockName:   TestData.Block,
						RunNum:      "[97]",
						LumiList:    "[27414,26422,29838]",
						SumOverLumi: "1",
					},
					output:   []Response{errorResp},
					respCode: http.StatusBadRequest,
				},
				{
					description: "Test POST with block_name, list run_num, lumi_list, sumOverLumi, detail", // DBSClientReader.test03300q
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumLumiListSumOverLumiDetailRequest{
						BlockName:   TestData.Block,
						RunNum:      "[97]",
						LumiList:    "[27414,26422,29838]",
						SumOverLumi: "1",
						Detail:      "1",
					},
					output:   []Response{errorResp},
					respCode: http.StatusBadRequest,
				},
				{
					description: "Test POST with block_name, list run_num, sumOverLumi, detail", // DBSClientReader.test03300r
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumSumOverLumiDetailRequest{
						BlockName:   TestData.Block,
						RunNum:      "[97]",
						SumOverLumi: "1",
						Detail:      "1",
					},
					output:   []Response{errorResp2},
					respCode: http.StatusBadRequest,
				},
				{
					description: "Test POST with block_name, list run_num, sumOverLumi", // DBSClientReader.test03300s
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumSumOverLumiRequest{
						BlockName:   TestData.Block,
						RunNum:      "[97]",
						SumOverLumi: "1",
					},
					output:   []Response{errorResp2},
					respCode: http.StatusBadRequest,
				},
				{
					description: "Test POST with block_name, range run_num, sumOverLumi, detail", // DBSClientReader.test03300t
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumSumOverLumiDetailRequest{
						BlockName:   TestData.Block,
						RunNum:      "97-99",
						SumOverLumi: "1",
						Detail:      "1",
					},
					output:   largeFileResp,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray API with logical_file_name parameter",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with logical_file_name", // DBSClientReader_t.test03400a
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRequest{
						LogicalFileName: TestData.Files[0],
					},
					output:   lfns[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, validFileOnly 1", // DBSClientReader_t.test03400b
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNValidFileRequest{
						LogicalFileName: TestData.Files[0],
						ValidFileOnly:   "1",
					},
					output:   []Response{},
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, validFileOnly 1", // DBSClientReader_t.test03400b2
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNValidFileRequest{
						LogicalFileName: TestData.Files[1],
						ValidFileOnly:   "1",
					},
					output:   lfns[1:2],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, validFileOnly 0", // DBSClientReader_t.test03400c
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNValidFileRequest{
						LogicalFileName: TestData.Files[0],
						ValidFileOnly:   "0",
					},
					output:   lfns[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, lumi_list", // DBSClientReader_t.test03400d
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "97",
						LumiList:        "[27414,26422,29838]",
					},
					output:   lfnsRun97[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, nested lumi_list", // DBSClientReader_t.test03400e
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "97",
						LumiList:        "[[27414 27418] [26422 26426] [29838 29842]]",
					},
					output:   lfnsRun97[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, lumi_list, detail", // DBSClientReader_t.test03400f
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListDetailRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "97",
						LumiList:        "[27414,26422,29838]",
						Detail:          "1",
					},
					output:   detailRunResp[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, nested lumi_list, detail", // DBSClientReader_t.test03400g
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListDetailRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "97",
						LumiList:        "[[27414 27418] [26422 26426] [29838 29842]]",
						Detail:          "1",
					},
					output:   detailRunResp[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, lumi_list, validFileOnly", // DBSClientReader_t.test03400h
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListValidFileRequest{
						LogicalFileName: TestData.Files[1],
						RunNum:          "97",
						LumiList:        "[27414,26422,29838]",
						ValidFileOnly:   "1",
					},
					output:   lfnsRun97[1:2],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, nested lumi_list, validFileOnly", // DBSClientReader_t.test03400i
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListValidFileRequest{
						LogicalFileName: TestData.Files[1],
						RunNum:          "97",
						LumiList:        "[[27414 27418] [26422 26426] [29838 29842]]",
						ValidFileOnly:   "1",
					},
					output:   lfnsRun97[1:2],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, lumi_list, validFileOnly, detail", // DBSClientReader_t.test03400j
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListValidFileDetailRequest{
						LogicalFileName: TestData.Files[1],
						RunNum:          "97",
						LumiList:        "[27414,26422,29838]",
						ValidFileOnly:   "1",
						Detail:          "1",
					},
					output:   detailRunResp[1:2],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, nested lumi_list, validFileOnly, detail", // DBSClientReader_t.test03400k
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListValidFileDetailRequest{
						LogicalFileName: TestData.Files[1],
						RunNum:          "97",
						LumiList:        "[[27414 27418] [26422 26426] [29838 29842]]",
						ValidFileOnly:   "1",
						Detail:          "1",
					},
					output:   detailRunResp[1:2],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, list run_num, lumi_list", // DBSClientReader_t.test03400l
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "[97]",
						LumiList:        "[27414,26422,29838]",
					},
					output:   lfnsRun97[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, run_num, lumi_list", // DBSClientReader_t.test03400m
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumLumiListRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "97",
						LumiList:        "[27414,26422,29838]",
					},
					output:   lfnsRun97[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with logical_file_name, list run_num, sumOverLumi, detail", // DBSClientReader_t.test03400n
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumSumOverLumiDetailRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "[97]",
						SumOverLumi:     "1",
						Detail:          "1",
					},
					output:   []Response{errorResp2},
					respCode: http.StatusBadRequest,
				},
				{
					description: "Test POST with logical_file_name, ranged run_num, sumOverLumi, detail", // DBSClientReader_t.test03400o
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumSumOverLumiDetailRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "97-99",
						SumOverLumi:     "1",
						Detail:          "1",
					},
					output:   detailRunSumLumiResp,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with dataset and output_module_config parameters",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with dataset and release_version", // DBSClientReader_t.test03500a
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetReleaseRequest{
						Dataset:        TestData.Dataset,
						ReleaseVersion: TestData.ReleaseVersion,
					},
					output:   lfns,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with dataset, release_version, validFileOnly 1", // DBSClientReader_t.test03500b
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetReleaseValidFileRequest{
						Dataset:        TestData.Dataset,
						ReleaseVersion: TestData.ReleaseVersion,
						ValidFileOnly:  "1",
					},
					output:   lfns[1:],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with dataset, release_version, pset_hash, app_name, output_module_label", // DBSClientReader_t.test03600
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetOutputModRequest{
						Dataset:           TestData.Dataset,
						ReleaseVersion:    TestData.ReleaseVersion,
						PsetHash:          TestData.PsetHash,
						AppName:           TestData.AppName,
						OutputModuleLabel: TestData.OutputModuleLabel,
					},
					output:   lfns,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with logical_file_name and output_module_config parameters",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with dataset and release_version", // DBSClientReader_t.test03700a
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNOutputModRequest{
						LogicalFileName:   TestData.Files[0],
						ReleaseVersion:    TestData.ReleaseVersion,
						PsetHash:          TestData.PsetHash,
						AppName:           TestData.AppName,
						OutputModuleLabel: TestData.OutputModuleLabel,
					},
					output:   lfns[:1],
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with dataset, release_version, pset_hash, app_name, output_module_label", // DBSClientReader_t.test03700b
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNOutputModValidFileRequest{
						LogicalFileName:   TestData.Files[1],
						ReleaseVersion:    TestData.ReleaseVersion,
						PsetHash:          TestData.PsetHash,
						AppName:           TestData.AppName,
						OutputModuleLabel: TestData.OutputModuleLabel,
						ValidFileOnly:     "1",
					},
					output:   lfns[1:2],
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with non-existing fields",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with non-existing dataset", // DBSClientReader_t.test03800
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRequest{
						Dataset: "/does/not/EXIST",
					},
					output:   []Response{},
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with non-existing block_name", // DBSClientReader_t.test03900
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRequest{
						BlockName: "/does/not/EXIST#123",
					},
					output:   []Response{},
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with non-existing logical_file_name", // DBSClientReader_t.test04000
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRequest{
						LogicalFileName: "/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root",
					},
					output:   []Response{},
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with dataset, run_num, lumi_list fields",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{
					description: "Test POST with dataset, run_num, lumi_list", // DBSClientReader_t.test04000a
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[27414,26422,29838]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with dataset, run_num, nested lumi_list", // DBSClientReader_t.test04000b
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[[27414 27418] [26422 26426] [29838 29842]]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{
					description: "Test POST with dataset, run_num, lumi_list, detail", // DBSClientReader_t.test04000c
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiDetailRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[27414,26422,29838]",
						Detail:   "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000d
					description: "Test POST with dataset, run_num, nested lumi_list, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiDetailRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[[27414 27418] [26422 26426] [29838 29842]]",
						Detail:   "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000e
					description: "Test POST with dataset, run_num, lumi_list, validFileOnly",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiValidRequest{
						Dataset:       TestData.Dataset,
						RunNum:        "97",
						LumiList:      "[27414,26422,29838]",
						ValidFileOnly: "1",
					},
					output:   lfnsRun97[1:],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000f
					description: "Test POST with dataset, run_num, nested lumi_list, validFileOnly",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiValidRequest{
						Dataset:       TestData.Dataset,
						RunNum:        "97",
						LumiList:      "[[27414 27418] [26422 26426] [29838 29842]]",
						ValidFileOnly: "1",
					},
					output:   lfnsRun97[1:],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000g
					description: "Test POST with dataset, run_num, lumi_list, validFileOnly, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiValidDetailRequest{
						Dataset:       TestData.Dataset,
						RunNum:        "97",
						LumiList:      "[27414,26422,29838]",
						ValidFileOnly: "1",
						Detail:        "1",
					},
					output:   detailRunResp[1:],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000h
					description: "Test POST with dataset, run_num, nested lumi_list, validFileOnly, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiValidDetailRequest{
						Dataset:       TestData.Dataset,
						RunNum:        "97",
						LumiList:      "[[27414 27418] [26422 26426] [29838 29842]]",
						ValidFileOnly: "1",
						Detail:        "1",
					},
					output:   detailRunResp[1:],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000i
					description: "Test POST with dataset, run_num list, lumi_list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "[97]",
						LumiList: "[27414,26422,29838]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000j
					description: "Test POST with dataset, run_num list, nested lumi_list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[[27414 27418] [26422 26426] [29838 29842]]",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000k
					description: "Test POST with dataset, run_num list, lumi_list, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiDetailRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "[97]",
						LumiList: "[27414,26422,29838]",
						Detail:   "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000l
					description: "Test POST with dataset, run_num list, nested lumi_list, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiDetailRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[[27414 27418] [26422 26426] [29838 29842]]",
						Detail:   "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test04000m
					description: "Test POST with dataset, run_num list, nested lumi_list > 1000, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunLumiDetailRequest{
						Dataset:  TestData.Dataset,
						RunNum:   "97",
						LumiList: "[[27414 27418] [26422 26426] [29838 29842]]",
						Detail:   "1",
					},
					output:   detailRunResp,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with dataset, range run_num",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{ // DBSClientReader_t.test06100a
					description: "Test POST with dataset, range run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunRequest{
						Dataset: TestData.Dataset,
						RunNum:  "97-99",
					},
					output:   lfnsRuns,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test06100b // TODO: Check if there should be a response
					description: "Test POST with dataset, list run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunRequest{
						Dataset: TestData.Dataset,
						RunNum:  "[97,99]",
					},
					output:   []Response{},
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test06100c
					description: "Test POST with dataset, single run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunRequest{
						Dataset: TestData.Dataset,
						RunNum:  "97",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test06100d
					description: "Test POST with dataset, single run_num in list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunRequest{
						Dataset: TestData.Dataset,
						RunNum:  "[99]",
					},
					output:   lfnsRun99,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test06100e
					description: "Test POST with dataset, single run_num in list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunRequest{
						Dataset: TestData.Dataset,
						RunNum:  "[99]",
					},
					output:   lfnsRun99,
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test06100f
					description: "Test POST with dataset, single run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayDatasetRunRequest{
						Dataset: TestData.Dataset,
						RunNum:  "97",
					},
					output:   lfnsRun97,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with block_name, range run_num",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{ // DBSClientReader_t.test06200
					description: "Test fileArray with block_name, range run_num list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayBlockNameRunNumRequest{
						BlockName: TestData.Block,
						RunNum:    "[97-99]",
					},
					output:   lfnsRuns,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with logical_file_name, run_num",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{ // DBSClientReader_t.test06300a
					description: "Test fileArray with logical_file_name, mixed run_num list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "[97-99,100,10000]",
					},
					output:   lfnsRuns[:3],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test06300b
					description: "Test fileArray with logical_file_name, mixed run_num range list",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumRequest{
						LogicalFileName: TestData.Files[0],
						RunNum:          "[97-99,100,10000,50-100]",
					},
					output:   lfnsRuns[:3],
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with origin_site_name, dataset",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{ // DBSClientReader_t.test07000
					description: "Test fileArray with origin_site_name, dataset",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayOriginDatasetRequest{
						OriginSiteName: childBulk.Block.OriginSiteName,
						Dataset:        childBulk.Dataset.Dataset,
					},
					output:   siteDSResp,
					respCode: http.StatusOK,
				},
			},
		},
		{
			description:     "Test fileArray with logical_file_name list",
			defaultHandler:  web.FileArrayHandler,
			defaultEndpoint: "/dbs/fileArray",
			testCases: []testCase{
				{ // DBSClientReader_t.test034d
					description: "Test fileArray with logical_file_name list, validFileOnly 0",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNValidFileOnlyRequest{
						LogicalFileName: []string{
							TestData.Files[0],
							TestData.Files[1],
							TestData.Files[2],
							TestData.Files[3],
						},
						ValidFileOnly: "0",
					},
					output:   lfns[:4],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test034e
					description: "Test fileArray with logical_file_name list, validFileOnly 1",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNValidFileOnlyRequest{
						LogicalFileName: []string{
							TestData.Files[0],
							TestData.Files[1],
							TestData.Files[2],
							TestData.Files[3],
						},
						ValidFileOnly: "1",
					},
					output:   lfns[1:4],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test034f
					description: "Test fileArray with logical_file_name list, validFileOnly 1, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNValidFileOnlyDetailRequest{
						LogicalFileName: []string{
							TestData.Files[0],
							TestData.Files[1],
							TestData.Files[2],
							TestData.Files[3],
						},
						ValidFileOnly: "1",
						Detail:        "1",
					},
					output:   detailResp[1:4],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test034g
					description: "Test fileArray with logical_file_name list, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNDetailRequest{
						LogicalFileName: []string{
							TestData.Files[0],
							TestData.Files[1],
							TestData.Files[2],
							TestData.Files[3],
						},
						Detail: "1",
					},
					output:   detailResp[0:4],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test034h
					description: "Test fileArray with logical_file_name list, run_num, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNRunNumDetailRequest{
						LogicalFileName: []string{
							TestData.Files[0],
							TestData.Files[1],
							TestData.Files[2],
							TestData.Files[3],
						},
						Detail: "1",
						RunNum: "97",
					},
					output:   detailRunResp[0:4],
					respCode: http.StatusOK,
				},
				{ // DBSClientReader_t.test034i
					description: "Test fileArray with logical_file_name list, sumOverLumi, detail",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayLFNSumOverLumiDetailRequest{
						LogicalFileName: []string{
							TestData.Files[0],
							TestData.Files[1],
							TestData.Files[2],
							TestData.Files[3],
						},
						Detail:      "1",
						SumOverLumi: "1",
					},
					output:   []Response{errorResp3},
					respCode: http.StatusBadRequest,
				},
				{ // DBSClientReader_t.test034i_2
					description: "Test fileArray with run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayRunNumReqest{
						RunNum: "1",
					},
					output:   []Response{errorResp4},
					respCode: http.StatusBadRequest,
				},
				{ // DBSClientReader_t.test034j
					description: "Test fileArray with dataset, run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayRunNumDatasetRequest{
						RunNum:  "1",
						Dataset: TestData.Dataset,
					},
					output:   []Response{errorResp4},
					respCode: http.StatusBadRequest,
				},
				{ // DBSClientReader_t.test034k
					description: "Test fileArray with block_name, run_num",
					method:      "POST",
					serverType:  "DBSReader",
					input: fileArrayRunNumBlockNameRequest{
						RunNum:    "1",
						BlockName: TestData.Block,
					},
					output:   []Response{errorResp4},
					respCode: http.StatusBadRequest,
				},
				// TODO: Figure out logicl for DBSClientReader_t.test.034l
			},
		},
	}
}
