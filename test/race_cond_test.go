package main

// Race Condition Tests
// This file contains code necessary to test for racing conditions in DBSWriter APIs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/dmwm/dbs2go/dbs"
)

var testData initialData

// creates a file with shared Output Module Config for bulkblocks
func createFile2(t *testing.T, i int, factor int) (dbs.File, dbs.FileConfig) {
	algo := dbs.FileConfig{
		ReleaseVersion:    testData.ReleaseVersion,
		PsetHash:          testData.PsetHash,
		AppName:           testData.AppName,
		OutputModuleLabel: testData.OutputModuleLabel,
		GlobalTag:         testData.GlobalTag,
	}

	file := dbs.File{
		Adler32:          "NOTSET",
		FileType:         "EDM",
		FileSize:         2012211901,
		AutoCrossSection: 0.0,
		CheckSum:         "1504266448",
		FileLumiList: []dbs.FileLumi{
			{
				LumiSectionNumber: int64(27414 + i*100 + factor*100),
				RunNumber:         98,
				EventCount:        66,
			},
			{
				LumiSectionNumber: int64(26422 + i*100 + factor*100),
				RunNumber:         98,
				EventCount:        67,
			},
			{
				LumiSectionNumber: int64(29838 + i*100 + factor*100),
				RunNumber:         98,
				EventCount:        68,
			},
		},
		EventCount:      201,
		LogicalFileName: fmt.Sprintf("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/StepChain_/p%v%d/%v%d.root", TestData.UID, i*10, i, factor*100),
		IsFileValid:     1,
	}

	algo.LFN = file.LogicalFileName
	return file, algo
}

// generate a single bulkblock
func generateBulkBlock(t *testing.T, factor int) dbs.BulkBlocks {
	var bulkBlock dbs.BulkBlocks

	algo := dbs.DatasetConfig{
		ReleaseVersion:    testData.ReleaseVersion,
		PsetHash:          testData.PsetHash,
		AppName:           testData.AppName,
		OutputModuleLabel: testData.OutputModuleLabel,
		GlobalTag:         testData.GlobalTag,
		CreationDate:      1501192514,
	}

	primDS := dbs.PrimaryDataset{
		PrimaryDSName: testData.StepPrimaryDSName,
		PrimaryDSType: "test",
		CreateBy:      "WMAgent",
		CreationDate:  1501192514, // Replace with fixed time
	}

	dataset := dbs.Dataset{
		PhysicsGroupName:     testData.PhysicsGroupName,
		ProcessedDSName:      testData.ProcDataset,
		DataTierName:         testData.Tier,
		DatasetAccessType:    testData.DatasetAccessType2,
		Dataset:              testData.StepchainDataset,
		PrepID:               "TestPrepID",
		CreateBy:             "WMAgent",
		LastModifiedBy:       "WMAgent",
		CreationDate:         1501192514,
		LastModificationDate: 1501192514,
	}

	processingEra := dbs.ProcessingEra{
		ProcessingVersion: testData.ProcessingVersion,
		CreateBy:          "WMAgent",
		CreationDate:      1501192514,
	}

	acqEra := dbs.AcquisitionEra{
		AcquisitionEraName: testData.AcquisitionEra,
		StartDate:          123456789,
		CreationDate:       1501192514,
	}

	fileCount := 1000

	block := dbs.Block{
		BlockName:      fmt.Sprintf("%s%d", testData.StepchainBlock, factor),
		OriginSiteName: testData.Site,
		FileCount:      int64(fileCount),
		BlockSize:      20122119010,
		CreationDate:   1501192514,
	}

	bulkBlock.DatasetConfigList = append(bulkBlock.DatasetConfigList, algo)
	bulkBlock.PrimaryDataset = primDS
	bulkBlock.Dataset = dataset
	bulkBlock.ProcessingEra = processingEra
	bulkBlock.AcquisitionEra = acqEra
	// bulkBlock.DatasetParentList = []string{testData.ParentStepchainDataset}
	bulkBlock.Block = block

	for i := 0; i < fileCount; i++ {
		f, fileConfig := createFile2(t, i*factor, factor)
		bulkBlock.Files = append(bulkBlock.Files, f)
		bulkBlock.FileConfigList = append(bulkBlock.FileConfigList, fileConfig)
	}

	return bulkBlock
}

// submit a bulkblock to DBSWriter in a goroutine
func submitBulkBlock(t *testing.T, wg *sync.WaitGroup, errs chan<- string, tries int, block dbs.BulkBlocks) {
	retries := tries
	defer wg.Done()
	data, err := json.Marshal(block)
	if err != nil {
		errs <- err.Error()
	}
	reader := bytes.NewReader(data)
	ht := http.DefaultTransport.(*http.Transport).Clone()
	ht.CloseIdleConnections()
	ht.DisableKeepAlives = true
	c := http.Client{Transport: ht}

	resp, err := c.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
	if err != nil {
		errs <- err.Error()
	}
	defer resp.Body.Close()
	t.Logf("Try #%d, Block: %s, response: %d\n", retries, block.Block.BlockName, resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		if retries < 3 {
			wg.Add(1)
			retries++
			t.Logf("Re-Trying for Block: %s", block.Block.BlockName)
			time.Sleep(10 * time.Second)
			go submitBulkBlock(t, wg, errs, retries, block)
		} else {
			msg := fmt.Sprintf("Submit bulkblocks failed on try #%d for block %s", retries, block.Block.BlockName)
			errs <- msg
		}
	}

	//return resp.StatusCode
}

// TestRaceConditions tests for the racing conditions in the DBSWriter bulkblocks API
func TestRaceConditions(t *testing.T) {
	t.Log("Starting tests for race conditions")
	// var BulkBlocksData dbs.BulkBlocks

	t.Cleanup(func() {
		exec.Command("pkill", "dbs2go").Output()
	})

	// load basic data definitions
	testCaseFile := os.Getenv("INTEGRATION_DATA_FILE")
	if testCaseFile == "" {
		t.Fatal("INTEGRATION_DATA_FILE not defined")
	}
	if _, err := os.Stat(testCaseFile); errors.Is(err, os.ErrNotExist) {
		fmt.Println("Generating base data")
		generateBaseData(t, testCaseFile)
	}
	err := readJsonFile(t, testCaseFile, &testData)
	if err != nil {
		t.Fatal(err.Error())
	}

	numBlocks := 3
	var blocks []dbs.BulkBlocks

	for i := 1; i < numBlocks+1; i++ {
		b := generateBulkBlock(t, i)
		blocks = append(blocks, b)
	}

	t.Log("Sleep for debugging")
	// time.Sleep(30 * time.Second)

	t.Run("Insert blocks simultaneously", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan string, 10)

		for _, block := range blocks {
			// time.Sleep(1 * time.Nanosecond)
			wg.Add(1)
			go submitBulkBlock(t, &wg, errs, 1, block)
		}

		go func() {
			wg.Wait()
			close(errs)
		}()

		for err := range errs {
			t.Fatal(err)
		}
	})

}
