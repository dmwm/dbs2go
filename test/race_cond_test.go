package main

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

// generate a single bulkblock
func generateBulkBlock(t *testing.T, factor int) dbs.BulkBlocks {
	var bulkBlock dbs.BulkBlocks

	algo := dbs.DatasetConfig{
		ReleaseVersion:    testData.ReleaseVersion,
		PsetHash:          testData.PsetHash,
		AppName:           testData.AppName,
		OutputModuleLabel: testData.OutputModuleLabel,
		GlobalTag:         testData.GlobalTag,
	}

	primDS := dbs.PrimaryDataset{
		PrimaryDSName: testData.StepPrimaryDSName,
		PrimaryDSType: "test",
		CreateBy:      "WMAgent",
		CreationDate:  time.Now().Unix(), // Replace with fixed time
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
		CreationDate:         time.Now().Unix(),
		LastModificationDate: time.Now().Unix(),
	}

	processingEra := dbs.ProcessingEra{
		ProcessingVersion: testData.ProcessingVersion,
		CreateBy:          "WMAgent",
	}

	acqEra := dbs.AcquisitionEra{
		AcquisitionEraName: testData.AcquisitionEra,
		StartDate:          123456789,
	}

	fileCount := 50

	block := dbs.Block{
		BlockName:      testData.StepchainBlock,
		OriginSiteName: testData.Site,
		FileCount:      int64(fileCount),
		BlockSize:      20122119010,
	}

	bulkBlock.DatasetConfigList = []dbs.DatasetConfig{algo}
	bulkBlock.PrimaryDataset = primDS
	bulkBlock.Dataset = dataset
	bulkBlock.ProcessingEra = processingEra
	bulkBlock.AcquisitionEra = acqEra
	// bulkBlock.DatasetParentList = []string{testData.ParentStepchainDataset}
	bulkBlock.Block = block

	for i := 0; i < fileCount; i++ {
		f := createFile(t, i*factor)
		bulkBlock.Files = append(bulkBlock.Files, f)
	}

	return bulkBlock
}

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

	numBlocks := 10
	var blocks []dbs.BulkBlocks

	for i := 1; i < numBlocks+1; i++ {
		b := generateBulkBlock(t, i)
		fmt.Printf("%v\n", b)
		blocks = append(blocks, b)
	}

	// time.Sleep(time.Minute)

	var wg sync.WaitGroup

	t.Run("Insert blocks simultaneously", func(t *testing.T) {
		for _, block := range blocks {
			time.Sleep(100 * time.Millisecond)
			wg.Add(1)
			go func(t *testing.T, b dbs.BulkBlocks) {
				defer wg.Done()
				data, err := json.Marshal(b)
				if err != nil {
					t.Error(err)
				}
				reader := bytes.NewReader(data)
				resp, err := http.Post("http://localhost:8990/dbs-one-writer/bulkblocks", "application/json", reader)
				if err != nil {
					t.Error(err)
				}
				t.Logf("%v\n", resp.StatusCode)
			}(t, block)
		}
		wg.Wait()
		fmt.Println("Done")
	})

}
