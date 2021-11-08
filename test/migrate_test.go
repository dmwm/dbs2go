package main

import (
	"fmt"
	"log"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// TestMigrateGetBlocks
func TestMigrateGetBlocks(t *testing.T) {
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	//     parentDataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW"
	dataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM"
	blocks, err := dbs.GetBlocks(rurl, dataset)
	if err != nil {
		t.Error("Fail TestMigrateGetBlocks")
	}
	fmt.Printf("url=%s dataset=%s blocks=%v\n", rurl, dataset, blocks)
	if len(blocks) != 1 {
		t.Error("Wrong number of expected blocks")
	}
	blk := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM#e9b596e0-25b1-4c17-a628-9d9964be123a"
	if blocks[0] != blk {
		t.Error("Unexpected block")
	}
	blocks, err = dbs.GetBlocks(rurl, blk)
	if err != nil {
		t.Error("Fail TestMigrateGetBlocks")
	}
	fmt.Printf("url=%s block=%s blocks=%v\n", rurl, blk, blocks)
	if len(blocks) != 1 {
		t.Error("Wrong number of expected blocks")
	}
	if blocks[0] != blk {
		t.Error("Unexpected block")
	}
}

// TestMigrateGetParents
func TestMigrateGetParents(t *testing.T) {
	//     t.Error("Fail TestInList")
}

// TestMigrateGetParentBlocks
func TestMigrateGetParentBlocks(t *testing.T) {
	blk := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM#e9b596e0-25b1-4c17-a628-9d9964be123a"
	parents := []string{
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW#15f769b1-a371-4f5d-8d0f-d9c4a6723869",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW#53c10dee-274d-412a-82ca-6f925ac8ed72",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIFall18GS-SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM#a52529ca-c902-45c9-a372-0fadaf96a159",
		"/ZMM_13TeV_TuneCP5-pythia8/RunIIFall18GS-SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM#a52529ca-c902-45c9-a372-0fadaf96a159",
	}
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	utils.Localhost = "http://localhost:9898"
	utils.VERBOSE = 2
	log.SetFlags(0)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	result, err := dbs.GetParentBlocks(rurl, blk)
	if err != nil {
		t.Error("unable to get parent blocks, error", err)
	}
	fmt.Println("expect", parents)
	fmt.Println("result", result)
	for _, blk := range parents {
		if !utils.InList(blk, result) {
			t.Error("block", blk, "not found in result list")
		}
	}
}

// TestMigrateGetParentDatasets
func TestMigrateGetParentDatasets(t *testing.T) {
	rurl := "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
	if rurl == "" {
		return
	}
	parentDataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/GEN-SIM-RAW"
	dataset := "/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNBHP_SNB_HP_102X_upgrade2018_realistic_v17-v2/AODSIM"
	datasets, err := dbs.GetParents(rurl, dataset)
	if err != nil {
		t.Error("Fail TestMigrateGetParentDatasets")
	}
	if len(datasets) != 1 {
		t.Error("Wrong number of expected datasets")
	}
	if datasets[0] != parentDataset {
		t.Error("Unexpected dataset")
	}
}
