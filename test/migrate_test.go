package main

import (
	"fmt"
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
)

// TestMigrateGetBlocks
func TestMigrateGetBlocks(t *testing.T) {
	rurl := ""
	if rurl == "" {
		return
	}
	dataset := ""
	blocks, err := dbs.GetBlocks(rurl, dataset)
	if err != nil {
		t.Error("Fail TestMigrateGetBlocks")
	}
	fmt.Printf("url=%s dataset=%s blocks=%v\n", rurl, dataset, blocks)
	blk := ""
	blocks, err = dbs.GetBlocks(rurl, blk)
	if err != nil {
		t.Error("Fail TestMigrateGetBlocks")
	}
	fmt.Printf("url=%s block=%s blocks=%v\n", rurl, blk, blocks)
}

// TestMigrateGetParents
func TestMigrateGetParents(t *testing.T) {
	//     t.Error("Fail TestInList")
}

// TestMigrateGetParentBlocks
func TestMigrateGetParentBlocks(t *testing.T) {
	//     t.Error("Fail TestInList")
}

// TestMigrateGetParentDatasets
func TestMigrateGetParentDatasets(t *testing.T) {
	//     t.Error("Fail TestInList")
}
