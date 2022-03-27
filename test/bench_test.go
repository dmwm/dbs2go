package main

import (
	"testing"

	"github.com/dmwm/dbs2go/dbs"
	"github.com/dmwm/dbs2go/utils"
)

// BenchmarkRecordSize
func BenchmarkRecordSize(b *testing.B) {
	utils.VERBOSE = 0
	rec := make(map[string]int)
	rec["a"] = 1
	rec["b"] = 2
	for i := 0; i < b.N; i++ {
		utils.RecordSize(rec)
	}
}

// BenchmarkLoadTemplateSQL
func BenchmarkLoadTemplateSQL(b *testing.B) {
	// initialize DB for testing
	db := initDB(false)
	utils.VERBOSE = 0
	defer db.Close()

	rec := make(dbs.Record)
	rec["a"] = 1
	rec["b"] = 2
	for i := 0; i < b.N; i++ {
		dbs.LoadTemplateSQL("blocks", rec)
	}
}

// BenchmarkUpdateOrderedDict
func BenchmarkUpdateOrderedDict(b *testing.B) {
	utils.VERBOSE = 0
	blocks := []string{"aaaaaa", "bbbbbb", "cccccc", "dddddd"}
	omap := make(map[int][]string)
	for i := 0; i < 100; i++ {
		omap[i] = blocks
	}
	nmap := make(map[int][]string)
	for i := 50; i < 120; i++ {
		nmap[i] = blocks
	}
	for i := 0; i < b.N; i++ {
		utils.UpdateOrderedDict(omap, nmap)
	}
}
