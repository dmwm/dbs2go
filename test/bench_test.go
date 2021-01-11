package main

import (
	"testing"

	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// BenchmarkRecordSize
func BenchmarkRecordSize(b *testing.B) {
	rec := make(map[string]int)
	rec["a"] = 1
	rec["b"] = 2
	for i := 0; i < b.N; i++ {
		utils.RecordSize(rec)
	}
}

// BenchmarkLoadTemplateSQL
func BenchmarkLoadTemplateSQL(b *testing.B) {
	rec := make(dbs.Record)
	rec["a"] = 1
	rec["b"] = 2
	for i := 0; i < b.N; i++ {
		dbs.LoadTemplateSQL("blocks", rec)
	}
}
