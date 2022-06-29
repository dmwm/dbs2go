package main

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

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
	dburi := os.Getenv("DBS_DB_FILE")
	if dburi == "" {
		log.Fatal("DBS_DB_FILE not defined")
	}
	db := initDB(false, dburi)
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

// BenchmarkInList
func BenchmarkInList(b *testing.B) {
	utils.VERBOSE = 0
	N := 1000
	list := make([]int, N)
	for i := 0; i < N; i++ {
		list[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		elem := rand.Intn(N)
		res := utils.InList(elem, list)
		if res != true {
			b.Fatal("unable to find random element in a list")
		}
	}
}

// BenchmarkEqual
func BenchmarkEqual(b *testing.B) {
	utils.VERBOSE = 0
	N := 1000
	list := make([]int, N)
	for i := 0; i < N; i++ {
		list[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		set1 := utils.Set(list)
		set2 := utils.Set(list)
		res := utils.Equal(set1, set2)
		if res != true {
			b.Fatal("unable to compare sets")
		}
	}
}
