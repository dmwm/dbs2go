package main

// Helper code to generate large JSON files to be used by bulkblocks DBS API
// go build
// ./dbsdata -fname bulkblock.json -lumis 300000 -pattern 207 -ofile bulkblocks_big.json
// for sqlite testing use
// ./dbsdata -fname bulkblock.json -lumis 3000 -pattern 207 -ofile bulkblocks_sqlitebig.json -drop dataset_parent_list

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/vkuznet/dbs2go/dbs"
)

func main() {
	var fname string
	flag.StringVar(&fname, "fname", "", "input file name of bulkblock json")
	var pattern string
	flag.StringVar(&pattern, "pattern", "", "pattern to change in bulkblock json")
	var ofile string
	flag.StringVar(&ofile, "ofile", "", "output file name of bulkblock json")
	var drop string
	flag.StringVar(&drop, "drop", "", "drop attribute, e.g. dataset_parent_list")
	var lumis int
	flag.IntVar(&lumis, "lumis", 0, "number of unique lumis to generate in bulkblock json")
	flag.Parse()
	run(fname, pattern, lumis, drop, ofile)
}

func run(fname, pattern string, lumis int, drop, ofile string) {
	data, err := os.ReadFile(fname)
	sdata := string(data)
	tstamp := fmt.Sprintf("%d", time.Now().Unix())
	sdata = strings.Replace(sdata, pattern, tstamp, -1)
	data = []byte(sdata)
	var rec dbs.BulkBlocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Fatal(err)
	}
	if drop == "dataset_parent_list" {
		rec.DatasetParentList = []string{}
	}
	fileLumi := rec.Files[0].FileLumiList[0]
	var fileLumiList []dbs.FileLumi
	for i := 0; i < lumis; i++ {
		fl := dbs.FileLumi{
			LumiSectionNumber: int64(i),
			RunNumber:         fileLumi.RunNumber,
			EventCount:        fileLumi.EventCount,
		}
		fileLumiList = append(fileLumiList, fl)
	}
	log.Println("generated", len(fileLumiList), "file lumi list records")
	var files []dbs.File
	for _, f := range rec.Files {
		f.FileLumiList = fileLumiList
		files = append(files, f)
	}
	rec.Files = files
	data, err = json.Marshal(rec)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(ofile, data, 0666); err != nil {
		log.Fatal(err)
	}
}
