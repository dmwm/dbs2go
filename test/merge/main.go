package main

// Author Valentin Kuznetsov <vkuznet [AT] gmail {DOT] com >

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-oci8"
)

func main() {
	var dbfile string
	flag.StringVar(&dbfile, "dbfile", "", "dbfile name")
	var nrec int
	flag.IntVar(&nrec, "nrec", 10000, "number of records to isnert")
	var chunk int
	flag.IntVar(&chunk, "chunk", 1000, "chunk size")
	var maxSize int
	flag.IntVar(&maxSize, "maxSize", 100000, "maxSize controls total number of inserted records at once")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "verbose level")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	run(dbfile, nrec, chunk, maxSize, verbose)
}

// ParseDBFile function parses given file name and extracts from it dbtype and dburi
// file should contain the "dbtype dburi" string
func ParseDBFile(dbfile string) (string, string, string) {
	dat, err := ioutil.ReadFile(dbfile)
	if err != nil {
		log.Fatal(err)
	}
	arr := strings.Split(string(dat), " ")
	return arr[0], arr[1], strings.Replace(arr[2], "\n", "", -1)
}

func run(dbfile string, nrec, chunkSize, maxSize, verbose int) {
	dbtype, dburi, _ := ParseDBFile(dbfile)

	db, dberr := sql.Open(dbtype, dburi)
	if dberr != nil {
		log.Fatalf("unable to open %s %s, error %v", dbtype, dburi)
	}
	dberr = db.Ping()
	if dberr != nil {
		log.Fatal(dberr)
	}
	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(100)

	// drop table
	stm := `DROP TABLE FILE_PARENTS`
	_, err := db.Exec(stm)
	if err != nil {
		if verbose > 0 {
			log.Printf("unable to drop table %v", err)
		}
	}

	// create initial table
	stm = `CREATE TABLE FILE_PARENTS (THIS_FILE_ID INTEGER, PARENT_FILE_ID INTEGER)`
	_, err = db.Exec(stm)
	if err != nil {
		if verbose > 0 {
			log.Printf("unable to create table %v", err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	time0 := time.Now()
	// create temp table
	stm = `CREATE PRIVATE TEMPORARY TABLE ORA$PTT_TEMP_FILE_PARENTS (THIS_FILE_ID INTEGER, PARENT_FILE_ID INTEGER) ON COMMIT DROP DEFINITION`
	if verbose > 0 {
		log.Println("execute", stm)
	}

	_, err = tx.Exec(stm)
	if err != nil {
		log.Fatal("unable to create temp table", err)
	}
	log.Println("elapsed time for creation of temp table", time.Since(time0))

	metrics := ProcFSMetrics()
	rss0 := metrics.Rss
	log.Println("metrics RSS", rss0)

	// inject some data into temp table
	if maxSize > nrec {
		maxSize = nrec
	}
	for k := 0; k < nrec; k = k + maxSize {
		t0 := time.Now()
		var wg sync.WaitGroup
		ngoroutines := 0
		for i := k; i < k+maxSize; i = i + chunkSize {
			wg.Add(1)
			size := i + chunkSize
			if size > (k + maxSize) {
				size = k + maxSize
			}
			if size > nrec {
				size = nrec
			}
			if verbose > 1 {
				log.Printf("k=%d i=%d size=%d max=%d nrec=%d", k, i, size, maxSize, nrec)
			}
			go insertChunk(tx, &wg, i, size, verbose)
			ngoroutines += 1
		}
		limit := k + maxSize
		if limit > nrec {
			limit = nrec
		}
		log.Printf("process %d goroutines, step %d-%d, elapsed time %v", ngoroutines, k, limit, time.Since(t0))
		wg.Wait()
	}
	log.Printf("elapsed time for inserting %d records into temp table %v", nrec, time.Since(time0))

	// merge temp table into original one
	stm = `MERGE INTO FILE_PARENTS x
USING (SELECT THIS_FILE_ID, PARENT_FILE_ID FROM ORA$PTT_TEMP_FILE_PARENTS ) y
ON (x.THIS_FILE_ID = y.THIS_FILE_ID AND x.PARENT_FILE_ID = y.PARENT_FILE_ID)
WHEN NOT MATCHED THEN
    INSERT(x.THIS_FILE_ID, x.PARENT_FILE_ID)
    VALUES(y.THIS_FILE_ID, y.PARENT_FILE_ID)`
	if verbose > 0 {
		log.Println("execute", stm)
	}
	_, err = tx.Exec(stm)
	if err != nil {
		log.Fatal("unable to insert all into temp table", err)
	}
	log.Printf("elapsed time for merge step %v", time.Since(time0))

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	metrics = ProcFSMetrics()
	rss := metrics.Rss
	log.Println("metrics RSS", rss)
	log.Println("metrics RSS increase", SizeFormat(rss-rss0))
	log.Println("elapsed time", time.Since(time0))

}

func insertChunk(tx *sql.Tx, wg *sync.WaitGroup, idx, limit, verbose int) {
	defer wg.Done()
	var args []interface{}
	stm := "INSERT ALL"
	for i := idx; i < limit; i++ {
		into := `INTO ORA$PTT_TEMP_FILE_PARENTS (THIS_FILE_ID, PARENT_FILE_ID) VALUES (:fval, :pval)`
		stm = fmt.Sprintf("%s\n%s", stm, into)
		args = append(args, i)
		args = append(args, i)
	}
	stm = fmt.Sprintf("%s\nSELECT * FROM dual", stm)
	if verbose > 1 {
		log.Println("execute", stm)
	}
	if verbose > 0 {
		log.Println("execute INSERT ALL", len(args)/2)
	}
	_, err := tx.Exec(stm, args...)
	if err != nil {
		log.Fatal("unable to insert all into temp table", err)
	}
}
