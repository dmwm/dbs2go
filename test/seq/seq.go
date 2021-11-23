package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	_ "github.com/mattn/go-oci8"
)

var DBOWNER string

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

// IncrementSequences API
func IncrementSequences(tx *sql.Tx, seq string, n int) ([]int64, error) {
	log.Println("increment", seq, n)
	var out []int64
	var pid float64
	for i := 0; i < n; i++ {
		stm := fmt.Sprintf("select %s.%s.nextval as val from dual", DBOWNER, seq)
		err := tx.QueryRow(stm).Scan(&pid)
		if err != nil {
			msg := fmt.Sprintf("fail to increment sequence, query='%s' error=%v", stm, err)
			return out, errors.New(msg)
		}
		out = append(out, int64(pid))
	}
	return out, nil
}

func run(dbfile, seq string, n int) error {
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

	// start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return err
	}
	defer tx.Rollback()
	numbers, err := IncrementSequences(tx, seq, n)
	if err != nil {
		log.Println("fail to increment", err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return err
	}
	for _, s := range numbers {
		log.Printf("seq number %d\n", s)
	}
	return nil
}

func main() {
	var dbfile string
	flag.StringVar(&dbfile, "dbfile", "", "dbfile name")
	var dbowner string
	flag.StringVar(&dbowner, "dbowner", "", "dbowner name")
	var seq string
	flag.StringVar(&seq, "seq", "", "seq name")
	var nseq int
	flag.IntVar(&nseq, "nseq", 0, "generate n numbers")
	flag.Parse()
	DBOWNER = dbowner
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("dbfile", dbfile, "seq", seq, "numbers", nseq)
	run(dbfile, seq, nseq)
}
