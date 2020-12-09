package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// StdoutWrite provides the same functionality as http.ResponseWriter
// to cover unit tests of DBS APIs. It prints given data directly to stdout.
type StdoutWriter string

// Header implements Header() API of http.ResponseWriter interface
func (s StdoutWriter) Header() http.Header {
	return http.Header{}
}

// Write implements Write API of http.ResponseWriter interface
func (s StdoutWriter) Write(b []byte) (int, error) {
	v := string(b)
	fmt.Println(v)
	return len(v), nil
}

// WriteHeader implements WriteHeader API of http.ResponseWriter interface
func (s StdoutWriter) WriteHeader(statusCode int) {
	fmt.Println("statusCode", statusCode)
}

// helper function to initialize DB for tests
func initDB() *sql.DB {
	// current directory is a <pwd>/test
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("unable to get current working dir")
	}
	utils.STATICDIR = fmt.Sprintf("%s/../static", dir)
	utils.VERBOSE = 1
	dbtype := "sqlite3"
	dburi := "/tmp/dbs-test.db"
	dbowner := "sqlite"

	db, err := sql.Open(dbtype, dburi)
	if err != nil {
		log.Fatal("unable to open db file", err)
	}
	dbs.DB = db
	dbs.DBTYPE = dbtype
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql
	dbs.DBOWNER = dbowner
	return db
}
