package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	validator "github.com/go-playground/validator/v10"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
)

// helper function to initialize DB for tests
func initDB(dryRun bool) *sql.DB {
	log.SetFlags(0)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// current directory is a <pwd>/test
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("unable to get current working dir")
	}
	utils.STATICDIR = fmt.Sprintf("%s/../static", dir)
	utils.VERBOSE = 2
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
	if dryRun {
		dbs.DRYRUN = true
	}
	// init validator
	dbs.RecordValidator = validator.New()
	return db
}
