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

type StdoutWriter string

func (s StdoutWriter) Header() http.Header {
	return http.Header{}
}
func (s StdoutWriter) Write(b []byte) (int, error) {
	v := string(b)
	fmt.Println(v)
	return len(v), nil
}
func (s StdoutWriter) WriteHeader(statusCode int) {
	fmt.Println("statusCode", statusCode)
}

func initDB() *sql.DB {
	// current directory is a <pwd>/test
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("unable to get current working dir")
	}
	utils.STATICDIR = fmt.Sprintf("%s/../static", dir)
	dbtype := "sqlite3"
	dburi := fmt.Sprintf("%s/dbs.db", dir)
	dbowner := "sqlite"

	db, err := sql.Open(dbtype, dburi)
	//     defer db.Close()
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
