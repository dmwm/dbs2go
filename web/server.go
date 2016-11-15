// DBS web server
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
//
// Some links:  http://www.alexedwards.net/blog/golang-response-snippets
//              http://blog.golang.org/json-and-go
// Go patterns: http://www.golangpatterns.info/home
// Templates:   http://gohugo.io/templates/go-templates/
//              http://golang.org/pkg/html/template/
// Go examples: https://gobyexample.com/
// for Go database API: http://go-database-sql.org/overview.html
// Oracle drivers:
//   _ "gopkg.in/rana/ora.v3"
//   _ "github.com/mattn/go-oci8"
// MySQL driver:
//   _ "github.com/go-sql-driver/mysql"
// SQLite driver:
//  _ "github.com/mattn/go-sqlite3"
//
package web

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/cmsauth"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
	_ "gopkg.in/rana/ora.v3"
	"log"
	"net/http"
	"strings"
)

// profiler, see https://golang.org/pkg/net/http/pprof/
import _ "net/http/pprof"

// global variables used in this module
var _tdir string
var _cmsAuth cmsauth.CMSAuth

func processRequest(params dbs.Record) []dbs.Record {
	// defer function will propagate panic message to higher level
	defer utils.ErrPropagate("processRequest")
	if utils.VERBOSE > 0 {
		log.Println("request", params)
	}

	// form response from the server
	if api, ok := params["api"]; ok {
		delete(params, "api") // remove api key from params
		return dbs.GetData(api.(string), params)
	}
	var out []dbs.Record
	return out
}

/*
 * RequestHandler is used by web server to handle incoming requests
 */
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	if utils.CMSAUTH == 1 {
		// check if server started with hkey file (auth is required)
		status := _cmsAuth.CheckAuthnAuthz(r.Header)
		if !status {
			msg := "You are not allowed to access this resource"
			http.Error(w, msg, http.StatusForbidden)
			return
		}
	}

	// TODO: need to implement how to parse input http parameters
	r.ParseForm() // parse url parameters
	if utils.VERBOSE > 2 {
		fmt.Println("Process", r)
	}
	params := make(dbs.Record)
	arr := strings.Split(r.URL.Path, "/") // something like /base/api?param=1
	api := ""
	if len(arr) == 3 {
		api = arr[2]
	}
	params["api"] = api
	for k, v := range r.Form {
		params[k] = v
	}

	// process requests based on the path
	if api == "" {
		tmplData := make(map[string]interface{})
		tmplData["Content"] = "Home page"
		tmplData["User"] = "user"
		page := utils.ParseTmpl(_tdir, "main.tmpl", tmplData)
		w.Write([]byte(page))
		return
	} else {
		// defer function will be fired when following processRequest will panic
		defer func() {
			if err := recover(); err != nil {
				log.Println("ERROR, web server error", err, utils.Stack())
				response := make(map[string]interface{})
				response["status"] = "fail"
				response["reason"] = err
				js, err := json.Marshal(&response)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(js)
				return
			}
		}()

		if r.Method == "GET" {
			// process given query
			response := processRequest(params)
			js, err := json.Marshal(&response)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
			return
		} else if r.Method == "POST" {
			// TODO: need to implement the logic
			response := make(dbs.Record)
			response["status"] = "ok"
			js, err := json.Marshal(&response)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
			return
		}
	}
}

// proxy server. It defines /fetch public interface
func Server(afile, dbfile, base, port string) {
	log.Printf("Start server localhost:%s/%s", port, base)
	_tdir = fmt.Sprintf("%s/templates", utils.STATICDIR) // template area

	// static content for js/css/images requests
	for _, dir := range []string{"js", "css", "images"} {
		m := fmt.Sprintf("/%s/%s/", base, dir)
		d := fmt.Sprintf("%s/%s", utils.STATICDIR, dir)
		http.Handle(m, http.StripPrefix(m, http.FileServer(http.Dir(d))))
	}

	// dynamic content
	apiMap := dbs.LoadApiMap()
	dbs.APIMAP = apiMap
	for api, endpoint := range apiMap {
		callMethod := fmt.Sprintf("/%s/%s", base, endpoint)
		if utils.VERBOSE > 0 {
			fmt.Printf("map %s API to %v endpoint\n", api, endpoint)
		}
		http.HandleFunc(callMethod, RequestHandler)
	}
	http.HandleFunc(fmt.Sprintf("/%s/", base), RequestHandler)

	// set database connection once
	dbtype, dburi, dbowner := dbs.ParseDBFile(dbfile)
	db, dberr := sql.Open(dbtype, dburi)
	defer db.Close()
	if dberr != nil {
		log.Fatal(dberr)
	}
	dberr = db.Ping()
	if dberr != nil {
		log.Fatal(dberr)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	dbs.DB = db
	dbs.DBTYPE = dbtype

	// load DBS SQL statements
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql

	// setup CMSAuth module
	_cmsAuth.Init(afile)

	// start server
	err := http.ListenAndServe(":"+port, nil)
	// NOTE: later this can be replaced with secure connection
	// replace ListenAndServe(addr string, handler Handler)
	// with TLS function
	// ListenAndServeTLS(addr string, certFile string, keyFile string, handler
	// Handler)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
