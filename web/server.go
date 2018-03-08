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
//   _ "gopkg.in/rana/ora.v4"
//   _ "github.com/mattn/go-oci8"
// MySQL driver:
//   _ "github.com/go-sql-driver/mysql"
// SQLite driver:
//  _ "github.com/mattn/go-sqlite3"
//
package web

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-oci8"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/dbs2go/config"
	"github.com/vkuznet/dbs2go/dbs"
	"github.com/vkuznet/dbs2go/utils"
	_ "gopkg.in/rana/ora.v4"

	logs "github.com/sirupsen/logrus"

	_ "net/http/pprof"
)

// profiler, see https://golang.org/pkg/net/http/pprof/

// UserDNs structure holds information about user DNs
type UserDNs struct {
	DNs  []string
	Time time.Time
}

// global variable which we initialize once
var _userDNs UserDNs

// global variables used in this module
var _tdir string

// var _cmsAuth cmsauth.CMSAuth
func userDNs() []string {
	var out []string
	rurl := "https://cmsweb.cern.ch/sitedb/data/prod/people"
	resp := utils.FetchResponse(rurl, []byte{})
	if resp.Error != nil {
		logs.WithFields(logs.Fields{
			"Error": resp.Error,
		}).Error("Unable to fetch SiteDB records", resp.Error)
		return out
	}
	var rec map[string]interface{}
	err := json.Unmarshal(resp.Data, &rec)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("Unable to unmarshal response", err)
		return out
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	var idx int
	for i, h := range headers {
		if h.(string) == "dn" {
			idx = i
			break
		}
	}
	values := rec["result"].([]interface{})
	for _, item := range values {
		val := item.([]interface{})
		v := val[idx]
		if v != nil {
			out = append(out, v.(string))
		}
	}
	return out
}

// UserDN function parses user Distinguished Name (DN) from client's HTTP request
func UserDN(r *http.Request) string {
	var names []interface{}
	for _, cert := range r.TLS.PeerCertificates {
		for _, name := range cert.Subject.Names {
			switch v := name.Value.(type) {
			case string:
				names = append(names, v)
			}
		}
	}
	if len(names) == 0 {
		return "not-found"
	}
	parts := names[:7]
	return fmt.Sprintf("/DC=%s/DC=%s/OU=%s/OU=%s/CN=%s/CN=%s/CN=%s", parts...)
}

// custom logic for CMS authentication, users may implement their own logic here
func auth(r *http.Request) bool {

	userDN := UserDN(r)
	match := utils.InList(userDN, _userDNs.DNs)
	if !match {
		logs.WithFields(logs.Fields{
			"User DN": userDN,
		}).Error("Auth userDN not found in SiteDB")
	}
	return match
}

// AuthHandler authenticate incoming requests and route them to appropriate handler
func AuthHandler(w http.ResponseWriter, r *http.Request) {
	// check if server started with hkey file (auth is required)
	status := auth(r)
	if !status {
		msg := "You are not allowed to access this resource"
		http.Error(w, msg, http.StatusForbidden)
		return
	}
	RequestHandler(w, r)
}

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

// RequestHandler is used by web server to handle incoming requests
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	// This is an example of cmsAuth used in cmsweb
	// check if server started with hkey file (auth is required)
	//     status := _cmsAuth.CheckAuthnAuthz(r.Header)
	//     if !status {
	//         msg := "You are not allowed to access this resource"
	//         http.Error(w, msg, http.StatusForbidden)
	//         return
	//     }

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
	} else if len(arr) == 2 {
		api = arr[1]
	}
	// TMP for frontend redirect
	if api == "" {
		api = "datasets"
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

// use this signature if we need to use afile
// Server provides HTTPs server for our application
func Server(configFile string) {
	err := config.ParseConfig(configFile)
	if err != nil {
		panic(err)
	}
	utils.VERBOSE = config.Config.Verbose
	utils.STATICDIR = config.Config.StaticDir
	logs.Info(config.Config.String())
	_tdir = fmt.Sprintf("%s/templates", utils.STATICDIR) // template area

	// static content for js/css/images requests
	for _, dir := range []string{"js", "css", "images"} {
		m := fmt.Sprintf("/%s/%s/", config.Config.Base, dir)
		d := fmt.Sprintf("%s/%s", utils.STATICDIR, dir)
		http.Handle(m, http.StripPrefix(m, http.FileServer(http.Dir(d))))
	}

	// dynamic content
	apiMap := dbs.LoadApiMap()
	dbs.APIMAP = apiMap
	for api, endpoint := range apiMap {
		callMethod := fmt.Sprintf("/%s/%s", config.Config.Base, endpoint)
		if utils.VERBOSE > 0 {
			fmt.Printf("map %s API to %v endpoint\n", api, endpoint)
		}
		http.HandleFunc(callMethod, RequestHandler)
	}
	http.HandleFunc(fmt.Sprintf("/%s/", config.Config.Base), RequestHandler)

	// set database connection once
	dbtype, dburi, dbowner := dbs.ParseDBFile(config.Config.DBFile)
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
	dbs.DBOWNER = dbowner

	// setup CMSAuth module
	//     _cmsAuth.Init(afile)

	// start server
	addr := fmt.Sprintf(":%d", config.Config.Port)
	if config.Config.ServerCrt != "" && config.Config.ServerKey != "" {
		// init userDNs
		_userDNs = UserDNs{DNs: userDNs(), Time: time.Now()}
		go func() {
			for {
				d := time.Duration(config.Config.UpdateDNs) * time.Minute
				logs.WithFields(logs.Fields{"Time": time.Now(), "Duration": d}).Info("userDNs are updated")
				time.Sleep(d) // sleep for next iteration
				_userDNs = UserDNs{DNs: userDNs(), Time: time.Now()}
			}
		}()
		// https server and use AuthHandler to allow access to it
		http.HandleFunc("/", AuthHandler)
		server := &http.Server{
			Addr: addr,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequestClientCert,
			},
		}
		logs.WithFields(logs.Fields{"Addr": addr}).Info("Starting HTTPs server")
		err = server.ListenAndServeTLS(config.Config.ServerCrt, config.Config.ServerKey)
	} else {
		// http server on certain port should be used behind frontend, cmsweb way
		logs.WithFields(logs.Fields{"Addr": addr}).Info("Starting HTTP server")
		http.HandleFunc("/", RequestHandler)
		err = http.ListenAndServe(addr, nil)
	}
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
