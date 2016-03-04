/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DBS web server, it handles all DBS requests
 * Created    : Mon Feb 29 13:52:06 EST 2016
 *
 * Some links:  http://www.alexedwards.net/blog/golang-response-snippets
 *              http://blog.golang.org/json-and-go
 * Go patterns: http://www.golangpatterns.info/home
 * Templates:   http://gohugo.io/templates/go-templates/
 *              http://golang.org/pkg/html/template/
 * Go examples: https://gobyexample.com/
 * for Go database API: http://go-database-sql.org/overview.html
 * Oracle drivers:
	 _ "gopkg.in/rana/ora.v3"
	 _ "github.com/mattn/go-oci8"
 * MySQL driver:
     _ "github.com/go-sql-driver/mysql"
*/
package web

import (
	"database/sql"
	"dbs"
	"encoding/json"
	"fmt"
	//     _ "github.com/go-sql-driver/mysql"
//    _ "github.com/mattn/go-sqlite3"
	_ "gopkg.in/rana/ora.v3"
	"log"
	"net/http"
	"strings"
	"utils"
)

// profiler
import _ "net/http/pprof"

// global variables used in this module
var _afile, _tdir string

func processRequest(params dbs.Record) []dbs.Record {
	// defer function will propagate panic message to higher level
	defer utils.ErrPropagate("processRequest")
	if utils.VERBOSE > 0 {
		log.Println("request", params)
	}

	// form response from the server
	if api, ok := params["api"]; ok {
		delete(params, "api") // remove api key from params
		data := dbs.GetData(api.(string), params)
        return data
	}
    var out []dbs.Record
	return out
}

/*
 * RequestHandler is used by web server to handle incoming requests
 */
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	// check if server started with hkey file (auth is required)
//    if len(_afile) > 0 {
//        status := checkAuthnAuthz(r.Header)
//        if !status {
//            msg := "You are not allowed to access this resource"
//            http.Error(w, msg, http.StatusForbidden)
//            return
//        }
//    }

	// TODO: need to implement how to parse input http parameters
	r.ParseForm() // parse url parameters
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
		}
	}
}

// proxy server. It defines /fetch public interface
func Server(afile, dbfile, base, port string) {
	log.Printf("Start server localhost:%s/%s", port, base)
	_afile = afile                                       // location of auth file
	_tdir = fmt.Sprintf("%s/templates", utils.STATICDIR) // template area

	// static content for js/css/images requests
	for _, dir := range []string{"js", "css", "images"} {
		m := fmt.Sprintf("/%s/%s/", base, dir)
		d := fmt.Sprintf("%s/%s", utils.STATICDIR, dir)
		http.Handle(m, http.StripPrefix(m, http.FileServer(http.Dir(d))))
	}

	// dynamic content
	apis := []string{"datasets", "blocks"} // list of DBS apis
	for _, api := range apis {
		callMethod := fmt.Sprintf("/%s/%s", base, api)
		http.HandleFunc(callMethod, RequestHandler)
	}
	http.HandleFunc(fmt.Sprintf("/%s/", base), RequestHandler)

	// set database connection once
	dbtype, dburi, dbowner := dbs.ParseDBFile(dbfile)
	db, dberr := sql.Open(dbtype, dburi)
	if dberr != nil {
		log.Fatal(dberr)
	}
	dbs.DB = db
	dbs.DBTYPE = dbtype
	defer db.Close()

	// load DBS SQL statements
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql

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
