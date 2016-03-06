/*
 * DBS APIs
 */

package dbs

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"utils"
)

// main record we work with
type Record map[string]interface{}

// global variable to keep pointer to DB
var DB *sql.DB
var DBTYPE string
var DBSQL Record

// helper function to load DBS SQL statements
func LoadSQL(owner string) Record {
	dbsql := make(Record)
	// query statement
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	for _, f := range utils.Listfiles(sdir) {
		k := strings.Split(f, ".")[0]
		dbsql[k] = utils.ParseTmpl(sdir, f, tmplData)
	}
	return dbsql
}

// helper function to get SQL statement from DBSQL dict for a given key
func getSQL(key string) string {
	// use generic query API to fetch the results from DB
	stm, ok := DBSQL[key]
	if !ok {
		msg := fmt.Sprintf("Unable to load %s SQL", key)
		log.Fatal(msg)
	}
	return stm.(string)
}

// Function to access internal back-end and return records for provided
// api and params
func GetData(api string, params Record) []Record {
	var data []Record
	switch api {
	case "blocks":
		data = blocks(params)
	case "datasets":
		data = datasets(params)
	}
    return data
}

// helper function to get value from record
func getValues(params Record, key string) []string {
	var out []string
	val, ok := params[key]
	if ok {
		values := val.([]string)
		return values
	}
	return out
}

// helper function to get single value from a record
func getSingleValue(params Record, key string) string {
	values := getValues(params, key)
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

// function to parse given file name and extract from it dbtype and dburi
// file should contain the "dbtype dburi" string
func ParseDBFile(dbfile string) (string, string, string) {
	dat, err := ioutil.ReadFile(dbfile)
	if err != nil {
		log.Fatal(err)
	}
	arr := strings.Split(string(dat), " ")
	return arr[0], arr[1], strings.Replace(arr[2], "\n", "", -1)
}

func placeholder(pholder string) string {
	if DBTYPE == "ora"  || DBTYPE == "oci8" {
		return fmt.Sprintf(":%s", pholder)
	} else if DBTYPE == "PostgreSQL" {
		return fmt.Sprintf("$%s", pholder)
	} else {
		return "?"
	}
}

// generic API to execute given statement
// ideas are taken from
// http://stackoverflow.com/questions/17845619/how-to-call-the-scan-variadic-function-in-golang-using-reflection
func execute(stm string, args ...interface{}) []Record {
	var out []Record

//    var rows *sql.Rows
//    var err error
    if utils.VERBOSE > 1 {
        fmt.Println(stm, args)
    }
//    if len(args) == 1 {
//        rows, err = DB.Query(stm, args[0])
//    } else {
//        rows, err = DB.Query(stm, args...)
//    }
    rows, err := DB.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("ERROR: DB.Query, query='%s' args='%v' error=%v", stm, args, err)
		log.Fatal(msg)
	}
	defer rows.Close()

	// extract columns from Rows object and create values & valuesPtrs to retrieve results
	columns, _ := rows.Columns()
    var cols []string
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
    rowCount := 0

	for rows.Next() {
        if rowCount == 0 {
            // initialize value pointers
            for i, _ := range columns {
                valuePtrs[i] = &values[i]
            }
        }
		err := rows.Scan(valuePtrs...)
		if err != nil {
			msg := fmt.Sprintf("ERROR: rows.Scan, dest='%v', error=%v", valuePtrs, err)
			log.Fatal(msg)
		}
        rowCount += 1
		// store results into generic record (a dict)
		rec := make(Record)
		for i, col := range columns {
            if len(cols) != len(columns) {
                cols = append(cols, strings.ToLower(col))
            }
			val := values[i]
			b, ok := val.([]byte)
			if ok {
                rec[cols[i]] = string(b)
			} else {
                rec[cols[i]] = val
			}
		}
		out = append(out, rec)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return out
}
