// DBS APIs
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
package dbs

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/vkuznet/dbs2go/utils"
)

// use API struct for reflection
type API struct{}

// main record we work with
type Record map[string]interface{}

// global variable to keep pointer to DB
var DB *sql.DB
var DBTYPE string
var DBSQL Record
var DBOWNER string

// helper function to load DBS SQL statements
func LoadSQL(owner string) Record {
	dbsql := make(Record)
	// query statement
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	log.Println("sql area", sdir)
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

// helper function to add condition to where clause
func addCond(where, cond string) string {
	w := strings.Trim(where, " ")
	if w == "WHERE" || w == "where" {
		return fmt.Sprintf(" %s", cond)
	}
	return fmt.Sprintf(" AND %s", cond)
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
	if DBTYPE == "ora" || DBTYPE == "oci8" {
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
func executeAll(stm string, args ...interface{}) []Record {
	var out []Record

	if utils.VERBOSE > 1 {
		log.Println(stm, args)
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("fail to obtain transaction, %s", err)
		return errorRecord(msg)
	}
	defer tx.Rollback()
	//     rows, err := DB.Query(stm, args...)
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("DB.Query, query='%s' args='%v' error=%v", stm, args, err)
		return errorRecord(msg)
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
			msg := fmt.Sprintf("rows.Scan, dest='%v', error=%v", valuePtrs, err)
			return errorRecord(msg)
		}
		rowCount += 1
		// store results into generic record (a dict)
		rec := make(Record)
		for i, col := range columns {
			if len(cols) != len(columns) {
				cols = append(cols, strings.ToLower(col))
			}
			vvv := values[i]
			switch val := vvv.(type) {
			case *sql.NullString:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			case *sql.NullInt64:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			case *sql.NullFloat64:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			case *sql.NullBool:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			default:
				//                 fmt.Printf("SQL result: %v (%T) %v (%T)\n", vvv, vvv, val, val)
				rec[cols[i]] = val
			}
			//             rec[cols[i]] = values[i]
		}
		out = append(out, rec)
	}
	if err = rows.Err(); err != nil {
		return errorRecord(fmt.Sprintf("unable to scan rows %v", err))
	}
	return out
}

// similar to executeAll function but it takes explicit set of columns and values
func execute(stm string, cols []string, vals []interface{}, args ...interface{}) []Record {
	var out []Record

	if utils.VERBOSE > 1 {
		log.Println(stm, args)
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to obtain transaction %v", err)
		errorRecord(msg)
	}
	defer tx.Rollback()
	//     rows, err := DB.Query(stm, args...)
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("DB.Query, query='%s' args='%v' error=%v", stm, args, err)
		return errorRecord(msg)
	}
	defer rows.Close()

	// loop over rows
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			msg := fmt.Sprintf("rows.Scan, vals='%v', error=%v", vals, err)
			return errorRecord(msg)
		}
		rec := make(Record)
		for i, _ := range cols {
			vvv := vals[i]
			switch val := vvv.(type) {
			case *sql.NullString:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			case *sql.NullInt64:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			case *sql.NullFloat64:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			case *sql.NullBool:
				v, e := val.Value()
				if e == nil {
					rec[cols[i]] = v
				}
			default:
				//                 fmt.Printf("SQL result: %v (%T) %v (%T)\n", vvv, vvv, val, val)
				rec[cols[i]] = val
			}
			//             rec[cols[i]] = vals[i]
		}
		out = append(out, rec)
	}
	if err = rows.Err(); err != nil {
		return errorRecord(fmt.Sprintf("unable to scan rows: %v", err))
	}
	return out
}

// helper function to generate error record
func errorRecord(msg string) []Record {
	var out []Record
	erec := make(Record)
	erec["error"] = msg
	out = append(out, erec)
	return out
}
