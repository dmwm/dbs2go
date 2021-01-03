// DBS APIs
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
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
		stm := utils.ParseTmpl(sdir, f, tmplData)
		if owner == "sqlite" {
			stm = strings.Replace(stm, "sqlite.", "", -1)
		}
		dbsql[k] = stm
		//         dbsql[k] = utils.ParseTmpl(sdir, f, tmplData)
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

// helper function to generate error record
func errorRecord(msg string) []Record {
	var out []Record
	erec := make(Record)
	erec["error"] = msg
	out = append(out, erec)
	return out
}

// generic API to execute given statement
// ideas are taken from
// http://stackoverflow.com/questions/17845619/how-to-call-the-scan-variadic-function-in-golang-using-reflection
// here we use http response writer in order to make encoder
// then we literally stream data with our encoder (i.e. write records
// to writer)
func executeAll(w http.ResponseWriter, stm string, args ...interface{}) (int64, error) {
	var size int64
	var enc *json.Encoder
	if w != nil {
		enc = json.NewEncoder(w)
		w.Write([]byte("[\n"))
		defer w.Write([]byte("]\n"))
	}

	if utils.VERBOSE > 1 {
		log.Printf(stm, args...)
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement=%v error=%v", stm, err)
		return 0, errors.New(msg)
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
			msg := fmt.Sprintf("unabelt to scan DB results %s", err)
			return 0, errors.New(msg)
		}
		if rowCount != 0 && w != nil {
			w.Write([]byte(",\n"))
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
				rec[cols[i]] = val
			}
		}
		if w != nil {
			err = enc.Encode(rec)
			if err != nil {
				return 0, err
			}
		}
		s, e := utils.RecordSize(rec)
		if e == nil {
			size += s
		}
	}
	if err = rows.Err(); err != nil {
		msg := fmt.Sprintf("rows error %v", err)
		return 0, errors.New(msg)
	}
	return size, nil
}

// similar to executeAll function but it takes explicit set of columns and values
func execute(w http.ResponseWriter, stm string, cols []string, vals []interface{}, args ...interface{}) (int64, error) {
	var size int64
	var enc *json.Encoder
	if w != nil {
		enc = json.NewEncoder(w)
		w.Write([]byte("[\n"))
		defer w.Write([]byte("]\n"))
	}

	if utils.VERBOSE > 1 {
		log.Println(stm, args)
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to obtain transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("DB.Query, query='%s' args='%v' error=%v", stm, args, err)
		return 0, errors.New(msg)
	}
	defer rows.Close()

	// loop over rows
	rowCount := 0
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			msg := fmt.Sprintf("rows.Scan, vals='%v', error=%v", vals, err)
			return 0, errors.New(msg)
		}
		if rowCount != 0 && w != nil {
			w.Write([]byte(",\n"))
		}
		rowCount += 1
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
				rec[cols[i]] = val
			}
		}
		if w != nil {
			err = enc.Encode(rec)
			if err != nil {
				return 0, err
			}
		}
		s, e := utils.RecordSize(rec)
		if e == nil {
			size += s
		}
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	return size, nil
}

// helper function to execute sessions
func executeSessions(tx *sql.Tx, sessions []string) error {
	// sessions should be executed only for ORACLE backend
	if !utils.ORACLE {
		return nil
	}
	for _, s := range sessions {
		_, err := tx.Exec(s)
		if err != nil {
			log.Println("DB error", s, err)
			return err
		}
	}
	return nil
}

// insert api to insert data into DB
func insert(stm string, vals []interface{}) error {
	if utils.VERBOSE > 1 {
		log.Printf("%s %+v\n", stm, vals)
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to obtain transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	_, err = tx.Exec(stm, vals...)
	err = tx.Commit()
	if err != nil {
		log.Println("DB error", stm, err)
		tx.Rollback()
		return err
	}
	return nil
}
