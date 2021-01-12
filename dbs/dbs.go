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

// DRYRUN allows to skip query execution and printout DB statements along with passed parameters
var DRYRUN bool

// helper function to load DBS SQL templated statements
func LoadTemplateSQL(tmpl string, tmplData Record) string {
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	if !strings.HasSuffix(tmpl, ".sql") {
		tmpl += ".sql"
	}
	stm := utils.ParseTmpl(sdir, tmpl, tmplData)
	if owner, ok := tmplData["Owner"]; ok && owner == "sqlite" {
		stm = strings.Replace(stm, "sqlite.", "", -1)
	}
	return stm
}

// helper function to load DBS SQL statements with Owner
func LoadSQL(owner string) Record {
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	log.Println("sql area", sdir)
	dbsql := make(Record)
	for _, f := range utils.Listfiles(sdir) {
		k := strings.Split(f, ".")[0]
		stm := utils.ParseTmpl(sdir, f, tmplData)
		if owner, ok := tmplData["Owner"]; ok && owner == "sqlite" {
			stm = strings.Replace(stm, "sqlite.", "", -1)
		}
		dbsql[k] = stm
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
func getSingleValue(params Record, key string) (string, error) {
	values := getValues(params, key)
	if len(values) > 0 {
		return values[0], nil
	}
	return "", errors.New(fmt.Sprintf("no list is allowed for provided key: %s", key))
}

// WhereClause function construct proper SQL statement from given statement and list of conditions
func WhereClause(stm string, conds []string) string {
	if len(conds) == 0 {
		return strings.Trim(stm, " ")
	}
	if strings.Contains(stm, " WHERE") {
		stm = fmt.Sprintf(" %s %s", stm, strings.Join(conds, " AND "))
	} else {
		stm = fmt.Sprintf("%s WHERE %s", stm, strings.Join(conds, " AND "))
	}
	return strings.Trim(stm, " ")
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

// CleanStatement cleans the given SQL statement to remove empty strings, etc.
func CleanStatement(stm string) string {
	var out []string
	for _, s := range strings.Split(stm, "\n") {
		//         s = strings.Trim(s, " ")
		if s == "" || s == " " {
			continue
		}
		out = append(out, s)
	}
	stm = strings.Join(out, "\n")
	return stm
}

// generic API to execute given statement
// ideas are taken from
// http://stackoverflow.com/questions/17845619/how-to-call-the-scan-variadic-function-in-golang-using-reflection
// here we use http response writer in order to make encoder
// then we literally stream data with our encoder (i.e. write records
// to writer)
func executeAll(w http.ResponseWriter, stm string, args ...interface{}) (int64, error) {
	stm = CleanStatement(stm)
	if DRYRUN {
		log.Printf("\n### SQL statement ###\n%s\n### SQL arguments ###\n%+v", stm, args)
		return 0, nil
	}
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
	stm = CleanStatement(stm)
	if DRYRUN {
		log.Printf("\n### SQL statement ###\n%s\n### SQL arguments ###\n%+v", stm, args)
		return 0, nil
	}
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
	if err != nil {
		log.Println("DB error", stm, err)
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Println("DB error", stm, err)
		tx.Rollback()
		return err
	}
	return nil
}

// helper function to generate operator, value pair for given argument
func OperatorValue(arg string) (string, string) {
	op := "="
	val := arg
	if strings.Contains(arg, "*") {
		op = "like"
		val = strings.Replace(arg, "*", "%", -1)
	}
	return op, val
}

// ParseRuns parse run_num parameter and convert it to run list
func ParseRuns(runs []string) ([]string, error) {
	var out []string
	for _, v := range runs {
		if matched := intPattern.MatchString(v); matched {
			out = append(out, v)
		} else if matched := runRangePattern.MatchString(v); matched {
			out = append(out, v)
		} else if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
			runs := strings.Replace(v, "[", "", -1)
			runs = strings.Replace(runs, "]", "", -1)
			for _, r := range strings.Split(runs, ",") {
				run := strings.Trim(r, " ")
				log.Println("input", r)
				out = append(out, run)
			}
		} else {
			err := errors.New(fmt.Sprintf("invalid run input parameter %s", v))
			return out, err
		}
	}
	return out, nil
}

// TokenGenerator creates a SQL token generator statement
// https://betteratoracle.com/posts/20-how-do-i-bind-a-variable-in-list
func TokenGenerator(runs []string, limit int) (string, []string) {
	stm := "WITH TOKEN_GENERATOR AS (\n"
	var tstm []string
	var vals []string
	for idx, chunk := range GetChunks(runs, limit) {
		t := fmt.Sprintf("token_%d", idx)
		s := fmt.Sprintf("\tSELECT REGEXP_SUBSTR(:%s, '[^,]+', 1, LEVEL) token ", t)
		s += "\n\tFROM DUAL\n"
		s += fmt.Sprintf("\tCONNECT BY LEVEL <= length(:%s) - length(REPLACE(:%s, ',', '')) + 1", t, t)
		tstm = append(tstm, s)
		vals = append(vals, chunk)
	}
	stm += strings.Join(tstm, " UNION ALL ")
	stm += "\n)"
	return stm, vals
}

// helper function to get ORACLE chunks from provided list of values
func GetChunks(vals []string, limit int) []string {
	var chunks []string
	if len(vals) < limit {
		return []string{strings.Join(vals, ",")}
	}
	idx := 0
	exit := false
	for {
		end := idx + limit
		if end > len(vals) {
			end = len(vals)
			exit = true
		}
		chunk := strings.Join(vals[idx:end], ",")
		chunks = append(chunks, chunk)
		idx = end
		if exit {
			break
		}
	}
	return chunks
}

// helper function to create runs where clause
// given list of runs values should be converted into OR clauses
// it return token generator, where clause and where caluse parameters
func runsClause(table string, runs []string) (string, string, []string) {
	var args []string    // output bind arguments
	var where string     // output where clause
	var runList []string // list of run numbers
	var conds []string   // list of conditions with run numbers
	for _, r := range runs {
		if strings.Contains(r, "-") { // run-range argument
			cond := fmt.Sprintf(" %s.RUN_NUM between %s and %s ", table, placeholder("minrun"), placeholder("maxrun"))
			conds = append(conds, cond)
			rr := strings.Split(r, "-")
			args = append(args, rr[0])
			args = append(args, rr[1])
		} else { // plain run numbers we store to run list
			runList = append(runList, r)
		}
	}
	// take run list and generate token statement
	stm := fmt.Sprintf("%s.RUN_NUM in (SELECT TOKEN FROM TOKEN_GENERATOR)", table)
	token, binds := TokenGenerator(runList, 4000) // 4000 is hard ORACLE limit
	conds = append(conds, stm)
	for _, v := range binds {
		args = append(args, v)
	}
	where = fmt.Sprintf("( %s )", strings.Join(conds, " OR "))
	return token, where, args
}

// helper function to get attribute ID from a given table
func GetID(table, id, attr, value string) (int64, error) {
	stm := fmt.Sprintf("SELECT T.%s FROM %s.%s T WHERE T.%s = ?", id, DBOWNER, table, attr)
	if DBOWNER == "sqlite" {
		stm = strings.Replace(stm, "sqlite.", "", -1)
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()
	var args []interface{}
	args = append(args, value)
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement=%v error=%v", stm, err)
		return 0, errors.New(msg)
	}
	defer rows.Close()
	var rid int64
	for rows.Next() {
		if err := rows.Scan(&rid); err != nil {
			return 0, errors.New(fmt.Sprintf("rows.Scan, error=%v", err))
		}
		break
	}
	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return 0, errors.New(fmt.Sprintf("rows.Err, error=%v", err))
	}
	return rid, nil
}

// AddParam adds single parameter to SQL statement
func AddParam(name, sqlName string, params Record, conds []string, args []interface{}) ([]string, []interface{}) {
	vals := getValues(params, name)
	if len(vals) == 1 {
		op, val := OperatorValue(vals[0])
		cond := fmt.Sprintf(" %s %s %s", sqlName, op, placeholder(name))
		conds = append(conds, cond)
		args = append(args, val)
	}
	return conds, args
}
