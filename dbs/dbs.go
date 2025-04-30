// DBS APIs
// Copyright (c) 2015 - Valentin Kuznetsov <vkuznet@gmail.com>
// some docs:
// goland SQL: https://golang.org/pkg/database/sql/
// golang SQL transactions: https://www.sohamkamani.com/golang/sql-transactions/

package dbs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/dbs2go/utils"
	validator "github.com/go-playground/validator/v10"
)

// API structure represents DBS API. Each API has reader (to read
// HTTP POST payload), HTTP writer to write results back to client,
// HTTP context, input HTTP GET paramers, separator for writer,
// create by and api string values passed at run-time.
type API struct {
	Reader    io.Reader           // reader to read data payload
	Writer    http.ResponseWriter // writer to write results back to client
	Context   context.Context     // HTTP context
	Params    Record              // HTTP input parameters
	Separator string              // string separator for ndjson format
	CreateBy  string              // create by value from run-time
	Api       string              // api name
}

// String provides string representation of API struct
func (a *API) String() string {
	return fmt.Sprintf(
		"API=%s params=%+v createBy=%s separator='%s'",
		a.Api, a.Params, a.CreateBy, a.Separator)
}

// RecordValidator pointer to validator Validate method
var RecordValidator *validator.Validate

// Record represents DBS record
type Record map[string]interface{}

// DB represents sql DB pointer
var DB *sql.DB

// DBTYPE represents DBS DB type, e.g. ORACLE or SQLite
var DBTYPE string

// DBSQL represents DBS SQL record
var DBSQL Record

// DBOWNER represents DBS DB owner
var DBOWNER string

// DRYRUN allows to skip query execution and printout DB statements along with passed parameters
var DRYRUN bool

// FileLumiChunkSize controls chunk size for FileLumi list insertion
var FileLumiChunkSize int

// FileLumiMaxSize controls max size for FileLumi list insertion
var FileLumiMaxSize int

// FileLumiInsertMethod controls which method to use for insertion of FileLumi list
var FileLumiInsertMethod string

// ConcurrentBulkBlocks defines if code should use concurrent bulkblocks API
var ConcurrentBulkBlocks bool

// ConcurrentHashSize defines size of hash to use to encode concurrent requests
var ConcurrentHashSize int

// DBRecord interface represents general DB record used by DBS APIs.
// Each DBS API represents specific Table in back-end DB. And, each individual
// DBS API implements logic for its own DB records
type DBRecord interface {
	Insert(tx *sql.Tx) error  // used to insert given record to DB
	Validate() error          // used to validate given record
	SetDefaults()             // used to set proper defaults for given record
	Decode(r io.Reader) error // used to decode given record
}

// DecodeValidatorError provides uniform error representation
// of DBRecord validation errors
func DecodeValidatorError(r, err interface{}) error {
	if err != nil {
		msg := fmt.Sprintf("DBS structure")
		d, e := json.MarshalIndent(r, "", "   ")
		if e == nil {
			msg = fmt.Sprintf("%s JSON representation\n%v", msg, string(d))
		} else {
			msg = fmt.Sprintf("%s\n%+v", msg, r)
		}
		for _, err := range err.(validator.ValidationErrors) {
			msg = fmt.Sprintf(
				"%s\nkey=%v type=%v value=%v, constrain %v %v",
				msg, err.Field(), err.Type(), err.Value(), err.ActualTag(), err.Param())
		}
		log.Println(msg)
		return Error(ValidationErr, ValidateErrorCode, msg, "dbs.DecodeValidatorError")
	}
	return nil
}

// Date provides default date for DB records
func Date() int64 {
	return time.Now().Unix()
}

// helper function to insert DB record with given reader
func insertRecord(rec DBRecord, r io.Reader) error {
	err := rec.Decode(r)
	if err != nil {
		msg := fmt.Sprintf("fail to decode record")
		log.Println(msg)
		return Error(err, DecodeErrorCode, msg, "dbs.insertRecord")
	}
	if utils.VERBOSE > 2 {
		log.Printf("insertRecord %+v", rec)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "transaction error", "dbs.insertRecord")
	}
	defer tx.Rollback()

	// set defaults
	if utils.VERBOSE > 2 {
		log.Printf("insert record %+v", rec)
	}
	err = rec.Insert(tx)
	if err != nil {
		msg := fmt.Sprintf("unable to insert %+v", rec)
		log.Println(msg)
		return Error(err, InsertErrorCode, msg, "dbs.insertRecord")
	}

	// commit transaction
	if utils.VERBOSE > 2 {
		log.Printf("record %+v tx.Commit", rec)
	}
	err = tx.Commit()
	if err != nil {
		return Error(err, CommitErrorCode, "unable to commit transaction", "dbs.insertRecord")
	}
	return nil
}

// LoadTemplateSQL function loads DBS SQL templated statements
func LoadTemplateSQL(tmpl string, tmplData Record) (string, error) {
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	if !strings.HasSuffix(tmpl, ".sql") {
		tmpl += ".sql"
	}
	if utils.VERBOSE > 1 {
		log.Println("load template", tmpl)
	}
	stm, err := utils.ParseTmpl(sdir, tmpl, tmplData)
	if err != nil {
		return "", Error(err, LoadErrorCode, "", "dbs.LoadTemplateSQL")
	}
	if owner, ok := tmplData["Owner"]; ok && owner == "sqlite" {
		stm = strings.Replace(stm, "sqlite.", "", -1)
	}
	if DBOWNER == "sqlite" {
		stm = utils.ReplaceBinds(stm)
	}
	return stm, nil
}

// LoadSQL function loads DBS SQL statements with Owner
func LoadSQL(owner string) Record {
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	if utils.VERBOSE > 1 {
		log.Println("sql area", sdir)
	}
	dbsql := make(Record)
	for _, f := range utils.ListFiles(sdir) {
		k := strings.Split(f, ".")[0]
		stm, err := utils.ParseTmpl(sdir, f, tmplData)
		if err != nil {
			log.Fatal("unable to parse template", err)
		}
		if owner, ok := tmplData["Owner"]; ok && owner == "sqlite" {
			stm = strings.Replace(stm, "sqlite.", "", -1)
		}
		dbsql[k] = stm
	}
	return dbsql
}

// GetTestData executes simple query to ensure that connection to DB is valid.
// So far we can ask for a data tier id of specific tier since this table
// is very small and query execution will be really fast.
func GetTestData() error {
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	var args []interface{}
	args = append(args, "VALID")
	stm, err := LoadTemplateSQL("test_db", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "unable to load test_db sql template", "dbs.GetTestData")
	}
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "transaction error", "dbs.GetTestData")
	}
	defer tx.Rollback()
	var dtype string
	err = tx.QueryRow(stm, args...).Scan(&dtype)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement: %v, error %v", stm, err)
		log.Println(msg)
		if !strings.Contains(err.Error(), "no rows in result set") {
			msg := fmt.Sprintf("unable to GetID from DATASET_ACCESS_TYPES table")
			log.Println(msg)
			return Error(err, GetIDErrorCode, msg, "dbs.GetTestData")
		}
	}
	if dtype != "VALID" {
		return Error(err, InvalidParameterErrorCode, "invalid dataset access type", "dbs.GetTestData")
	}
	return nil
}

// helper function to get SQL statement from DBSQL dict for a given key
func getSQL(key string) string {
	// use generic query API to fetch the results from DB
	val, ok := DBSQL[key]
	if !ok {
		msg := fmt.Sprintf("Unable to load %s SQL", key)
		log.Fatal(msg)
	}
	stm := val.(string)
	if DBOWNER == "sqlite" {
		stm = utils.ReplaceBinds(stm)
	}
	return stm
}

// helper function to get value from record
func getValues(params Record, key string) []string {
	var out []string
	val, ok := params[key]
	if ok {
		switch v := val.(type) {
		case []string:
			return v
		case string:
			return []string{v}
		case []interface{}:
			for _, val := range v {
				out = append(out, fmt.Sprintf("%v", val))
			}
			return out
		case interface{}:
			return []string{fmt.Sprintf("%v", v)}
		}
	}
	return out
}

// helper function to get single value from a record
func getSingleValue(params Record, key string) (string, error) {
	values := getValues(params, key)
	if len(values) > 0 {
		return values[0], nil
	}
	msg := fmt.Sprintf("list is not allowed for provided key: %s", key)
	return "", Error(InvalidParamErr, ParseErrorCode, msg, "dbs.getSingleValue")
}

// WhereClause function construct proper SQL statement from given statement and list of conditions
func WhereClause(stm string, conds []string) string {
	if len(conds) == 0 {
		return strings.Trim(stm, " ")
	}
	if strings.Contains(stm, " WHERE") && !strings.Contains(stm, "TOKEN_GENERATOR WHERE LENGTH") {
		stm = fmt.Sprintf(" %s %s", stm, strings.Join(conds, " AND "))
	} else {
		stm = fmt.Sprintf("%s WHERE %s", stm, strings.Join(conds, " AND "))
	}
	return strings.Trim(stm, " ")
}

// ParseDBFile function parses given file name and extracts from it dbtype and dburi
// file should contain the "dbtype dburi" string
func ParseDBFile(dbfile string) (string, string, string) {
	dat, err := os.ReadFile(dbfile)
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
//
//gocyclo:ignore
func executeAll(w io.Writer, sep, stm string, args ...interface{}) error {
	stm = CleanStatement(stm)
	if DRYRUN {
		utils.PrintSQL(stm, args, "")
		return nil
	}
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}
	var enc *json.Encoder
	if w != nil {
		enc = json.NewEncoder(w)
	}

	// execute transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "transaction error", "dbs.executeAll")
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement: %v", stm)
		log.Println(msg)
		return Error(err, QueryErrorCode, "query error", "dbs.executeAll")
	}
	defer rows.Close()

	// extract columns from Rows object and create values & valuesPtrs to retrieve results
	columns, _ := rows.Columns()
	var cols []string
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	rowCount := 0
	writtenResults := false
	for rows.Next() {
		if rowCount == 0 {
			// initialize value pointers
			for i := range columns {
				valuePtrs[i] = &values[i]
			}
		}
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return Error(err, RowsScanErrorCode, "unable to obtain rows values", "dbs.executeAll")
		}
		if rowCount != 0 && w != nil {
			// add separator line to our output
			w.Write([]byte(sep))
		}
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
			if rowCount == 0 {
				if sep != "" {
					writtenResults = true
					w.Write([]byte("[\n"))
					defer w.Write([]byte("]\n"))
				}
			}
			err = enc.Encode(rec)
			if err != nil {
				return Error(err, EncodeErrorCode, "unable to encode data record", "dbs.executeAll")
			}
		}
		rowCount += 1
	}
	if err = rows.Err(); err != nil {
		return Error(err, RowsScanErrorCode, "unable to get rows values", "dbs.executeAll")
	}
	// make sure we write proper response if no result written
	if sep != "" && !writtenResults {
		w.Write([]byte("[]"))
	}
	return nil
}

// similar to executeAll function but it takes explicit set of columns and values
//
//gocyclo:ignore
func execute(
	w io.Writer,
	sep, stm string,
	cols []string,
	vals []interface{}, args ...interface{}) error {

	stm = CleanStatement(stm)
	if DRYRUN {
		utils.PrintSQL(stm, args, "")
		return nil
	}
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}
	var enc *json.Encoder
	if w != nil {
		enc = json.NewEncoder(w)
	}

	// execute transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "transaction error", "dbs.execute")
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("DB.Query, query='%s' args='%v'", stm, args)
		log.Println(msg)
		return Error(err, QueryErrorCode, msg, "dbs.execute")
	}
	defer rows.Close()

	// loop over rows
	rowCount := 0
	writtenResults := false
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			msg := fmt.Sprintf("rows.Scan, vals='%v'", vals)
			log.Println(msg)
			return Error(err, RowsScanErrorCode, "unable to get rows values", "dbs.execute")
		}
		if rowCount != 0 && w != nil {
			// add separator line to our output
			w.Write([]byte(sep))
		}
		rec := make(Record)
		for i := range cols {
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
			if rowCount == 0 {
				if sep != "" {
					writtenResults = true
					w.Write([]byte("[\n"))
					defer w.Write([]byte("]\n"))
				}
			}
			err = enc.Encode(rec)
			if err != nil {
				return Error(err, EncodeErrorCode, "unable to encode data record", "dbs.execute")
			}
		}
		rowCount += 1
	}
	if err = rows.Err(); err != nil {
		return Error(err, RowsScanErrorCode, "unable to get rows values", "dbs.execute")
	}
	// make sure we write proper response if no result written
	if sep != "" && !writtenResults {
		w.Write([]byte("[]"))
	}
	return nil
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
			msg := fmt.Sprintf("DB session statement")
			log.Println(msg, "\n###", s)
			return Error(err, SessionErrorCode, "ORACLE session error", "dbs.executeSession")
		}
	}
	return nil
}

// QueryRow function fetches results from given table
func QueryRow(table, id, attr string, val interface{}) (int64, error) {
	var stm string
	if DBOWNER == "sqlite" {
		stm = fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", id, table, attr)
	} else {
		stm = fmt.Sprintf("SELECT T.%s FROM %s.%s T WHERE T.%s = :%s", id, DBOWNER, table, attr, attr)
	}
	if utils.VERBOSE > 1 {
		log.Printf("QueryRow\n%s; binding value=%+v", stm, val)
	}
	// in SQLite the ids are int64 while on ORACLE they are float64
	var tid int64
	err := DB.QueryRow(stm, val).Scan(&tid)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Printf("fail to get id for %s, %v, error %v", stm, val, err)
		}
		return int64(tid), Error(err, QueryErrorCode, "", "dbs.GetID")
	}
	return int64(tid), nil
}

// GetID function fetches table primary id for a given value
func GetID(tx *sql.Tx, table, id, attr string, val ...interface{}) (int64, error) {
	var stm string
	if DBOWNER == "sqlite" {
		stm = fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", id, table, attr)
	} else {
		stm = fmt.Sprintf("SELECT T.%s FROM %s.%s T WHERE T.%s = :%s", id, DBOWNER, table, attr, attr)
	}
	if utils.VERBOSE > 1 {
		log.Printf("getID\n%s; binding value=%+v", stm, val)
	}
	// in SQLite the ids are int64 while on ORACLE they are float64
	var tid int64
	err := tx.QueryRow(stm, val...).Scan(&tid)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Printf("fail to get id for %s, %v, error %v", stm, val, err)
		}
		return int64(tid), Error(err, QueryErrorCode, "", "dbs.GetID")
	}
	return int64(tid), nil
}

// GetRecID function fetches table primary id for a given value and insert it if necessary
func GetRecID(tx *sql.Tx, rec DBRecord, table, id, attr string, val ...interface{}) (int64, error) {
	rid, err := GetID(tx, table, id, attr, val...)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Printf("unable to find %s for %v", id, val)
		}
		err = rec.Insert(tx)
		if err != nil {
			// if we have concurrent threads to insert data we may end-up with
			// ORA-00001 error which violates unique constrain
			if strings.Contains(err.Error(), "ORA-00001") {
				time.Sleep(1 * time.Second)
				err = rec.Insert(tx)
				if err != nil {
					return 0, Error(err, InsertErrorCode, "", "dbs.GetRecID")
				}
			} else {
				return 0, Error(err, InsertErrorCode, "", "dbs.GetRecID")
			}
		}
		rid, err = GetID(tx, table, id, attr, val...)
		if err != nil {
			return 0, Error(err, InsertErrorCode, "", "dbs.GetRecID")
		}
	}
	return rid, nil
}

// IfExistMulti checks if given rid exists in given table for provided value conditions
func IfExistMulti(tx *sql.Tx, table, rid string, args []string, vals ...interface{}) bool {
	var stm string
	var wheres []string
	if DBOWNER == "sqlite" {
		stm = fmt.Sprintf("SELECT %s FROM %s", rid, table)
		for _, a := range args {
			wheres = append(wheres, fmt.Sprintf("%s=?", a))
		}
	} else {
		stm = fmt.Sprintf("SELECT T.%s FROM %s.%s T", rid, DBOWNER, table)
		for _, a := range args {
			wheres = append(wheres, fmt.Sprintf("%s=:%s", a, a))
		}
	}
	stm = fmt.Sprintf("%s WHERE %s", stm, strings.Join(wheres, " AND "))
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, vals, "execute")
	}
	var tid float64
	err := tx.QueryRow(stm, vals...).Scan(&tid)
	if err == nil {
		return true
	}
	if utils.VERBOSE > 1 {
		log.Printf("fail to get ID from table %s %s for %v values %v", table, rid, args, vals)
	}
	return false
}

// IfExist check if given rid, attr exists in given table for provided value conditions
func IfExist(tx *sql.Tx, table, rid, attr string, val ...interface{}) bool {
	// check if our data already exist in DB
	fid, err := GetID(tx, table, rid, attr, val...)
	if err == nil {
		if fid > 0 {
			if utils.VERBOSE > 1 {
				log.Printf("%s found in %s with id=%v", attr, table, fid)
			}
			return true
		}
	}
	if utils.VERBOSE > 1 {
		log.Printf("fail to get ID from table %s %s for %s=%v", table, rid, attr, val)
	}
	return false
}

// OperatorValue function generates operator and value pair for a given argument
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
			arr := strings.Split(v, "-")
			if len(arr) != 2 {
				msg := fmt.Sprintf("fail to parse run-range '%s'", v)
				return out, Error(InvalidParamErr, ParseErrorCode, msg, "dbs.ParseRuns")
			}
			minValR := strings.Trim(arr[0], " ")
			minR, err := strconv.Atoi(minValR)
			if err != nil {
				return out, Error(err, ParseErrorCode, "fail to convert min run value", "dbs.ParseRuns")
			}
			maxValR := strings.Trim(arr[1], " ")
			maxR, err := strconv.Atoi(maxValR)
			if err != nil {
				return out, Error(err, ParseErrorCode, "fail to convert max run value", "dbs.ParseRuns")
			}
			for r := minR; r <= maxR; r++ {
				out = append(out, fmt.Sprintf("%d", r))
			}
		} else if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
			runs := strings.Replace(v, "[", "", -1)
			runs = strings.Replace(runs, "]", "", -1)
			for _, r := range strings.Split(runs, ",") {
				run := strings.Trim(r, " ")
				out = append(out, run)
			}
		} else {
			msg := fmt.Sprintf("invalid run input parameter %s", v)
			err := errors.New(msg)
			return out, Error(err, ParseErrorCode, "fail to parse run input", "dbs.ParseRuns")
		}
	}
	return out, nil
}

// TokenGenerator creates a SQL token generator statement
func TokenGenerator(runs []string, limit int, name string) (string, []string) {
	if DBOWNER == "sqlite" {
		return TokenGeneratorSQLite(runs, name)
	}
	return TokenGeneratorORACLE(runs, limit, name)
}

// TokenGeneratorORACLE creates a SQL token generator statement using ORACLE syntax
// https://betteratoracle.com/posts/20-how-do-i-bind-a-variable-in-list
func TokenGeneratorORACLE(runs []string, limit int, name string) (string, []string) {
	stm := "WITH TOKEN_GENERATOR AS (\n"
	var tstm []string
	var vals []string
	for idx, chunk := range GetChunks(runs, limit) {
		t := fmt.Sprintf("%s_%d", name, idx)
		s := fmt.Sprintf("\tSELECT REGEXP_SUBSTR(:%s, '[^,]+', 1, LEVEL) token ", t)
		s += "\n\tFROM DUAL\n"
		s += fmt.Sprintf("\tCONNECT BY LEVEL <= length(:%s) - length(REPLACE(:%s, ',', '')) + 1", t, t)
		tstm = append(tstm, s)
		// since we have three bind values in token statemnt, we'll need to add them all
		vals = append(vals, chunk)
		vals = append(vals, chunk)
		vals = append(vals, chunk)
	}
	stm += strings.Join(tstm, " UNION ALL ")
	stm += "\n)"
	stm += "\n"
	return stm, vals
}

// TokenGeneratorSQLite creates a SQL token generator statement using SQLite syntax
// https://stackoverflow.com/questions/67372811/what-is-equivalent-of-token-generator-oracle-sql-statement-in-sqlite
func TokenGeneratorSQLite(runs []string, name string) (string, []string) {
	stm := `WITH TOKEN_GENERATOR AS (
  SELECT '' token, :token_0 || ',' value
  UNION ALL
  SELECT SUBSTR(value, 1, INSTR(value, ',') - 1),
         SUBSTR(value, INSTR(value, ',') + 1)
  FROM TOKEN_GENERATOR WHERE LENGTH(value) > 1
)
`
	s := fmt.Sprintf(":%s", name)
	stm = strings.Replace(stm, ":token_0", s, -1)
	vals := strings.Join(runs, ",")
	return stm, []string{vals}
}

// TokenCondition provides proper condition statement for TokenGenerator
func TokenCondition() string {
	if DBOWNER == "sqlite" {
		return "(SELECT token FROM TOKEN_GENERATOR WHERE token <> '')"
	}
	return "(SELECT TOKEN FROM TOKEN_GENERATOR)"
}

// GetChunks helper function to get ORACLE chunks from provided list of values
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
			rr := strings.Split(r, "-")
			if len(runs) == 1 {
				cond := fmt.Sprintf(
					" %s.RUN_NUM between %s and %s ",
					table, placeholder("minrun"), placeholder("maxrun"))
				conds = append(conds, cond)
				args = append(args, rr[0])
				args = append(args, rr[1])
				where = fmt.Sprintf("( %s )", strings.Join(conds, " OR "))
				return "", where, args
			}
			v, _ := strconv.Atoi(rr[0])
			minR := v
			v, _ = strconv.Atoi(rr[1])
			maxR := v
			for i := minR; i <= maxR; i++ {
				runList = append(runList, fmt.Sprintf("%d", i))
			}
		} else { // plain run numbers we store to run list
			runList = append(runList, r)
		}
	}
	if len(args) > 0 {
		where = fmt.Sprintf("( %s )", strings.Join(conds, " OR "))
		return "", where, args
	}
	// take run list and generate token statement
	stm := fmt.Sprintf("%s.RUN_NUM in %s", table, TokenCondition())
	token, binds := TokenGenerator(runList, 4000, "run_num_token") // 4000 is hard ORACLE limit
	conds = append(conds, stm)
	for _, v := range binds {
		args = append(args, v)
	}
	where = fmt.Sprintf("( %s )", strings.Join(conds, " OR "))
	return token, where, args
}

// AddParam adds single parameter to SQL statement
func AddParam(
	name, sqlName string,
	params Record,
	conds []string,
	args []interface{}) ([]string, []interface{}) {

	vals := getValues(params, name)
	if len(vals) == 1 {
		op, val := OperatorValue(vals[0])
		if strings.Contains(val, "e+") || strings.Contains(val, "E+") {
			val = utils.ConvertFloat(val)
		}
		if strings.Contains(val, "[") {
			val = strings.Replace(val, "[", "", -1)
			val = strings.Replace(val, "]", "", -1)
			val = strings.Trim(val, " ")
		}
		cond := fmt.Sprintf(" %s %s %s", sqlName, op, placeholder(name))
		conds = append(conds, cond)
		args = append(args, val)
	}
	return conds, args
}

// IncrementSequences API provide a way to get N unique IDs for given sequence name
func IncrementSequences(tx *sql.Tx, seq string, n int) ([]int64, error) {
	var out []int64
	if DBOWNER == "sqlite" {
		ts := time.Now().UnixNano()
		for i := 0; i < n; i++ {
			out = append(out, ts+int64(i))
		}
		return out, nil
	}
	var pid float64
	for i := 0; i < n; i++ {
		stm := fmt.Sprintf("select %s.%s.nextval as val from dual", DBOWNER, seq)
		err := tx.QueryRow(stm).Scan(&pid)
		if err != nil {
			msg := fmt.Sprintf("fail to increment sequence, query='%s'", stm)
			log.Println(msg)
			return out, Error(err, QueryErrorCode, "", "dbs.IncrementSequences")
		}
		out = append(out, int64(pid))
	}
	return out, nil
}

// IncrementSequence API returns single unique ID for a given sequence
func IncrementSequence(tx *sql.Tx, seq string) (int64, error) {
	ids, err := IncrementSequences(tx, seq, 1)
	if len(ids) == 1 && err == nil {
		return ids[0], nil
	}
	return 0, Error(err, LastInsertErrorCode, "", "dbs.IncrementSequence")
}

// LastInsertID returns last insert id of given table and idname parameter
func LastInsertID(tx *sql.Tx, table, idName string) (int64, error) {
	stm := fmt.Sprintf("select MAX(%s) from %s.%s", idName, DBOWNER, table)
	if DBOWNER == "sqlite" {
		stm = fmt.Sprintf("select MAX(%s) from %s", idName, table)
	}
	var pid sql.NullFloat64
	if utils.VERBOSE > 1 {
		log.Println("execute", stm)
	}
	err := tx.QueryRow(stm).Scan(&pid)
	if err != nil {
		msg := fmt.Sprintf("fail to process query='%s'", stm)
		log.Println(msg)
		return 0, Error(err, QueryErrorCode, "", "dbs.LastInsertID")
	}
	return int64(pid.Float64), nil
}

// RunsConditions function to handle runs conditions
func RunsConditions(runs []string, table string) (string, []string, []interface{}, error) {
	var args []interface{}
	var conds []string
	var token string

	if len(runs) > 1 {
		var runList []string
		for _, rrr := range runs {
			pruns, e := ParseRuns([]string{rrr})
			if e != nil {
				msg := "unable to parse runs input"
				return token, conds, args, Error(e, ParseErrorCode, msg, "dbs.RunsConditions")
			}
			for _, v := range pruns {
				runList = append(runList, v)
			}
		}
		tok, whereRuns, bindsRuns := runsClause(table, runList)
		token = tok
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	} else if len(runs) == 1 {
		if strings.Contains(runs[0], "-") {
			rrr := strings.Split(runs[0], "-")
			minR := rrr[0]
			maxR := rrr[len(rrr)-1]
			cond := fmt.Sprintf(" %s.RUN_NUM  between :minrun0 and :maxrun0 ", table)
			conds = append(conds, cond)
			args = append(args, minR)
			args = append(args, maxR)
		} else {
			cond := fmt.Sprintf(" %s.RUN_NUM = :run_num ", table)
			conds = append(conds, cond)
			args = append(args, runs[0])
		}
	}
	return token, conds, args, nil
}
