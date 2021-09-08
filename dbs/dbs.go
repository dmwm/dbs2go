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

	validator "github.com/go-playground/validator/v10"
	"github.com/vkuznet/dbs2go/utils"
)

// use API struct for reflection
type API struct {
	Reader    io.Reader
	Writer    http.ResponseWriter
	Context   context.Context
	Params    Record
	Separator string
	CreateBy  string
	Api       string
}

// String provides string representation of API struct
func (a *API) String() string {
	return fmt.Sprintf("API=%s params=%+v createBy=%s separator='%s'", a.Api, a.Params, a.CreateBy, a.Separator)
}

// use a single instance of Validate, it caches struct info
var RecordValidator *validator.Validate

// main record we work with
type Record map[string]interface{}

// global variable to keep pointer to DB
var DB *sql.DB
var DBTYPE string
var DBSQL Record
var DBOWNER string

// DRYRUN allows to skip query execution and printout DB statements along with passed parameters
var DRYRUN bool

// DBRecord interface allows to insert DBS record using given transaction
type DBRecord interface {
	Insert(tx *sql.Tx) error
	Validate() error
	SetDefaults()
	Decode(r io.Reader) error
}

// DecodeValidatorError provides uniform error representation
// of DBRecord validation errors
func DecodeValidatorError(r, err interface{}) error {
	if err != nil {
		msg := fmt.Sprintf("ERROR:\n%+v", r)
		for _, err := range err.(validator.ValidationErrors) {
			msg = fmt.Sprintf("%s\nkey=%v type=%v value=%v, constrain %v %v", msg, err.Field(), err.Type(), err.Value(), err.ActualTag(), err.Param())
		}
		return errors.New(msg)
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
		log.Println("fail to decode record", err)
		return err
	}
	if utils.VERBOSE > 2 {
		log.Printf("insertRecord %+v", rec)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return err
	}
	defer tx.Rollback()

	// set defaults
	if utils.VERBOSE > 2 {
		log.Printf("insert record %+v", rec)
	}
	err = rec.Insert(tx)
	if err != nil {
		log.Printf("unable to insert %+v, %v", rec, err)
		return err
	}

	// commit transaction
	if utils.VERBOSE > 2 {
		log.Printf("record %+v tx.Commit", rec)
	}
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return err
	}
	return nil
}

// helper function to load DBS SQL templated statements
func LoadTemplateSQL(tmpl string, tmplData Record) (string, error) {
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	if !strings.HasSuffix(tmpl, ".sql") {
		tmpl += ".sql"
	}
	if utils.VERBOSE > 0 {
		log.Println("load template", tmpl)
	}
	stm, err := utils.ParseTmpl(sdir, tmpl, tmplData)
	if err != nil {
		return "", err
	}
	if owner, ok := tmplData["Owner"]; ok && owner == "sqlite" {
		stm = strings.Replace(stm, "sqlite.", "", -1)
	}
	if DBOWNER == "sqlite" {
		stm = utils.ReplaceBinds(stm)
	}
	return stm, nil
}

// helper function to load DBS SQL statements with Owner
func LoadSQL(owner string) Record {
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
	if utils.VERBOSE > 0 {
		log.Println("sql area", sdir)
	}
	dbsql := make(Record)
	for _, f := range utils.Listfiles(sdir) {
		k := strings.Split(f, ".")[0]
		stm, err := utils.ParseTmpl(sdir, f, tmplData)
		if err != nil {
			log.Fatal("unable to parse tempalte", err)
		}
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
		case interface{}:
			return []string{fmt.Sprintf("%v", v)}
		case []interface{}:
			for _, val := range v {
				out = append(out, fmt.Sprintf("%v", val))
			}
			return out
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
	return "", errors.New(fmt.Sprintf("no list is allowed for provided key: %s", key))
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

// function to parse given file name and extract from it dbtype and dburi
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
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
		return errors.New(msg)
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
			msg := fmt.Sprintf("unable to scan DB results %s", err)
			return errors.New(msg)
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
				return err
			}
		}
		rowCount += 1
	}
	if err = rows.Err(); err != nil {
		msg := fmt.Sprintf("rows error %v", err)
		return errors.New(msg)
	}
	// make sure we write proper response if no result written
	if sep != "" && !writtenResults {
		w.Write([]byte("[]"))
	}
	return nil
}

// similar to executeAll function but it takes explicit set of columns and values
func execute(w io.Writer, sep, stm string, cols []string, vals []interface{}, args ...interface{}) error {
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
		msg := fmt.Sprintf("unable to obtain transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	rows, err := tx.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("DB.Query, query='%s' args='%v' error=%v", stm, args, err)
		return errors.New(msg)
	}
	defer rows.Close()

	// loop over rows
	rowCount := 0
	writtenResults := false
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			msg := fmt.Sprintf("rows.Scan, vals='%v', error=%v", vals, err)
			return errors.New(msg)
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
				return err
			}
		}
		rowCount += 1
	}
	if err = rows.Err(); err != nil {
		return err
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
			log.Printf("DB error\n### %s\n%v", s, err)
			return err
		}
	}
	return nil
}

// helper function to get table primary id for a given value
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
	var tid float64
	err := tx.QueryRow(stm, val...).Scan(&tid)
	if err != nil {
		log.Printf("fail to get id for %s, %v, error %v", stm, val, err)
	}
	return int64(tid), err
}

// helper function to get table primary id for a given value and insert it if necessary
func GetRecID(tx *sql.Tx, rec DBRecord, table, id, attr string, val ...interface{}) (int64, error) {
	rid, err := GetID(tx, table, id, attr, val...)
	if err != nil {
		log.Printf("unable to find %s for %v", id, val)
		err = rec.Insert(tx)
		if err != nil {
			return 0, err
		}
		rid, err = GetID(tx, table, id, attr, val...)
		if err != nil {
			return 0, err
		}
	}
	return rid, err
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
			arr := strings.Split(v, "-")
			if len(arr) != 2 {
				msg := fmt.Sprintf("fail to parse run-range '%s'", v)
				return out, errors.New(msg)
			}
			minValR := strings.Trim(arr[0], " ")
			minR, err := strconv.Atoi(minValR)
			if err != nil {
				return out, err
			}
			maxValR := strings.Trim(arr[1], " ")
			maxR, err := strconv.Atoi(maxValR)
			if err != nil {
				return out, err
			}
			for r := minR; r <= maxR; r++ {
				out = append(out, fmt.Sprintf("%d", r))
			}
		} else if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
			runs := strings.Replace(v, "[", "", -1)
			runs = strings.Replace(runs, "]", "", -1)
			for _, r := range strings.Split(runs, ",") {
				run := strings.Trim(r, " ")
				//                 log.Println("input", r)
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
				cond := fmt.Sprintf(" %s.RUN_NUM between %s and %s ", table, placeholder("minrun"), placeholder("maxrun"))
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
func AddParam(name, sqlName string, params Record, conds []string, args []interface{}) ([]string, []interface{}) {
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

// IncrementSequence API
func IncrementSequence(tx *sql.Tx, seq string) (int64, error) {
	if DBOWNER == "sqlite" {
		return 0, nil
	}
	var pid float64
	stm := fmt.Sprintf("select %s.%s.nextval as val from dual", DBOWNER, seq)
	if utils.VERBOSE > 0 {
		log.Println("execute", stm)
	}
	err := tx.QueryRow(stm).Scan(&pid)
	if err != nil {
		msg := fmt.Sprintf("fail to increment sequence, query='%s' error=%v", stm, err)
		log.Println(msg)
		return 0, errors.New(msg)
	}
	return int64(pid), nil
}

// LastInsertId shoudl return last insert id of given table and idname parameter
func LastInsertID(tx *sql.Tx, table, idName string) (int64, error) {
	stm := fmt.Sprintf("select MAX(%s) from %s.%s", idName, DBOWNER, table)
	if DBOWNER == "sqlite" {
		stm = fmt.Sprintf("select MAX(%s) from %s", idName, table)
	}
	var pid sql.NullInt64
	if utils.VERBOSE > 0 {
		log.Println("execute", stm)
	}
	err := tx.QueryRow(stm).Scan(&pid)
	if err != nil {
		msg := fmt.Sprintf("tx.Exec, query='%s' error=%v", stm, err)
		return 0, errors.New(msg)
	}
	return pid.Int64, nil
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
				return token, conds, args, errors.New(msg)
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
