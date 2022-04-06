package dbs

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dmwm/dbs2go/utils"
)

// FileLumis API
//gocyclo:ignore
func (a *API) FileLumis() error {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Lfn"] = false
	tmpl["LfnGenerator"] = ""
	tmpl["TokenGenerator"] = ""
	tmpl["LfnList"] = false
	tmpl["ValidFileOnly"] = false
	tmpl["BlockName"] = false
	tmpl["Migration"] = false

	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) > 1 {
		token, binds := TokenGenerator(lfns, 100, "lfns_token") // 100 is max for # of allowed entries
		tmpl["TokenGenerator"] = token
		tmpl["Lfn"] = true
		tmpl["LfnList"] = true
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME in %s", TokenCondition())
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		tmpl["Lfn"] = true
		tmpl["LfnList"] = false
		conds = append(conds, "F.LOGICAL_FILE_NAME = :logical_file_name")
		args = append(args, lfns[0])
	}

	validFileOnly := getValues(a.Params, "validFileOnly")
	if len(validFileOnly) == 1 {
		if validFileOnly[0] == "1" {
			tmpl["ValidFileOnly"] = true
			conds = append(conds, "F.IS_FILE_VALID = 1")
			conds = append(conds, "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ")
		}
	}

	blocks := getValues(a.Params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	}

	stm, err := LoadTemplateSQL("filelumis", tmpl)
	if utils.VERBOSE > 0 {
		log.Println("### stm", stm)
	}
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.filelumis.FileLumis")
	}

	// generate run_num token
	runs := getValues(a.Params, "run_num")
	t, c, na, e := RunsConditions(runs, "FL")
	if e != nil {
		return Error(e, ParametersErrorCode, "", "dbs.filelumis.FileLumis")
	}
	if t != "" {
		stm = fmt.Sprintf("%s %s", t, stm)
	}
	for _, v := range c {
		conds = append(conds, v)
	}
	for _, v := range na {
		if t != "" { // we got token, therefore need to insert args
			args = utils.Insert(args, v)
		} else {
			args = append(args, v)
		}
	}

	// check if we got both run and lfn lists
	if _, ok := a.Params["runList"]; ok {
		if len(runs) > 1 && len(lfns) > 1 {
			msg := "filelumis API supports single list of lfns or run numbers"
			return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.filelumis.FileLumis")
		}
	}

	stm = WhereClause(stm, conds)

	// fix binding variables for SQLite
	if DBOWNER == "sqlite" {
		for k := range a.Params {
			key := fmt.Sprintf(":%s", strings.ToLower(k))
			if strings.Contains(stm, key) {
				stm = strings.Replace(stm, key, "?", -1)
			}
		}
	}

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.filelumis.FileLumis")
	}
	return nil
}

// FileLumis represents File Lumis DBS DB table
type FileLumis struct {
	FILE_ID          int64 `json:"file_id validate:"required,number""`
	LUMI_SECTION_NUM int64 `json:"lumi_section_num" validate:"required,number"`
	RUN_NUM          int64 `json:"run_num" validate:"required,number"`
	EVENT_COUNT      int64 `json:"event_count"`
}

// Insert implementation of FileLumis
func (r *FileLumis) Insert(tx *sql.Tx) error {
	var err error
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.filelumis.Insert")
	}
	// get SQL statement from static area
	var stm string
	if r.EVENT_COUNT != 0 {
		stm = getSQL("insert_filelumis")
		_, err = tx.Exec(stm, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID, r.EVENT_COUNT)
	} else {
		stm = getSQL("insert_filelumis2")
		_, err = tx.Exec(stm, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID)
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert FileLumis\n%s\n%+v", stm, r)
	}
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.filelumis.Insert")
	}
	return nil
}

// Validate implementation of FileLumis
func (r *FileLumis) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for FileLumis
func (r *FileLumis) SetDefaults() {
}

// Decode implementation for FileLumis
func (r *FileLumis) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.filelumis.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.filelumis.Decode")
	}
	return nil
}

// InsertFileLumisTx DBS API
func (a *API) InsertFileLumisTx(tx *sql.Tx) error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.filelumis.InsertFileLumisTx")
	}
	rec := FileLumis{}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.filelumis.InsertFileLumisTx")
	}
	err = rec.Insert(tx)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to insert %+v, %v", rec, err)
		}
		return Error(err, InsertErrorCode, "", "dbs.filelumis.InsertFileLumisTx")
	}
	return nil
}

// InsertFileLumisTxViaChunks DBS API
//gocyclo:ignore
func InsertFileLumisTxViaChunks(tx *sql.Tx, table string, records []FileLumis) error {

	var stm string
	var err error

	if FileLumiInsertMethod == "temptable" {
		// create temp table
		tmpl := make(Record)
		tmpl["Owner"] = DBOWNER
		tmpl["TempTable"] = table
		stm, err = LoadTemplateSQL("temp_filelumis", tmpl)
		if err != nil {
			if utils.VERBOSE > 0 {
				log.Printf("Unable to load temp_filelumis, error %v", err)
			}
			return Error(err, LoadErrorCode, "", "dbs.filelumis.InsertFileLumisTxViaChunks")
		}
		stm = CleanStatement(stm)
		if utils.VERBOSE > 1 {
			args := []interface{}{}
			utils.PrintSQL(stm, args, "execute")
		}
		_, err = tx.Exec(stm)
		if err != nil {
			if utils.VERBOSE > 0 {
				log.Printf("Unable to create temp FileLumis table, error %v", err)
			}
			if strings.Contains(err.Error(), "ORA-00955") {
				log.Printf("Temp table %s is already exists\n", table)
			} else {
				return Error(err, InsertErrorCode, "", "dbs.filelumis.InsertFileLumisTxViaChunks")
			}
		}
	}

	// prepare loop using maxSize/chunkSize insertion, see
	// test/filelumis_test.go
	nrec := len(records)
	maxSize := FileLumiMaxSize     // optimal value should be around 100000
	chunkSize := FileLumiChunkSize // optimal value should be around 500
	if maxSize > nrec {
		maxSize = nrec
	}
	for k := 0; k < nrec; k = k + maxSize {
		t0 := time.Now()
		var wg sync.WaitGroup
		ngoroutines := 0
		for i := k; i < k+maxSize; i = i + chunkSize {
			wg.Add(1)
			size := i + chunkSize
			if size > (k + maxSize) {
				size = k + maxSize
			}
			if size > nrec {
				size = nrec
			}
			go insertFLChunk(tx, &wg, table, records[i:size])
			ngoroutines += 1
		}
		limit := k + maxSize
		if limit > nrec {
			limit = nrec
		}
		if utils.VERBOSE > 0 {
			log.Printf(
				"process %d goroutines, step %d-%d, elapsed time %v",
				ngoroutines, k, limit, time.Since(t0))
		}
		wg.Wait()
	}

	if FileLumiInsertMethod == "temptable" {
		// merge temp table back
		tmpl := make(Record)
		tmpl["Owner"] = DBOWNER
		tmpl["TempTable"] = table
		stm, err := LoadTemplateSQL("merge_filelumis", tmpl)
		if err != nil {
			if utils.VERBOSE > 0 {
				log.Printf("Unable to load merge_filelumis, error %v", err)
			}
			return Error(err, LoadErrorCode, "", "dbs.filelumis.InsertFileLumisTxViaChunks")
		}
		stm = CleanStatement(stm)
		if utils.VERBOSE > 1 {
			args := []interface{}{}
			utils.PrintSQL(stm, args, "execute")
		}
		_, err = tx.Exec(stm)
		if err != nil {
			if utils.VERBOSE > 0 {
				log.Printf("Unable to merge temp FileLumis table, error %v", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.filelumis.InsertFileLumisTxViaChunks")
		}
	}

	if err != nil {
		return Error(err, GenericErrorCode, "", "dbs.filelumis.InsertFileLumisTxViaChunks")
	}
	return nil
}

// helper function to insert FileLumis chunk via ORACLE INSERT ALL statement
func insertFLChunk(tx *sql.Tx, wg *sync.WaitGroup, table string, records []FileLumis) error {
	defer wg.Done()
	valueStrings := []string{}
	valueArgs := []interface{}{}
	if len(records) == 0 {
		msg := "zero array of FileLumi records"
		log.Println(msg)
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.filelumis.insertFLChunk")
	}
	if FileLumiInsertMethod == "temptable" && DBOWNER == "sqlite" {
		msg := "unable to use temp table with sqlite backend"
		log.Println(msg)
		return Error(DatabaseErr, DatabaseErrorCode, msg, "dbs.filelumis.insertFLChunk")
	}

	// prepare statement for insering all rows
	stm := fmt.Sprintf("INSERT ALL")
	names := "RUN_NUM,LUMI_SECTION_NUM,FILE_ID,EVENT_COUNT"
	vals := ":r,:l,:f,:e"
	for _, r := range records {
		valueArgs = append(valueArgs, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID, r.EVENT_COUNT)
		valueStrings = append(valueStrings, "(?,?,?,?)")
		stm = fmt.Sprintf("%s\nINTO %s (%s) VALUES (%s)", stm, table, names, vals)
	}
	stm = fmt.Sprintf("%s\nSELECT * FROM dual", stm)
	if DBOWNER == "sqlite" {
		stm = fmt.Sprintf(
			"INSERT OR IGNORE\nINTO %s (%s) VALUES %s",
			table, names, strings.Join(valueStrings, ","))
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 3 {
		log.Printf("new statement\n%v\n%v", stm, valueArgs)
	} else if utils.VERBOSE > 0 {
		shortStatement := strings.Split(stm, "(")[0]
		log.Printf("new statement\n%v\nwith %v value records", shortStatement, len(valueArgs))
	}
	_, err := tx.Exec(stm, valueArgs...)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("Unable to insert FileLumis records, error %v", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.filelumis.insertFLChunk")
	}
	return nil
}

// SelectFileLumiListInsert determines which of the three methods to insert the FileLumi list
func (a *API) SelectFileLumiListInsert(tx *sql.Tx, fll []FileLumi, tempTable string, fileID int64, function string) error {
	// there are three methods to insert FileLumi list
	// - via temp table
	// - via INSERT ALL and using chunks
	// - sequential method, i.e. record by record
	// we apply the following rules:
	// - if number of records is less FileLumiChunkSize we use sequential inserts
	// - otherwise we choose between temptable and chunks methods, and only use
	// temp table name, e.g. ORA$PTT_TEMP_FILE_LUMIS, for ORACLE inserts

	// insert FileLumi list via temptable or chunks
	if len(fll) > FileLumiChunkSize {
		var err error

		if utils.VERBOSE > 0 {
			log.Printf(
				"insert FileLumi list via %s method %d records",
				FileLumiInsertMethod, len(fll))
		}

		var fileLumiList []FileLumis
		for _, r := range fll {
			fl := FileLumis{
				FILE_ID:          fileID,
				RUN_NUM:          r.RunNumber,
				LUMI_SECTION_NUM: r.LumiSectionNumber,
				EVENT_COUNT:      r.EventCount,
			}
			fileLumiList = append(fileLumiList, fl)
		}
		err = InsertFileLumisTxViaChunks(tx, tempTable, fileLumiList)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert FileLumis records", err)
			}
			return Error(err, InsertErrorCode, "", function)
		}

	} else {
		if utils.VERBOSE > 0 {
			log.Println("insert FileLumi list sequentially", len(fll), "records")
		}

		// insert FileLumi list via sequential insert of file lumi records
		for _, r := range fll {
			var vals []interface{}
			vals = append(vals, fileID)
			vals = append(vals, r.RunNumber)
			vals = append(vals, r.LumiSectionNumber)
			args := []string{"file_id", "run_num", "lumi_section_num"}
			if IfExistMulti(tx, "FILE_LUMIS", "file_id", args, vals...) {
				// skip if we found valid filelumi record for given run and lumi
				continue
			}
			fl := FileLumis{
				FILE_ID:          fileID,
				RUN_NUM:          r.RunNumber,
				LUMI_SECTION_NUM: r.LumiSectionNumber,
				EVENT_COUNT:      r.EventCount,
			}
			data, err := json.Marshal(fl)
			if err != nil {
				if utils.VERBOSE > 1 {
					log.Println("unable to marshal dataset file lumi list", err)
				}
				return Error(err, MarshalErrorCode, "", function)
			}
			a.Reader = bytes.NewReader(data)
			err = a.InsertFileLumisTx(tx)
			if err != nil {
				if utils.VERBOSE > 1 {
					log.Println("unable to insert FileLumis record", err)
				}
				return Error(err, InsertErrorCode, "", function)
			}
		}
	}
	return nil
}
