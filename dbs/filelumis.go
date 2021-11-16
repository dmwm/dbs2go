package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/vkuznet/dbs2go/utils"
)

// FileLumis API
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
		tmpl["ValidFileOnly"] = true
		conds = append(conds, "F.IS_FILE_VALID = 1")
		conds = append(conds, "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION') ")
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
		return err
	}

	// generate run_num token
	runs := getValues(a.Params, "run_num")
	t, c, na, e := RunsConditions(runs, "FL")
	if e != nil {
		return e
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
			return errors.New(msg)
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
	return executeAll(a.Writer, a.Separator, stm, args...)
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
		return err
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
	return err
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
		return err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	return nil
}

// InsertFileLumisTx DBS API
func (a *API) InsertFileLumisTx(tx *sql.Tx) error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := FileLumis{}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	err = rec.Insert(tx)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to insert %+v, %v", rec, err)
		}
		return err
	}
	return err
}

// InsertFileLumisTxMany DBS API
func InsertFileLumisTxMany(tx *sql.Tx, records []FileLumis) error {
	valueStrings := []string{}
	valueArgs := []interface{}{}
	var stm string
	var err error
	var valArr string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	if len(records) == 0 {
		return errors.New("zero array of FileLumi records")
	}
	r := records[0]
	if r.EVENT_COUNT != 0 {
		stm, err = LoadTemplateSQL("insert_filelumis", tmpl)
		valArr = "(?,?,?,?)"
		if DBOWNER != "sqlite" {
			valArr = "(:r,:l,:f,:e)"
		}
	} else {
		stm, err = LoadTemplateSQL("insert_filelumis2", tmpl)
		valArr = "(?,?,?)"
		if DBOWNER != "sqlite" {
			valArr = "(:r,:l,:f)"
		}
	}
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("Fail to load template", err)
		}
		return err
	}
	stm = strings.Split(stm, "VALUES")[0]
	stmOra := fmt.Sprintf("INSERT ALL")
	for _, r := range records {
		valueStrings = append(valueStrings, valArr)
		names := "RUN_NUM,LUMI_SECTION_NUM,FILE_ID,EVENT_COUNT"
		vals := ":r,:l,:f,:e"
		if r.EVENT_COUNT != 0 {
			valueArgs = append(valueArgs, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID, r.EVENT_COUNT)
		} else {
			valueArgs = append(valueArgs, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID)
			names = "RUN_NUM,LUMI_SECTION_NUM,FILE_ID"
			vals = ":r,:l,:f"
		}
		stmOra = fmt.Sprintf("%s\nINTO %s.FILE_LUMIS (%s) VALUES (%s)", stmOra, DBOWNER, names, vals)
	}
	stm = fmt.Sprintf("%s VALUES %s", stm, strings.Join(valueStrings, ","))
	stmOra = fmt.Sprintf("%s\nSELECT * FROM dual", stmOra)
	if DBOWNER != "sqlite" {
		stm = stmOra
	}
	if utils.VERBOSE > 1 {
		log.Printf("Insert FileLumis bulk\n%s\n%+v FileLumi values", stm, len(valueArgs))
	}
	if utils.VERBOSE > 2 {
		log.Printf("new statement\n%v\n%v", stm, valueArgs)
	}
	_, err = tx.Exec(stm, valueArgs...)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("Unable to insert FileLumis records, error %v", err)
		}
	}
	return err
}

// InsertFileLumisTxViaMerge DBS API
func InsertFileLumisTxViaMerge(tx *sql.Tx, records []FileLumis) error {

	// create temp table
	stm := getSQL("temp_filelumis")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		args := []interface{}{}
		utils.PrintSQL(stm, args, "execute")
	}
	_, err := tx.Exec(stm)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("Unable to create temp FileLumis table, error %v", err)
		}
		return err
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
			go insertFLChunk(tx, &wg, records[i:size])
			ngoroutines += 1
		}
		limit := k + maxSize
		if limit > nrec {
			limit = nrec
		}
		if utils.VERBOSE > 0 {
			log.Printf("process %d goroutines, step %d-%d, elapsed time %v", ngoroutines, k, limit, time.Since(t0))
		}
		wg.Wait()
	}

	// merge temp table back
	stm = getSQL("merge_filelumis")
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
		return err
	}

	return err
}

// helper function to insert FileLumis chunk via ORACLE INSERT ALL statement
func insertFLChunk(tx *sql.Tx, wg *sync.WaitGroup, records []FileLumis) error {
	defer wg.Done()
	valueStrings := []string{}
	valueArgs := []interface{}{}
	if len(records) == 0 {
		msg := "zero array of FileLumi records"
		log.Println(msg)
		return errors.New(msg)
	}

	// prepare statement for insering all rows
	stm := fmt.Sprintf("INSERT ALL")
	for _, r := range records {
		names := "RUN_NUM,LUMI_SECTION_NUM,FILE_ID,EVENT_COUNT"
		vals := ":r,:l,:f,:e"
		if r.EVENT_COUNT != 0 {
			valueArgs = append(valueArgs, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID, r.EVENT_COUNT)
		} else {
			valueArgs = append(valueArgs, r.RUN_NUM, r.LUMI_SECTION_NUM, r.FILE_ID)
			names = "RUN_NUM,LUMI_SECTION_NUM,FILE_ID"
			vals = ":r,:l,:f"
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", vals))
		stm = fmt.Sprintf("%s\nINTO ORA$PTT_TEMP_FILE_LUMIS (%s) VALUES (%s)", stm, names, vals)
	}
	stm = fmt.Sprintf("%s\nSELECT * FROM dual", stm)
	if utils.VERBOSE > 1 {
		log.Printf("Insert FileLumis bulk\n%s\n%+v FileLumi values", stm, len(valueArgs))
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 2 {
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
	}
	return err
}
