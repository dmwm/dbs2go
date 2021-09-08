package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/vkuznet/dbs2go/utils"
)

// Blocks DBS API
//gocyclo:ignore
func (a *API) Blocks() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["TokenGenerator"] = ""
	tmpl["Detail"] = false

	// parse detail argument
	detail, _ := getSingleValue(a.Params, "detail")
	if detail == "1" { // for backward compatibility with Python detail=1 and detail=True
		detail = "true"
	}
	if strings.ToLower(detail) == "true" {
		tmpl["Detail"] = true
	}

	// use run_num first since it may produce TokenGenerator
	// which should contain bind variables
	runs, err := ParseRuns(getValues(a.Params, "run_num"))
	if err != nil {
		return err
	}
	if len(runs) > 0 {
		tmpl["Runs"] = true
		token, whereRuns, bindsRuns := runsClause("FLM", runs)
		tmpl["TokenGenerator"] = token
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}
	// parse arguments
	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) == 1 {
		tmpl["Lfns"] = true
		conds, args = AddParam("logical_file_name", "FL.LOGICAL_FILE_NAME", a.Params, conds, args)
	}

	conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	conds, args = AddParam("dataset", "DS.DATASET", a.Params, conds, args)
	conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", a.Params, conds, args)
	conds, args = AddParam("cdate", "B.CREATION_DATE", a.Params, conds, args)

	minDate := getValues(a.Params, "min_cdate")
	maxDate := getValues(a.Params, "max_cdate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE BETWEEN %s and %s", placeholder("min_cdate"), placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE > %s", placeholder("min_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE < %s", placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}

	conds, args = AddParam("ldate", "B.LAST_MODIFICATION_DATE", a.Params, conds, args)

	minDate = getValues(a.Params, "min_ldate")
	maxDate = getValues(a.Params, "max_ldate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE BETWEEN %s and %s", placeholder("min_ldate"), placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE > %s", placeholder("min_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" B.CREATION_DATE < %s", placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}
	stm, err := LoadTemplateSQL("blocks", tmpl)
	if err != nil {
		return err
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// Blocks represents Blocks DBS DB table
type Blocks struct {
	BLOCK_ID               int64  `json:"block_id"`
	BLOCK_NAME             string `json:"block_name" validate:"required"`
	DATASET_ID             int64  `json:"dataset_id" validate:"required,number,gt=0"`
	OPEN_FOR_WRITING       int64  `json:"open_for_writing" validate:"required,number"`
	ORIGIN_SITE_NAME       string `json:"origin_site_name" validate:"required"`
	BLOCK_SIZE             int64  `json:"block_size" validate:"required,number"`
	FILE_COUNT             int64  `json:"file_count" validate:"required,number"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
}

// Insert implementation of Blocks
func (r *Blocks) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.BLOCK_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "BLOCKS", "block_id")
			r.BLOCK_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_BK")
			r.BLOCK_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_blocks")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Blocks\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.BLOCK_ID, r.BLOCK_NAME, r.DATASET_ID, r.OPEN_FOR_WRITING, r.ORIGIN_SITE_NAME, r.BLOCK_SIZE, r.FILE_COUNT, r.CREATION_DATE, r.CREATE_BY, r.LAST_MODIFICATION_DATE, r.LAST_MODIFIED_BY)
	return err
}

// Validate implementation of Blocks
func (r *Blocks) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("block", r.BLOCK_NAME); err != nil {
		return err
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.LAST_MODIFICATION_DATE)); !matched {
		return errors.New("invalid pattern for last modification date")
	}
	return nil
}

// SetDefaults implements set defaults for Blocks
func (r *Blocks) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		r.LAST_MODIFICATION_DATE = Date()
	}
}

// Decode implementation for Blocks
func (r *Blocks) Decode(reader io.Reader) error {
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

// BlockRecord represents input record for insert blocks API
type BlockRecord struct {
	BLOCK_NAME             string `json:"block_name"`
	OPEN_FOR_WRITING       int64  `json:"open_for_writing"`
	ORIGIN_SITE_NAME       string `json:"origin_site_name"`
	BLOCK_SIZE             int64  `json:"block_size"`
	FILE_COUNT             int64  `json:"file_count"`
	CREATION_DATE          int64  `json:"creation_date"`
	CREATE_BY              string `json:"create_by"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
	LAST_MODIFIED_BY       string `json:"last_modified_by"`
}

// InsertBlocks DBS API
func (a *API) InsertBlocks() error {
	// implement the following logic
	// input values: blockname
	// optional values: open_for_writing, origin_site(name), block_size, file_count, creation_date, create_by, last_modification_date, last_modified_by
	// blkinput["dataset_id"] = self.datasetid.execute(conn,  ds_name, tran)
	// blkinput["block_id"] =  self.sm.increment(conn, "SEQ_BK", tran)
	// self.blockin.execute(conn, blkinput, tran)

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := BlockRecord{CREATE_BY: a.CreateBy, LAST_MODIFIED_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	// set dependent's records
	brec := Blocks{BLOCK_NAME: rec.BLOCK_NAME, OPEN_FOR_WRITING: rec.OPEN_FOR_WRITING, ORIGIN_SITE_NAME: rec.ORIGIN_SITE_NAME, BLOCK_SIZE: rec.BLOCK_SIZE, FILE_COUNT: rec.FILE_COUNT, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY, LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE, LAST_MODIFIED_BY: rec.LAST_MODIFIED_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// get all necessary IDs from different tables
	dataset := strings.Split(rec.BLOCK_NAME, "#")[0]
	dsId, err := GetID(tx, "DATASETS", "dataset_id", "dataset", dataset)
	if err != nil {
		log.Println("unable to find dataset_id for", dataset)
		return err
	}

	// assign all Id's in dataset DB record
	brec.DATASET_ID = dsId
	err = brec.Insert(tx)
	if err != nil {
		return err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return err
	}
	return err
}

// UpdateBlocks DBS API
//gocyclo:ignore
func (a *API) UpdateBlocks() error {
	// get input parameters
	date := time.Now().Unix()
	var createBy string
	var blockName string
	var origSiteName string
	var openForWriting int

	if v, ok := a.Params["block_name"]; ok {
		blockName = v.(string)
	}
	site := false
	if v, ok := a.Params["origin_site_name"]; ok {
		origSiteName = v.(string)
		site = true
	}
	if v, ok := a.Params["open_for_writing"]; ok {
		val, err := strconv.Atoi(v.(string))
		if err != nil {
			log.Println("invalid input parameter", err)
		}
		openForWriting = val
	}
	if v, ok := a.Params["create_by"]; ok {
		createBy = v.(string)
	}

	// validate input parameters
	if blockName == "" {
		return errors.New("invalid block_name parameter")
	}
	if createBy == "" {
		return errors.New("invalid create_by parameter")
	}
	if site {
		if origSiteName == "" {
			return errors.New("invalid origin_site_name parameter")
		}
	} else {
		if openForWriting < 0 || openForWriting > 1 {
			return errors.New("invalid open_for_writing parameter")
		}
	}

	// load teamplte
	tmplData := make(Record)
	tmplData["Site"] = site
	stm, err := LoadTemplateSQL("update_blocks", tmplData)
	if err != nil {
		log.Println("unable to load update_blocks template", err)
		return err
	}

	if utils.VERBOSE > 0 {
		log.Printf("update Blocks\n%s\n%+v", stm)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return err
	}
	defer tx.Rollback()

	if site {
		_, err = tx.Exec(stm, origSiteName, createBy, date, blockName)
	} else {
		_, err = tx.Exec(stm, openForWriting, createBy, date, blockName)
	}
	if err != nil {
		log.Printf("unable to update %v", err)
		return err
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return err
	}
	return err
}
