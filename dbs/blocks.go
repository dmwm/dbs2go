package dbs

import (
	"database/sql"
	"encoding/json"
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
		return Error(err, ParseErrorCode, "", "dbs.blocks.Blocks")
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
		return Error(err, LoadErrorCode, "", "dbs.blocks.Blocks")
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.blocks.Blocks")
	}
	return nil
}

// Blocks represents Blocks DBS DB table
type Blocks struct {
	BLOCK_ID               int64  `json:"block_id"`
	BLOCK_NAME             string `json:"block_name" validate:"required"`
	DATASET_ID             int64  `json:"dataset_id" validate:"required,number,gt=0"`
	OPEN_FOR_WRITING       int64  `json:"open_for_writing" validate:"number"`
	ORIGIN_SITE_NAME       string `json:"origin_site_name" validate:"required"`
	BLOCK_SIZE             int64  `json:"block_size" validate:"number"`
	FILE_COUNT             int64  `json:"file_count" validate:"number"`
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
			return Error(err, LastInsertErrorCode, "", "dbs.blocks.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.blocks.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_blocks")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Blocks\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.BLOCK_ID,
		r.BLOCK_NAME,
		r.DATASET_ID,
		r.OPEN_FOR_WRITING,
		r.ORIGIN_SITE_NAME,
		r.BLOCK_SIZE,
		r.FILE_COUNT,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("fail to insert block", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.blocks.Insert")
	}
	return nil
}

// Validate implementation of Blocks
func (r *Blocks) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("block", r.BLOCK_NAME); err != nil {
		return Error(err, PatternErrorCode, "", "dbs.blocks.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return Error(InvalidParamErr, PatternErrorCode, "invalid pattern for creation date", "dbs.blocks.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.LAST_MODIFICATION_DATE)); !matched {
		return Error(InvalidParamErr, PatternErrorCode, "invalid pattern for last modification date", "dbs.blocks.Validate")
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
		return Error(err, ReaderErrorCode, "", "dbs.blocks.Decode")
	}
	err = json.Unmarshal(data, &r)

	// check if open_for_writing was present in request, if not set it to 1
	if !strings.Contains(string(data), "open_for_writing") {
		r.OPEN_FOR_WRITING = 1
	}

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.blocks.Decode")
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
// implement the following logic
// input values: blockname
// optional values: open_for_writing, origin_site(name), block_size,
// file_count, creation_date, create_by, last_modification_date, last_modified_by
// It insert given data in the following steps:
// - obtain dataset_id from given ds_name
// - increment block id
// - insert block input
func (a *API) InsertBlocks() error {

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.blocks.InsertBlocks")
	}
	rec := BlockRecord{CREATE_BY: a.CreateBy, LAST_MODIFIED_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.blocks.InsertBlocks")
	}

	// check if open_for_writing was present in request, if not set it to 1
	if !strings.Contains(string(data), "open_for_writing") {
		rec.OPEN_FOR_WRITING = 1
	}

	// set dependent's records
	brec := Blocks{
		BLOCK_NAME:             rec.BLOCK_NAME,
		OPEN_FOR_WRITING:       rec.OPEN_FOR_WRITING,
		ORIGIN_SITE_NAME:       rec.ORIGIN_SITE_NAME,
		BLOCK_SIZE:             rec.BLOCK_SIZE,
		FILE_COUNT:             rec.FILE_COUNT,
		CREATION_DATE:          rec.CREATION_DATE,
		CREATE_BY:              rec.CREATE_BY,
		LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE,
		LAST_MODIFIED_BY:       rec.LAST_MODIFIED_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "", "dbs.blocks.InsertBlocks")
	}
	defer tx.Rollback()

	// check if our data already exist in DB
	if IfExist(tx, "BLOCKS", "block_id", "block_name", rec.BLOCK_NAME) {
		if a.Writer != nil {
			a.Writer.Write([]byte(`[]`))
		}
		return nil
	}

	// get all necessary IDs from different tables
	dataset := strings.Split(rec.BLOCK_NAME, "#")[0]
	dsId, err := GetID(tx, "DATASETS", "dataset_id", "dataset", dataset)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find dataset_id for", dataset)
		}
		return Error(err, GetIDErrorCode, "", "dbs.blocks.InsertBlocks")
	}

	// assign all Id's in dataset DB record
	brec.DATASET_ID = dsId
	err = brec.Insert(tx)
	if err != nil {
		return Error(err, InsertErrorCode, "", "dbs.blocks.InsertBlocks")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return Error(err, CommitErrorCode, "", "dbs.blocks.InsertBlocks")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
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
		msg := "invalid block_name parameter"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blocks.UpdateBlocks")
	}
	if createBy == "" {
		msg := "invalid create_by parameter"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blocks.UpdateBlocks")
	}
	if site {
		if origSiteName == "" {
			msg := "invalid origin_site_name parameter"
			return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blocks.UpdateBlocks")
		}
	} else {
		if openForWriting < 0 || openForWriting > 1 {
			msg := "invalid open_for_writing parameter"
			return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blocks.UpdateBlocks")
		}
	}

	// load teamplte
	tmplData := make(Record)
	tmplData["Site"] = site
	tmplData["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("update_blocks", tmplData)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to load update_blocks template", err)
		}
		return Error(err, LoadErrorCode, "", "dbs.blocks.UpdateBlocks")
	}

	if utils.VERBOSE > 0 {
		log.Printf("update Blocks\n%s", stm)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return Error(err, TransactionErrorCode, "", "dbs.blocks.UpdateBlocks")
	}
	defer tx.Rollback()

	if site {
		_, err = tx.Exec(stm, origSiteName, createBy, date, blockName)
	} else {
		_, err = tx.Exec(stm, openForWriting, createBy, date, blockName)
	}
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to update %v", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.blocks.UpdateBlocks")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return Error(err, CommitErrorCode, "", "dbs.blocks.UpdateBlocks")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// UpdateBlockStats DBS API
func (a *API) UpdateBlockStats(tx *sql.Tx, blockID int64) error {
	tmplData := make(Record)
	tmplData["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("block_stats", tmplData)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to load update_block_stats template", err)
		}
		return Error(err, LoadErrorCode, "", "dbs.blocks.UpdateBlockStats")
	}
	var fileCount, bid int64
	var blkSize float64
	err = tx.QueryRow(stm, blockID).Scan(&fileCount, &blkSize, &bid)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to load block_stats template", err)
		}
		return Error(err, QueryErrorCode, "", "dbs.blocks.UpdateBlockStats")
	}

	stm, err = LoadTemplateSQL("update_block_stats", tmplData)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to load update_block_stats template", err)
		}
		return Error(err, LoadErrorCode, "", "dbs.blocks.UpdateBlockStats")
	}

	if utils.VERBOSE > 0 {
		log.Printf("UpdateBlockStats\n%s\n%+v", stm)
	}
	_, err = tx.Exec(stm, fileCount, int64(blkSize), blockID)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to update block stats", stm, "error", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.blocks.UpdateBlockStats")
	}
	return nil
}
