package dbs

// nolint: gocyclo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/dbs2go/utils"
)

// Files DBS API
//
//gocyclo:ignore
func (a *API) Files() error {
	var args []interface{}
	var conds, lumis []string
	var lumigen, rungen, lfngen, runList, lfnList bool
	var sumOverLumi string
	var err error

	if len(a.Params) == 0 {
		msg := "Files API with empty parameter map"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Files")
	}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Addition"] = false
	tmpl["RunNumber"] = false
	tmpl["LumiList"] = false
	tmpl["Addition"] = false
	tmpl["Detail"] = false

	// parse detail argument
	detail, _ := getSingleValue(a.Params, "detail")
	if detail == "1" { // for backward compatibility with Python detail=1 and detail=True
		detail = "true"
	}
	if strings.ToLower(detail) == "true" {
		tmpl["Detail"] = true
	}

	// parse sumOverLumi
	if _, ok := a.Params["sumOverLumi"]; ok {
		sumOverLumi, err = getSingleValue(a.Params, "sumOverLumi")
		if err != nil {
			return Error(err, InvalidParameterErrorCode, "invalid sum over lumi parameter", "dbs.files.Files")
		}
	}

	lumiList := getValues(a.Params, "lumi_list")
	lumis, err = FlatLumis(lumiList)
	if err != nil {
		return Error(err, InvalidParameterErrorCode, "invalid lumi_list parameter", "dbs.files.Files")
	}
	runs := getValues(a.Params, "run_num")
	if len(runs) > 0 {
		tmpl["RunNumber"] = true
	}

	if len(lumis) > 0 {
		tmpl["LumiList"] = true
	}

	validFileOnly := getValues(a.Params, "validFileOnly")
	if len(validFileOnly) == 1 {
		_, val := OperatorValue(validFileOnly[0])
		if val == "1" {
			cond := "F.IS_FILE_VALID = 1"
			conds = append(conds, cond)
			cond = "DT.DATASET_ACCESS_TYPE in ('VALID', 'PRODUCTION')"
			conds = append(conds, cond)
		} else {
			cond := "F.IS_FILE_VALID <> -1"
			conds = append(conds, cond)
		}
	} else {
		cond := "F.IS_FILE_VALID <> -1"
		conds = append(conds, cond)
	}

	conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)
	conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	if _, e := getSingleValue(a.Params, "release_version"); e == nil {
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", a.Params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(a.Params, "pset_hash"); e == nil {
		conds, args = AddParam("pset_hash", "PSH.PSET_HASH", a.Params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(a.Params, "app_name"); e == nil {
		conds, args = AddParam("app_name", "AEX.APP_NAME", a.Params, conds, args)
		tmpl["Addition"] = true
	}
	if _, e := getSingleValue(a.Params, "output_module_label"); e == nil {
		conds, args = AddParam("output_module_label", "OMC.OUTPUT_MODULE_LABEL", a.Params, conds, args)
		tmpl["Addition"] = true
	}
	conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", a.Params, conds, args)

	// load our SQL statement
	stm, err := LoadTemplateSQL("files", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "unable to load files sql template", "dbs.files.Files")
	}

	// add lfns conditions
	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) > 1 {
		lfngen = true
		lfnList = true
		token, binds := TokenGenerator(lfns, 30, "lfn_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME in %s", TokenCondition())
		conds = append(conds, cond)
		// since token comes first in SQL statement we should
		// put binding args first as well
		var newArgs []interface{}
		for _, v := range binds {
			newArgs = append(newArgs, v)
		}
		for _, v := range args {
			newArgs = append(newArgs, v)
		}
		args = newArgs
	} else if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", a.Params, conds, args)
	}

	// files API does not support run_num=1 when no lumi and lfns
	if len(runs) == 1 && len(lumis) == 0 && runs[0] == "1" && len(lfns) == 0 {
		msg := "files API does not support run_num=1 when no lumi and lfns list provided"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Files")
	}

	// add run conditions
	t, c, na, e := RunsConditions(runs, "FL")
	if e != nil {
		return Error(e, InvalidParameterErrorCode, "invalid runs conditions parameter", "dbs.files.Files")
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
	if len(na) > 1 { // if we have more then one run arguments
		rungen = true
	}
	if _, ok := a.Params["runList"]; ok {
		// if our run value was send via POST payload as [97], then it is a rungen
		// and not single run value like 97
		if sumOverLumi == "1" {
			runList = true
			rungen = true
		}
	}

	// add lumis conditions
	if len(lumis) > 1 {
		lumigen = true
		token, binds := TokenGenerator(lumis, 4000, "lumis_token")
		if sumOverLumi != "1" {
			stm = fmt.Sprintf("%s %s", token, stm)
		}
		cond := fmt.Sprintf(" FL.LUMI_SECTION_NUM in %s", TokenCondition())
		conds = append(conds, cond)
		// since token comes first in SQL statement we should
		// put binding args first as well
		var newArgs []interface{}
		for _, v := range binds {
			newArgs = append(newArgs, v)
		}
		for _, v := range args {
			newArgs = append(newArgs, v)
		}
		args = newArgs
		tmpl["LumiGenerator"] = token
	} else if len(lumis) == 1 {
		conds, args = AddParam("lumi_list", "FL.LUMI_SECTION_NUM", a.Params, conds, args)
	}

	if (rungen && lfngen) || (lumigen && lfngen) || (rungen && lumigen) {
		msg := "cannot supply more than one list (lfn, run_num or lumi) at one query"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Files")
	}

	// check sumOverLumi conditions
	if sumOverLumi == "1" && runList {
		msg := "When sumOverLumi=1, no run_num list is allowed"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Files")
	}
	if sumOverLumi == "1" && lfnList {
		msg := "When sumOverLumi=1, no lfn list is allowed"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Files")
	}
	if len(runs) > 0 && sumOverLumi == "1" {
		stm = strings.Replace(stm, "F.EVENT_COUNT,", "", -1)
		stm = WhereClause(stm, conds)
		tmpl["Statement"] = stm
		stm, err = LoadTemplateSQL("files_sumoverlumi", tmpl)
		if err != nil {
			return Error(err, LoadErrorCode, "fail to load files_sumoverlumi sql template", "dbs.files.Files")
		}
	} else {
		stm = WhereClause(stm, conds)
	}

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "query error", "dbs.files.Files")
	}
	return nil
}

// Files represents Files DBS DB table
type Files struct {
	FILE_ID                int64   `json:"file_id"`
	LOGICAL_FILE_NAME      string  `json:"logical_file_name" validate:"required"`
	IS_FILE_VALID          int64   `json:"is_file_valid" validate:"number"`
	DATASET_ID             int64   `json:"dataset_id" validate:"number,gt=0"`
	BLOCK_ID               int64   `json:"block_id" validate:"number,gt=0"`
	FILE_TYPE_ID           int64   `json:"file_type_id" validate:"number,gt=0"`
	CHECK_SUM              string  `json:"check_sum" validate:"required"`
	FILE_SIZE              int64   `json:"file_size" validate:"required,number,gt=0"`
	EVENT_COUNT            int64   `json:"event_count" validate:"number"`
	ADLER32                string  `json:"adler32" validate:"required"`
	MD5                    string  `json:"md5"`
	AUTO_CROSS_SECTION     float64 `json:"auto_cross_section"`
	CREATION_DATE          int64   `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY              string  `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64   `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string  `json:"last_modified_by" validate:"required"`
}

// helper function to get next available FileID
func getFileID(tx *sql.Tx) (int64, error) {
	var err error
	var tid int64
	if DBOWNER == "sqlite" {
		tid, err = LastInsertID(tx, "FILES", "file_id")
		tid += 1
	} else {
		tid, err = IncrementSequence(tx, "SEQ_FL")
	}
	if err != nil {
		return tid, Error(err, LastInsertErrorCode, "", "dbs.files.getFileID")
	}
	return tid, nil
}

// Insert implementation of Files
func (r *Files) Insert(tx *sql.Tx) error {
	var err error
	if r.FILE_ID == 0 {
		fileID, err := getFileID(tx)
		if err != nil {
			log.Println("unable to get fileID", err)
			return Error(err, GetFileIDErrorCode, "unable to get file id", "dbs.files.Insert")
		}
		r.FILE_ID = fileID
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "fail to validate file record", "dbs.files.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_files")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Files file_id=%d lfn=%s", r.FILE_ID, r.LOGICAL_FILE_NAME)
	} else if utils.VERBOSE > 1 {
		log.Printf("Insert Files\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.FILE_ID,
		r.LOGICAL_FILE_NAME,
		r.IS_FILE_VALID,
		r.DATASET_ID,
		r.BLOCK_ID,
		r.FILE_TYPE_ID,
		r.CHECK_SUM,
		r.FILE_SIZE,
		r.EVENT_COUNT,
		r.ADLER32,
		r.MD5,
		r.AUTO_CROSS_SECTION,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert files, error", err)
		}
		return Error(err, InsertFileErrorCode, "unable to insert file record", "dbs.files.Insert")
	}
	return nil
}

// Validate implementation of Files
func (r *Files) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("logical_file_name", r.LOGICAL_FILE_NAME); err != nil {
		return Error(err, InvalidParameterErrorCode, "invalid lfn", "dbs.files.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.LAST_MODIFICATION_DATE)); !matched {
		msg := "invalid pattern for last modification date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.files.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Files
func (r *Files) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		r.LAST_MODIFICATION_DATE = Date()
	}
}

// Decode implementation for Files
func (r *Files) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read file record", "dbs.files.Decode")
	}
	err = json.Unmarshal(data, &r)

	// check if is_file_valid was present in request, if not set it to 1
	if !strings.Contains(string(data), "is_file_valid") {
		r.IS_FILE_VALID = 1
	}

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode file record", "dbs.files.Decode")
	}
	return nil
}

// type FileParent struct {
//     FileParentLfn string `json:"file_parent_lfn"`
// }

// FileParentLFNRecord represents file parent recoord supplied in file parent list of FileRecord
type FileParentLFNRecord struct {
	FILE_PARENT_LFN string `json:"file_parent_lfn"`
}

// FileRecord represents input record for insert files API
type FileRecord struct {
	LOGICAL_FILE_NAME      string  `json:"logical_file_name"`
	IS_FILE_VALID          int64   `json:"is_file_valid"`
	DATASET                string  `json:"dataset"`
	BLOCK_NAME             string  `json:"block_name"`
	FILE_TYPE              string  `json:"file_type"`
	CHECK_SUM              string  `json:"check_sum"`
	FILE_SIZE              int64   `json:"file_size"`
	EVENT_COUNT            int64   `json:"event_count"`
	ADLER32                string  `json:"adler32"`
	MD5                    string  `json:"md5"`
	AUTO_CROSS_SECTION     float64 `json:"auto_cross_section"`
	CREATION_DATE          int64   `json:"creation_date"`
	CREATE_BY              string  `json:"create_by"`
	LAST_MODIFICATION_DATE int64   `json:"last_modification_date"`
	LAST_MODIFIED_BY       string  `json:"last_modified_by"`

	FILE_LUMI_LIST          []FileLumi            `json:"file_lumi_list"`
	FILE_PARENT_LIST        []FileParentLFNRecord `json:"file_parent_list"`
	FILE_OUTPUT_CONFIG_LIST []OutputConfigRecord  `json:"file_output_config"`
}

// PyFileRecord represents DBS python input file record structure
type PyFileRecord struct {
	Records []FileRecord `json:"files"`
}

// InsertFiles DBS API implements the following logic:
//
// - extract dataset id for given dataset
// - extract output mod config for given dataset
// - get block information for given block
// - get file type id for given file type
// - loop over file parents and insert associative information into file tables
// - insert block parentage info
// - insert dataset parentage info
// - update block's info
//
//gocyclo:ignore
func (a *API) InsertFiles() error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read file record", "dbs.files.InsertFiles")
	}
	var records []FileRecord
	var pyrec PyFileRecord
	err = json.Unmarshal(data, &pyrec)
	if err != nil {
		log.Println("unable to decode input file record", err, "will proceed with []FileRecord")
		// proceed with file record list
		err = json.Unmarshal(data, &records)
		if err != nil {
			log.Println("fail to decode data", err)
			return Error(err, UnmarshalErrorCode, "unable to decode file record", "dbs.files.InsertFiles")
		}
	} else {
		records = pyrec.Records
	}

	for _, rec := range records {
		if rec.CREATE_BY == "" {
			rec.CREATE_BY = a.CreateBy
		}
		if rec.LAST_MODIFIED_BY == "" {
			rec.LAST_MODIFIED_BY = a.CreateBy
		}
		if utils.VERBOSE > 1 {
			log.Printf("insert %+v", rec)
		}
		// check if is_file_valid was present in request, if not set it to 1
		if !strings.Contains(string(data), "is_file_valid") {
			rec.IS_FILE_VALID = 1
		}

		// set dependent's records
		frec := Files{
			LOGICAL_FILE_NAME:      rec.LOGICAL_FILE_NAME,
			IS_FILE_VALID:          rec.IS_FILE_VALID,
			CHECK_SUM:              rec.CHECK_SUM,
			FILE_SIZE:              rec.FILE_SIZE,
			EVENT_COUNT:            rec.EVENT_COUNT,
			ADLER32:                rec.ADLER32,
			MD5:                    rec.MD5,
			AUTO_CROSS_SECTION:     rec.AUTO_CROSS_SECTION,
			CREATION_DATE:          rec.CREATION_DATE,
			CREATE_BY:              rec.CREATE_BY,
			LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE,
			LAST_MODIFIED_BY:       rec.LAST_MODIFIED_BY}

		// start transaction
		tx, err := DB.Begin()
		if err != nil {
			return Error(err, TransactionErrorCode, "unable to start transaction", "dbs.files.InsertFiles")
		}
		defer tx.Rollback()

		// check if our data already exist in DB
		if IfExist(tx, "FILES", "file_id", "logical_file_name", rec.LOGICAL_FILE_NAME) {
			if utils.VERBOSE > 1 {
				log.Printf("skip %s as it already exists in DB", rec.LOGICAL_FILE_NAME)
			}
			continue
		}

		// get all necessary IDs from different tables
		blkId, err := GetID(tx, "BLOCKS", "block_id", "block_name", rec.BLOCK_NAME)
		if err != nil {
			msg := fmt.Sprintf("unable to find block_id for %s", rec.BLOCK_NAME)
			if utils.VERBOSE > 0 {
				log.Println(msg)
			}
			return Error(err, GetBlockIDErrorCode, msg, "dbs.files.InsertFiles")
		}
		dsId, err := GetID(tx, "DATASETS", "dataset_id", "dataset", rec.DATASET)
		if err != nil {
			msg := fmt.Sprintf("unable to find dataset_id for %s", rec.DATASET)
			if utils.VERBOSE > 0 {
				log.Println(msg)
			}
			return Error(err, GetDatasetIDErrorCode, msg, "dbs.files.InsertFiles")
		}
		ftId, err := GetID(tx, "FILE_DATA_TYPES", "file_type_id", "file_type", rec.FILE_TYPE)
		if err != nil {
			if utils.VERBOSE > 0 {
				log.Println("unable to find file_type_id for", rec.FILE_TYPE)
			}
			// we will insert new file type
			ftrec := FileDataTypes{FILE_TYPE: rec.FILE_TYPE}
			err = ftrec.Insert(tx)
			if err != nil {
				return Error(err, InsertFileDataTypeErrorCode, "unable to insert file data types record", "dbs.files.InsertFiles")
			}
			ftId, err = GetID(tx, "FILE_DATA_TYPES", "file_type_id", "file_type", rec.FILE_TYPE)
			if err != nil {
				return Error(err, GetFileDataTypeIDErrorCode, "unable to get file data type id", "dbs.files.InsertFiles")
			}
		}

		// assign all Id's in dataset DB record
		frec.DATASET_ID = dsId
		frec.BLOCK_ID = blkId
		frec.FILE_TYPE_ID = ftId
		err = frec.Insert(tx)
		if err != nil {
			return Error(err, InsertFileErrorCode, "unable to insert file record", "dbs.files.InsertFiles")
		}

		// get current file ID
		fid, err := GetID(tx, "FILES", "file_id", "logical_file_name", rec.LOGICAL_FILE_NAME)
		if err != nil {
			return Error(err, GetFileIDErrorCode, "unable to get file id", "dbs.files.InsertFiles")
		}

		// * from dbs/bulkblocks.go line 546
		tempTable := fmt.Sprintf("ORA$PTT_TEMP_FILE_LUMIS_%d", time.Now().UnixMicro())
		if DBOWNER == "sqlite" {
			tempTable = "FILE_LUMIS"
		}

		// insert fileLumiList, depending on method
		err = a.SelectFileLumiListInsert(tx, rec.FILE_LUMI_LIST, tempTable, fid, "dbs.files.InsertFiles")
		if err != nil {
			return err
		}

		// insert file parent list
		for _, p := range rec.FILE_PARENT_LIST {
			// get current file ID
			fid, err := GetID(tx, "FILES", "file_id", "logical_file_name", rec.LOGICAL_FILE_NAME)
			if err != nil {
				msg := fmt.Sprintf("unable to get file id for %s", rec.LOGICAL_FILE_NAME)
				return Error(err, GetFileIDErrorCode, msg, "dbs.files.InsertFiles")
			}
			// get parent file ID
			pid, err := GetID(tx, "FILES", "file_id", "logical_file_name", p.FILE_PARENT_LFN)
			if err != nil {
				msg := fmt.Sprintf("unable to get file id for %s", p.FILE_PARENT_LFN)
				return Error(err, GetFileIDErrorCode, msg, "dbs.files.InsertFiles")
			}
			// inject file parents record
			r := FileParents{THIS_FILE_ID: fid, PARENT_FILE_ID: pid}
			err = r.Insert(tx)
			if err != nil {
				return Error(err, InsertFileParentErrorCode, "unable to insert file parent record", "dbs.files.InsertFiles")
			}
		}

		// match output_mod_config
		for _, oc := range rec.FILE_OUTPUT_CONFIG_LIST {
			ocid, err := GetID(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id", "output_module_label", oc.OUTPUT_MODULE_LABEL)
			if err != nil {
				msg := fmt.Sprintf("unable to get output_module_config for %s", oc.OUTPUT_MODULE_LABEL)
				return Error(err, GetOutputModConfigIDErrorCode, msg, "dbs.files.InsertFiles")
			}
			r := FileOutputModConfigs{
				OUTPUT_MOD_CONFIG_ID: ocid,
				FILE_ID:              fid,
			}
			err = r.Insert(tx)
			if err != nil {
				return Error(err, InsertFileOutputModConfigErrorCode, "unable to insert file output mod config record", "dbs.files.InsertFiles")
			}
		}

		// we need to update block info about inserted file
		a.UpdateBlockStats(tx, blkId)

		// commit transaction
		err = tx.Commit()
		if err != nil {
			log.Println("fail to commit transaction", err)
			return Error(err, InsertFileErrorCode, "unable to commit insert file record", "dbs.files.InsertFiles")
		}
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// UpdateFiles DBS API
//
//gocyclo:ignore
func (a *API) UpdateFiles() error {

	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["TokenGenerator"] = ""
	tmpl["Lfns"] = false
	tmpl["Dataset"] = false
	tmpl["SQLite"] = false
	if strings.ToLower(DBOWNER) == "sqlite" {
		tmpl["SQLite"] = true
	}

	// read input parameters
	if utils.VERBOSE > 1 {
		log.Printf("UpdateFiles params %+v", a.Params)
	}
	var createBy string
	var isFileValid int
	vals := getValues(a.Params, "is_file_valid")
	if len(vals) > 0 {
		val, err := strconv.Atoi(vals[0])
		if err != nil {
			log.Println("invalid input parameter", err)
		}
		isFileValid = val
	}
	if v, ok := a.Params["create_by"]; ok {
		switch t := v.(type) {
		case string:
			createBy = t
		case []string:
			createBy = t[0]
		}
	}
	tstamp := time.Now().Unix()
	// keep that order since it is present in sql statement
	args = append(args, createBy)
	args = append(args, tstamp)
	args = append(args, isFileValid)

	// additional where clause parameters
	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) == 1 {
		tmpl["Lfns"] = true
		if strings.ToLower(DBOWNER) == "sqlite" {
			conds, args = AddParam("logical_file_name", "LOGICAL_FILE_NAME", a.Params, conds, args)
		} else {
			conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", a.Params, conds, args)
		}
	}
	if _, ok := a.Params["dataset"]; ok {
		tmpl["Dataset"] = true
		conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("update_files", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "fail to load update_files sql template", "dbs.files.UpdateFiles")
	}
	if len(lfns) == 1 {
		// we only add where clause if lfns are present
		// for dataset where clause it is not necessary since
		// update_files.sql contains it as part of the wrapped query
		stm = WhereClause(stm, conds)
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return Error(err, TransactionErrorCode, "unable to start transaction", "dbs.files.UpdateFiles")
	}
	defer tx.Rollback()
	_, err = tx.Exec(stm, args...)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to update %v", err)
		}
		return Error(err, UpdateFileErrorCode, "unable to update file record", "dbs.files.UpdateFiles")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return Error(err, UpdateFileErrorCode, "unable to commit update file record", "dbs.files.UpdateFiles")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
