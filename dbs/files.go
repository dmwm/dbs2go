package dbs

// nolint: gocyclo

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

// Files DBS API
// gocyclo:ignore
func (a *API) Files() error {
	var args []interface{}
	var conds, lumis []string
	var lumigen, rungen, lfngen, runList, lfnList bool
	var sumOverLumi string
	var err error

	if len(a.Params) == 0 {
		msg := "Files API with empty parameter map"
		return errors.New(msg)
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
			return err
		}
	}

	lumiList := getValues(a.Params, "lumi_list")
	lumis, err = FlatLumis(lumiList)
	if err != nil {
		return err
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
		return err
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
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", a.Params, conds, args)
	}

	// files API does not support run_num=1 when no lumi and lfns
	if len(runs) == 1 && len(lumis) == 0 && runs[0] == "1" && len(lfns) == 0 {
		msg := "files API does not support run_num=1 when no lumi and lfns list provided"
		return errors.New(msg)
	}

	// add run conditions
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
		for _, v := range binds {
			args = append(args, v)
		}
		tmpl["LumiGenerator"] = token
	} else if len(lumis) == 1 {
		conds, args = AddParam("lumi_list", "FL.LUMI_SECTION_NUM", a.Params, conds, args)
	}

	if (rungen && lfngen) || (lumigen && lfngen) || (rungen && lumigen) {
		msg := "cannot supply more than one list (lfn, run_num or lumi) at one query"
		return errors.New(msg)
	}

	// check sumOverLumi conditions
	if sumOverLumi == "1" && runList {
		msg := "When sumOverLumi=1, no run_num list is allowed"
		return errors.New(msg)
	}
	if sumOverLumi == "1" && lfnList {
		msg := "When sumOverLumi=1, no lfn list list is allowed"
		return errors.New(msg)
	}
	if len(runs) > 0 && sumOverLumi == "1" {
		stm = strings.Replace(stm, "F.EVENT_COUNT,", "", -1)
		stm = WhereClause(stm, conds)
		tmpl["Statement"] = stm
		stm, err = LoadTemplateSQL("files_sumoverlumi", tmpl)
		if err != nil {
			return err
		}
	} else {
		stm = WhereClause(stm, conds)
	}

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
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
	EVENT_COUNT            int64   `json:"event_count" validate:"required,number"`
	BRANCH_HASH_ID         int64   `json:"branch_hash_id"`
	ADLER32                string  `json:"adler32" validate:"required"`
	MD5                    string  `json:"md5"`
	AUTO_CROSS_SECTION     float64 `json:"auto_cross_section"`
	CREATION_DATE          int64   `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY              string  `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64   `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string  `json:"last_modified_by" validate:"required"`
}

// Insert implementation of Files
func (r *Files) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.FILE_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "FILES", "file_id")
			r.FILE_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FL")
			r.FILE_ID = tid
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
	stm := getSQL("insert_files")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Files\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.FILE_ID, r.LOGICAL_FILE_NAME, r.IS_FILE_VALID, r.DATASET_ID, r.BLOCK_ID, r.FILE_TYPE_ID, r.CHECK_SUM, r.FILE_SIZE, r.EVENT_COUNT, r.BRANCH_HASH_ID, r.ADLER32, r.MD5, r.AUTO_CROSS_SECTION, r.CREATION_DATE, r.CREATE_BY, r.LAST_MODIFICATION_DATE, r.LAST_MODIFIED_BY)
	return err
}

// Validate implementation of Files
func (r *Files) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("logical_file_name", r.LOGICAL_FILE_NAME); err != nil {
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

// RunLumi represents run lumi record
type RunLumi struct {
	RunNumber    int64 `json:"run_num"`
	LumitSection int64 `json:"lumi_section_num"`
}

// type FileParent struct {
//     FileParentLfn string `json:"file_parent_lfn"`
// }

// FileRecord represent input recor for insert blocks API
type FileRecord struct {
	LOGICAL_FILE_NAME      string  `json:"logical_file_name"`
	IS_FILE_VALID          int64   `json:"is_file_valid"`
	DATASET                string  `json:"dataset"`
	BLOCK                  string  `json:"block"`
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

	FILE_LUMI_LIST          []RunLumi            `json:"file_lumi_list"`
	FILE_PARENT_LIST        []FileParent         `json:"file_parent_list"`
	FILE_OUTPUT_CONFIG_LIST []OutputConfigRecord `json:"file_output_config"`
}

// InsertFiles DBS API
func (a *API) InsertFiles() error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSFile.py
	/*
	        :param qInserts: True means that inserts will be queued instead of done immediately. INSERT QUEUE
	 Manager will perform the inserts, within few minutes.
	        :type qInserts: bool
	        :param logical_file_name (required) : string
	        :param is_file_valid: (optional, default = 1): 1/0
	        :param block, required: /a/b/c#d
	        :param dataset, required: /a/b/c
	        :param file_type (optional, default = EDM): one of the predefined types,
	        :param check_sum (optional): string
	        :param event_count (optional, default = -1): int
	        :param file_size (optional, default = -1.): float
	        :param adler32 (optional): string
	        :param md5 (optional): string
	        :param auto_cross_section (optional, default = -1.): float
	        :param file_lumi_list (optional, default = []): [{'run_num': 123, 'lumi_section_num': 12},{}....]
	        :param file_parent_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....]
	        :param file_assoc_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....]
	        :param file_output_config_list(optional, default = []) :
	        [{'app_name':..., 'release_version':..., 'pset_hash':...., output_module_label':...},{}.....]
	*/
	// logic:
	// dataset_id = self.datasetid.execute(conn, dataset=f["dataset"])
	// dsconfigs = [x['output_mod_config_id'] for x in self.dsconfigids.execute(conn, dataset=f["dataset"])]
	// block_info = self.blocklist.execute(conn, block_name=f["block_name"])
	// file_type_id = self.ftypeid.execute( conn, f.get("file_type", "EDM"))
	// self.filein.execute(conn, filein, transaction=tran)
	// fcdao["output_mod_config_id"] = self.outconfigid.execute(conn, fc["app_name"],
	// self.flumiin.execute(conn, flumis2insert, transaction=tran)
	// self.fparentin.execute(conn, fparents2insert, transaction=tran)
	// self.fconfigin.execute(conn, fconfigs2insert, transaction=tran)
	// self.blkparentin.execute(conn, bkParentage2insert, transaction=tran)
	// self.dsparentin.execute(conn, dsParentage2insert, transaction=tran)
	// blkParams = self.blkstats.execute(conn, block_id, transaction=tran)
	// self.blkstatsin.execute(conn, blkParams, transaction=tran)

	//     return InsertValues("insert_files", values)

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := FileRecord{CREATE_BY: a.CreateBy, LAST_MODIFIED_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	// set dependent's records
	frec := Files{LOGICAL_FILE_NAME: rec.LOGICAL_FILE_NAME, IS_FILE_VALID: rec.IS_FILE_VALID, CHECK_SUM: rec.CHECK_SUM, FILE_SIZE: rec.FILE_SIZE, EVENT_COUNT: rec.EVENT_COUNT, ADLER32: rec.ADLER32, MD5: rec.MD5, AUTO_CROSS_SECTION: rec.AUTO_CROSS_SECTION, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY, LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE, LAST_MODIFIED_BY: rec.LAST_MODIFIED_BY}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// get all necessary IDs from different tables
	blkId, err := GetID(tx, "BLOCKS", "block_id", "block_name", rec.BLOCK)
	if err != nil {
		log.Println("unable to find block_id for", rec.BLOCK)
		return err
	}
	dsId, err := GetID(tx, "DATASETS", "dataset_id", "dataset", rec.DATASET)
	if err != nil {
		log.Println("unable to find dataset_id for", rec.DATASET)
		return err
	}
	ftId, err := GetID(tx, "FILE_DATA_TYPES", "file_type_id", "file_type", rec.FILE_TYPE)
	if err != nil {
		log.Println("unable to find file_type_id for", rec.FILE_TYPE)
		return err
	}

	// assign all Id's in dataset DB record
	frec.DATASET_ID = dsId
	frec.BLOCK_ID = blkId
	frec.FILE_TYPE_ID = ftId
	err = frec.Insert(tx)
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

// UpdateFiles DBS API
func (a *API) UpdateFiles() error {

	// read input parameters
	var createBy string
	var isFileValid int
	if v, ok := a.Params["is_file_valid"]; ok {
		val, err := strconv.Atoi(v.(string))
		if err != nil {
			log.Println("invalid input parameter", err)
		}
		isFileValid = val
	}
	if v, ok := a.Params["create_by"]; ok {
		createBy = v.(string)
	}
	date := time.Now().Unix()

	// validate input parameters
	if createBy == "" {
		return errors.New("invalid create_by parameter")
	}
	if isFileValid < 0 || isFileValid > 1 {
		return errors.New("invalid is_file_valid parameter")
	}

	// get SQL statement from static area
	stm := getSQL("update_files")
	if utils.VERBOSE > 0 {
		log.Printf("update Files\n%s\n%+v", stm)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(stm, createBy, date, isFileValid)
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
