package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/dmwm/dbs2go/utils"
)

// Datasets API
//
//gocyclo:ignore
func (a *API) Datasets() error {
	if utils.VERBOSE > 1 {
		log.Printf("datasets params %+v", a.Params)
	}
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["TokenGenerator"] = ""
	tmpl["Runs"] = false
	tmpl["Lfns"] = false
	tmpl["Version"] = false
	tmpl["ParentDataset"] = false
	tmpl["Detail"] = false

	// run_num should come first since it may produce TokenGenerator
	// whose bind parameters should appear first
	runs, err := ParseRuns(getValues(a.Params, "run_num"))
	if err != nil {
		return Error(err, InvalidParameterErrorCode, "unable to get run_num values", "dbs.datasets.Datasets")
	}
	if len(runs) > 0 {
		tmpl["Runs"] = true
		token, whereRuns, bindsRuns := runsClause("FLLU", runs)
		tmpl["TokenGenerator"] = token
		conds = append(conds, whereRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
	}

	// parse detail argument
	detail, _ := getSingleValue(a.Params, "detail")
	if detail == "1" { // for backward compatibility with Python detail=1 and detail=True
		detail = "true"
		tmpl["Detail"] = true
	}
	if strings.ToLower(detail) == "true" {
		tmpl["Detail"] = true
	}

	// parse dataset argument
	datasets := getValues(a.Params, "dataset")
	if len(datasets) > 1 {
		cond := fmt.Sprintf("D.DATASET in %s", TokenCondition())
		// 100 is max for # of allowed datasets
		token, binds := TokenGenerator(datasets, 100, "dataset_token")
		tmpl["TokenGenerator"] = token
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(datasets) == 1 {
		conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)
	}

	// parse dataset_id argument
	datasetAccessType, _ := getSingleValue(a.Params, "dataset_access_type")
	oper := "="
	if datasetAccessType == "" {
		datasetAccessType = "VALID"
	} else if datasetAccessType == "*" {
		datasetAccessType = "%"
		oper = "like"
	}
	cond := fmt.Sprintf("DP.DATASET_ACCESS_TYPE %s %s", oper, placeholder("dataset_access_type"))
	conds = append(conds, cond)
	args = append(args, datasetAccessType)
	//     }

	// parse is_dataset_valid argument
	isValid, _ := getSingleValue(a.Params, "is_dataset_valid")
	if isValid != "" {
		cond = fmt.Sprintf("D.IS_DATASET_VALID = %s", placeholder("is_dataset_valid"))
		conds = append(conds, cond)
		args = append(args, isValid)
	}

	// optional arguments
	if _, e := getSingleValue(a.Params, "parent_dataset"); e == nil {
		tmpl["ParentDataset"] = true
		conds, args = AddParam("parent_dataset", "PDS.DATASET", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "release_version"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("release_version", "RV.RELEASE_VERSION", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "pset_hash"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("pset_hash", "PSH.PSET_HASH", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "app_name"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("app_name", "AEX.APP_NAME", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "output_module_label"); e == nil {
		tmpl["Version"] = true
		conds, args = AddParam("output_module_label", "OMC.OUTPUT_MODULE_LABEL", a.Params, conds, args)
	}
	if _, e := getSingleValue(a.Params, "logical_file_name"); e == nil {
		tmpl["Lfns"] = true
		conds, args = AddParam("logical_file_name", "FL.LOGICAL_FILE_NAME", a.Params, conds, args)
	}
	conds, args = AddParam("primary_ds_name", "P.PRIMARY_DS_NAME", a.Params, conds, args)
	conds, args = AddParam("processed_ds_name", "PD.PROCESSED_DS_NAME", a.Params, conds, args)
	conds, args = AddParam("data_tier_name", "DT.DATA_TIER_NAME", a.Params, conds, args)
	conds, args = AddParam("primary_ds_type", "PDT.PRIMARY_DS_TYPE", a.Params, conds, args)
	conds, args = AddParam("physics_group_name", "PH.PHYSICS_GROUP_NAME", a.Params, conds, args)
	conds, args = AddParam("global_tag", "OMC.GLOBAL_TAG", a.Params, conds, args)
	conds, args = AddParam("processing_version", "PE.PROCESSING_VERSION", a.Params, conds, args)
	conds, args = AddParam("acqusition_era", "AE.ACQUISITION_ERA_NAME", a.Params, conds, args)
	conds, args = AddParam("cdate", "D.CREATION_DATE", a.Params, conds, args)
	minDate := getValues(a.Params, "min_cdate")
	maxDate := getValues(a.Params, "max_cdate")
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(
				" D.CREATION_DATE BETWEEN %s and %s",
				placeholder("min_cdate"),
				placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE > %s", placeholder("min_cdate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE < %s", placeholder("max_cdate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}
	conds, args = AddParam("ldate", "D.LAST_MODIFICATION_DATE", a.Params, conds, args)
	minDate = getValues(a.Params, "min_ldate")
	maxDate = getValues(a.Params, "max_ldate")
	if len(minDate) == 1 || len(maxDate) == 1 {
		var minval, maxval string
		if len(minDate) == 1 {
			_, minval = OperatorValue(minDate[0])
		}
		if len(maxDate) == 1 {
			_, maxval = OperatorValue(maxDate[0])
		}
		if minval != "" && maxval != "" {
			cond := fmt.Sprintf(
				" D.LAST_MODIFICATION_DATE BETWEEN %s and %s",
				placeholder("min_ldate"),
				placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "" {
			cond := fmt.Sprintf(" D.LAST_MODIFICATION_DATE > %s", placeholder("min_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if maxval != "" {
			cond := fmt.Sprintf(" D.LAST_MODIFICATION_DATE < %s", placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, maxval)
		}
	}
	conds, args = AddParam("create_by", "D.CREATE_BY", a.Params, conds, args)
	conds, args = AddParam("last_modified_by", "D.LAST_MODIFIED_BY", a.Params, conds, args)
	conds, args = AddParam("prep_id", "D.PREP_ID", a.Params, conds, args)

	dids := getValues(a.Params, "dataset_id")
	if len(dids) == 1 {
		if !strings.Contains(dids[0], "[") {
			cond := fmt.Sprintf("D.DATASET_ID = %s", placeholder("dataset_id"))
			conds = append(conds, cond)
			args = append(args, dids[0])
		} else {
			vals := strings.Replace(dids[0], "[", "", -1)
			vals = strings.Replace(vals, "]", "", -1)
			var arr []string
			for _, v := range strings.Split(vals, " ") {
				arr = append(arr, utils.ConvertFloat(v))
			}
			token, binds := TokenGenerator(arr, 100, "dataset_id_token")
			tmpl["TokenGenerator"] = token
			cond := fmt.Sprintf(" D.DATASET_ID in %s", TokenCondition())
			conds = append(conds, cond)
			for _, v := range binds {
				args = append(args, v)
			}
		}
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("datasets", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "uname to load datasets template", "dbs.datasets.Datasets")
	}
	cols := []string{
		"dataset_id",
		"dataset",
		"prep_id",
		"xtcrosssection",
		"creation_date",
		"create_by",
		"last_modification_date",
		"last_modified_by",
		"primary_ds_name",
		"primary_ds_type",
		"processed_ds_name",
		"data_tier_name",
		"dataset_access_type",
		"acquisition_era_name",
		"processing_version",
		"physics_group_name"}
	vals := []interface{}{
		new(sql.NullInt64),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullFloat64),
		new(sql.NullInt64),
		new(sql.NullString),
		new(sql.NullInt64),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullString),
		new(sql.NullInt64),
		new(sql.NullString)}
	if tmpl["Version"].(bool) {
		cols = append(
			cols,
			"output_module_label",
			"global_tag",
			"release_version",
			"pset_hash",
			"app_name")
		vals = append(
			vals,
			new(sql.NullString),
			new(sql.NullString),
			new(sql.NullString),
			new(sql.NullString),
			new(sql.NullString))
	}
	if strings.ToLower(detail) != "true" {
		//         stm = getSQL("datasets_short")
		cols = []string{"dataset"}
		vals = []interface{}{new(sql.NullString)}
	}
	if tmpl["ParentDataset"].(bool) {
		cols = append(cols, "parent_dataset")
		vals = append(vals, new(sql.NullString))
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = execute(a.Writer, a.Separator, stm, cols, vals, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query DATASETs table", "dbs.datasets.Datasets")
	}
	return nil
}

// Datasets represents Datasets DBS DB table
type Datasets struct {
	DATASET_ID             int64   `json:"dataset_id"`
	DATASET                string  `json:"dataset" validate:"required"`
	IS_DATASET_VALID       int     `json:"is_dataset_valid" validate:"required,number"`
	PRIMARY_DS_ID          int64   `json:"primary_ds_id" validate:"required,number,gt=0"`
	PROCESSED_DS_ID        int64   `json:"processed_ds_id" validate:"required,number,gt=0"`
	DATA_TIER_ID           int64   `json:"data_tier_id" validate:"required,number,gt=0"`
	DATASET_ACCESS_TYPE_ID int64   `json:"dataset_access_type_id" validate:"required,number,gt=0"`
	ACQUISITION_ERA_ID     int64   `json:"acquisition_era_id" validate:"required,number,gt=0"`
	PROCESSING_ERA_ID      int64   `json:"processing_era_id" validate:"required,number,gt=0"`
	PHYSICS_GROUP_ID       int64   `json:"physics_group_id" validate:"required,number,gt=0"`
	XTCROSSSECTION         float64 `json:"xtcrosssection" validate:"required"`
	PREP_ID                string  `json:"prep_id"`
	CREATION_DATE          int64   `json:"creation_date" validate:"required,number"`
	CREATE_BY              string  `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64   `json:"last_modification_date" validate:"required,number"`
	LAST_MODIFIED_BY       string  `json:"last_modified_by" validate:"required"`
}

// Insert implementation of Datasets
func (r *Datasets) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DATASET_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "DATASETS", "dataset_id")
			r.DATASET_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DS")
			r.DATASET_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "unable to increment datasets sequence number", "dbs.datasets.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "unable to validate dataset record", "dbs.datasets.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Datasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.DATASET_ID,
		r.DATASET,
		r.IS_DATASET_VALID,
		r.PRIMARY_DS_ID,
		r.PROCESSED_DS_ID,
		r.DATA_TIER_ID,
		r.DATASET_ACCESS_TYPE_ID,
		r.ACQUISITION_ERA_ID,
		r.PROCESSING_ERA_ID,
		r.PHYSICS_GROUP_ID,
		r.XTCROSSSECTION,
		r.PREP_ID,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to insert Datasets %+v", err)
		}
		return Error(err, InsertDatasetErrorCode, fmt.Sprintf("unable to insert dataset %s", r.DATASET), "dbs.datasets.Insert")
	}
	return nil
}

// Validate implementation of Datasets
//
//gocyclo:ignore
func (r *Datasets) Validate() error {
	if err := CheckPattern("dataset", r.DATASET); err != nil {
		return Error(err, InvalidParameterErrorCode, "wrong dataset name", "dbs.datasets.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.IS_DATASET_VALID != 0 {
		if r.IS_DATASET_VALID != 1 {
			msg := "wrong is_dataset_valid value"
			return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
		}
	}
	if r.CREATION_DATE == 0 {
		msg := "missing creation_date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.CREATE_BY == "" {
		msg := "missing create_by"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		msg := "missing last_modification_date"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.LAST_MODIFIED_BY == "" {
		msg := "missing last_modified_by"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.PRIMARY_DS_ID == 0 {
		msg := "incorrect primary_ds_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.PROCESSED_DS_ID == 0 {
		msg := "incorrect processed_ds_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.DATA_TIER_ID == 0 {
		msg := "incorrect data_tier_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.DATASET_ACCESS_TYPE_ID == 0 {
		msg := "incorrect dataset_access_type_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.ACQUISITION_ERA_ID == 0 {
		msg := "incorrect acquisition_era_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.PROCESSING_ERA_ID == 0 {
		msg := "incorrect processing_era_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.PHYSICS_GROUP_ID == 0 {
		msg := "incorrect physics_group_id"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Datasets
func (r *Datasets) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		r.LAST_MODIFICATION_DATE = Date()
	}
}

// Decode implementation for Datasets
func (r *Datasets) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read dataset record", "dbs.datasets.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode dataset record", "dbs.datasets.Decode")
	}
	return nil
}

// DatasetRecord we receive for InsertDatasets API
type DatasetRecord struct {
	DATASET         string `json:"dataset" validate:"required"`
	PRIMARY_DS_NAME string `json:"primary_ds_name" validate:"required"`
	//     PRIMARY_DS_TYPE        string  `json:"primary_ds_type" validate:"required"`
	PROCESSED_DS_NAME      string               `json:"processed_ds_name" validate:"required"`
	DATA_TIER_NAME         string               `json:"data_tier_name" validate:"required"`
	ACQUISITION_ERA_NAME   string               `json:"acquisition_era_name" validate:"required"`
	DATASET_ACCESS_TYPE    string               `json:"dataset_access_type" validate:"required"`
	PROCESSING_VERSION     int64                `json:"processing_version" validate:"required,number,gt=0"`
	PHYSICS_GROUP_NAME     string               `json:"physics_group_name" validate:"required"`
	XTCROSSSECTION         float64              `json:"xtcrosssection" validate:"required,number"`
	CREATION_DATE          int64                `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY              string               `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64                `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string               `json:"last_modified_by" validate:"required"`
	OUTPUT_CONFIGS         []OutputConfigRecord `json:"output_configs"`
}

// InsertDatasets DBS API implements the following logic:
//
// - take given input and insert
// - primary dataset info
// - acquisition era info
// - physics group info
// - processing era info
// - output module config info
// - insert dataset info
//
//gocyclo:ignore
func (a *API) InsertDatasets() error {
	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "unable to read dataset record", "dbs.datasets.InsertDatasets")
	}
	rec := DatasetRecord{CREATE_BY: a.CreateBy, LAST_MODIFIED_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "unable to decode dataset record", "dbs.datasets.InsertDatasets")
	}
	// set dependent's records
	dsrec := Datasets{
		DATASET:                rec.DATASET,
		XTCROSSSECTION:         rec.XTCROSSSECTION,
		CREATION_DATE:          rec.CREATION_DATE,
		CREATE_BY:              rec.CREATE_BY,
		LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE,
		LAST_MODIFIED_BY:       rec.LAST_MODIFIED_BY,
		IS_DATASET_VALID:       1}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := "unable to get DB transaction"
		return Error(err, TransactionErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	defer tx.Rollback()

	// check if our data already exist in DB
	if IfExist(tx, "DATASETS", "dataset_id", "dataset", rec.DATASET) {
		if a.Writer != nil {
			a.Writer.Write([]byte(`[]`))
		}
		return nil
	}

	// get all necessary IDs from different tables
	primId, err := GetID(
		tx,
		"PRIMARY_DATASETS",
		"primary_ds_id",
		"primary_ds_name",
		rec.PRIMARY_DS_NAME)
	if err != nil {
		msg := fmt.Sprintf("unable to find primary_ds_id for", rec.PRIMARY_DS_NAME)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, GetPrimaryDSIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	//     primType, err := GetID(tx, "PRIMARY_DS_TYPE", "primary_ds_type_id", "primary_ds_type", rec.PRIMARY_DS_TYPE)
	//     if err != nil {
	//         log.Println("unable to find primary_ds_type_id for", rec.PRIMARY_DS_TYPE)
	//         return Error(err, GetPrimaryDatasetTypeIDError, "unable to get primary dataset type record", "dbs.datasets.InsertDatasets")
	//     }
	procId, err := GetID(
		tx,
		"PROCESSED_DATASETS",
		"processed_ds_id",
		"processed_ds_name",
		rec.PROCESSED_DS_NAME)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to find processed_ds_id for", rec.PROCESSED_DS_NAME)
		}
		prec := ProcessedDatasets{PROCESSED_DS_NAME: rec.PROCESSED_DS_NAME}
		err := prec.Insert(tx)
		if err != nil {
			msg := fmt.Sprintf("unable to insert processed dataset %s", rec.PROCESSED_DS_NAME)
			return Error(err, InsertPrimaryDatasetErrorCode, msg, "dbs.datasets.InsertDatasets")
		}
		procId, err = GetID(tx,
			"PROCESSED_DATASETS",
			"processed_ds_id",
			"processed_ds_name",
			rec.PROCESSED_DS_NAME)
		if err != nil {
			msg := fmt.Sprintf("unable to find processed dataset %s", rec.PROCESSED_DS_NAME)
			return Error(err, GetProcessedDatasetIDErrorCode, msg, "dbs.datasets.InsertDatasets")
		}
	}
	tierId, err := GetID(
		tx,
		"DATA_TIERS",
		"data_tier_id",
		"data_tier_name",
		rec.DATA_TIER_NAME)
	if err != nil {
		msg := fmt.Sprintf("unable to find data_tier id for %s", rec.DATA_TIER_NAME)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, GetDataTierIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	daccId, err := GetID(
		tx,
		"DATASET_ACCESS_TYPES",
		"dataset_access_type_id",
		"dataset_access_type",
		rec.DATASET_ACCESS_TYPE)
	if err != nil {
		msg := fmt.Sprintf("unable to find dataset_access_type_id for %s", rec.DATASET_ACCESS_TYPE)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, GetDatasetAccessTypeIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	aeraId, err := GetID(
		tx,
		"ACQUISITION_ERAS",
		"acquisition_era_id",
		"acquisition_era_name",
		rec.ACQUISITION_ERA_NAME)
	if err != nil {
		msg := fmt.Sprintf("unable to find acquisition_era_id for %s", rec.ACQUISITION_ERA_NAME)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, GetAcquisitionEraIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	peraId, err := GetID(
		tx,
		"PROCESSING_ERAS",
		"processing_era_id",
		"processing_version",
		rec.PROCESSING_VERSION)
	if err != nil {
		msg := fmt.Sprintf("unable to find processing_era_id for %s", rec.PROCESSING_VERSION)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, GetProcessingEraIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	pgrpId, err := GetID(
		tx,
		"PHYSICS_GROUPS",
		"physics_group_id",
		"physics_group_name",
		rec.PHYSICS_GROUP_NAME)
	if err != nil {
		msg := fmt.Sprintf("unable to find physics_group_id for %s", rec.PHYSICS_GROUP_NAME)
		if utils.VERBOSE > 0 {
			log.Println(msg)
		}
		return Error(err, GetPhysicsGroupIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}

	// assign all Id's in dataset DB record
	dsrec.PRIMARY_DS_ID = primId
	//     dsrec.PRIMARY_DS_TYPE = primType
	dsrec.PROCESSED_DS_ID = procId
	dsrec.DATA_TIER_ID = tierId
	dsrec.DATASET_ACCESS_TYPE_ID = daccId
	dsrec.ACQUISITION_ERA_ID = aeraId
	dsrec.PROCESSING_ERA_ID = peraId
	dsrec.PHYSICS_GROUP_ID = pgrpId
	err = dsrec.Insert(tx)
	if err != nil {
		msg := fmt.Sprintf("unable to insert dataset record %v", dsrec)
		return Error(err, InsertDatasetErrorCode, msg, "dbs.datasets.InsertDatasets")
	}

	// get current dataset id
	dsid, err := GetID(tx, "DATASETS", "dataset_id", "dataset", dsrec.DATASET)
	if err != nil {
		msg := fmt.Sprintf("unable to find dataset id for %s", dsrec.DATASET)
		return Error(err, GetDatasetIDErrorCode, msg, "dbs.datasets.InsertDatasets")
	}
	// match output_mod_config
	for _, oc := range rec.OUTPUT_CONFIGS {
		ocid, err := GetID(tx, "OUTPUT_MODULE_CONFIGS", "output_mod_config_id", "output_module_label", oc.OUTPUT_MODULE_LABEL)
		if err != nil {
			msg := fmt.Sprintf("unable to get output_module_config for %s", oc.OUTPUT_MODULE_LABEL)
			return Error(err, GetOutputModConfigIDErrorCode, msg, "dbs.datasets.InsertDatasets")
		}
		r := DatasetOutputModConfigs{OUTPUT_MOD_CONFIG_ID: ocid, DATASET_ID: dsid}
		err = r.Insert(tx)
		if err != nil {
			msg := fmt.Sprintf("unable to insert output_mod_config %v", r)
			return Error(err, InsertDatasetOutputModConfigErrorCode, msg, "dbs.datasets.InsertDatasets")
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return Error(err, InsertDatasetErrorCode, "unable to commit dataset insert record", "dbs.datasets.InsertDatasets")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// Validate POST/PUT Parameters
func ValidateParameter(params Record, key string) (string, error) {
	var value string
	value, err := getSingleValue(params, key)
	if err != nil {
		msg := fmt.Sprintf("unable to validate %s", key)
		return "", Error(err, ParseErrorCode, msg, "dbs.datasets.UpdateDatasets")
	}
	if value == "" {
		msg := fmt.Sprintf("invalid %s parameter", key)
		return "", Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.datasets.UpdateDatasets")
	}
	if err := CheckPattern(key, value); err != nil {
		msg := fmt.Sprintf("%s parameter pattern invalid", key)
		return "", Error(err, InvalidParameterErrorCode, msg, "dbs.datasets.UpdateDatasets")
	}
	return value, nil
}

// UpdateDatasets DBS API
//
//gocyclo:ignore
func (a *API) UpdateDatasets() error {

	var args []interface{}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["PhysicsGroup"] = false
	tmpl["DatasetAccessType"] = false

	// validate parameteres
	var createBy string
	if _, ok := a.Params["create_by"]; ok {
		v, err := ValidateParameter(a.Params, "create_by")
		if err != nil {
			return Error(err, ValidateErrorCode, "unable to validate create_by", "dbs.datasets.UpdateDatasets")
		}
		createBy = v
	}

	date := time.Now().Unix()

	var isValidDataset int64
	var dataset string
	var datasetAccessType string
	var physicsGroupName string
	// validate dataset_access_type parameter
	if _, ok := a.Params["dataset_access_type"]; ok {
		tmpl["DatasetAccessType"] = true
		v, err := ValidateParameter(a.Params, "dataset_access_type")
		if err != nil {
			return Error(err, ValidateErrorCode, "unable to validate dataset access type", "dbs.datasets.UpdateDatasets")
		}
		datasetAccessType = v
	}

	// validate physics_group_name parameter
	// ^[a-zA-Z0-9/][a-zA-Z0-9\\-_']*$
	if _, ok := a.Params["physics_group_name"]; ok {
		tmpl["PhysicsGroup"] = true
		v, err := ValidateParameter(a.Params, "physics_group_name")
		if err != nil {
			return Error(err, ValidateErrorCode, "unable to validate physics group name", "dbs.datasets.UpdateDatasets")
		}
		physicsGroupName = v
	}

	// validate dataset parameter
	if _, ok := a.Params["dataset"]; ok {
		v, err := ValidateParameter(a.Params, "dataset")
		if err != nil {
			return Error(err, ValidateErrorCode, "unable to validate dataset name", "dbs.datasets.UpdateDatasets")
		}
		dataset = v
		if datasetAccessType == "VALID" {
			isValidDataset = 1
		}
	}

	// get SQL statement from static area
	// stm := getSQL("update_datasets")
	stm, err := LoadTemplateSQL("update_datasets", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "unable to load update dataset template", "dbs.datasets.UpdateDatasets")
	}
	if utils.VERBOSE > 0 {
		params := []string{dataset, datasetAccessType}
		log.Printf("update Datasets\n%s\n%+v", stm, params)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return Error(err, TransactionErrorCode, "transaction error", "dbs.datasets.UpdateDatasets")
	}
	defer tx.Rollback()

	args = append(args, createBy)
	args = append(args, date)

	var physicsGroupID int64
	if tmpl["PhysicsGroup"].(bool) {
		physicsGroupID, err = GetID(
			tx,
			"PHYSICS_GROUPS",
			"physics_group_id",
			"physics_group_name",
			physicsGroupName)
		if err != nil {
			msg := fmt.Sprintf("unable to find physics_group_id for %s", physicsGroupName)
			if utils.VERBOSE > 0 {
				log.Println(msg)
			}
			return Error(err, GetPhysicsGroupIDErrorCode, msg, "dbs.datasets.UpdateDatasets")
		}
		args = append(args, physicsGroupID)
	}

	// get accessTypeID from Access dataset types table
	if tmpl["DatasetAccessType"].(bool) {
		accessTypeID, err := GetID(
			tx,
			"DATASET_ACCESS_TYPES",
			"dataset_access_type_id",
			"dataset_access_type",
			datasetAccessType)
		if err != nil {
			msg := fmt.Sprintf("unable to find dataset_access_type_id for %s", datasetAccessType)
			if utils.VERBOSE > 0 {
				log.Println(msg)
			}
			return Error(err, GetDatasetAccessTypeIDErrorCode, msg, "dbs.datasets.UpdateDatasets")
		}
		args = append(args, accessTypeID)
		args = append(args, isValidDataset)
	}

	args = append(args, dataset)

	// perform update
	// _, err = tx.Exec(stm, createBy, date, accessTypeID, isValidDataset, physicsGroupID, dataset)
	_, err = tx.Exec(stm, args...)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to update %v", err)
		}
		return Error(err, UpdateDatasetErrorCode, "unable to update dataset record", "dbs.datasets.UpdateDatasets")
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return Error(err, UpdateDatasetErrorCode, "unable to commit update dataset record", "dbs.datasets.UpdateDatasets")
	}
	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
