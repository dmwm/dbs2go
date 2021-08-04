package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/vkuznet/dbs2go/utils"
)

// Datasets API
func (a API) Datasets() error {
	log.Printf("datasets params %+v", a.Params)
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

	// run_num shouhld come first since it may produce TokenGenerator
	// whose bind parameters should appear first
	runs, err := ParseRuns(getValues(a.Params, "run_num"))
	if err != nil {
		return err
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

	// parse detail arugment
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
		token, binds := TokenGenerator(datasets, 100, "dataset_token") // 100 is max for # of allowed datasets
		conds = append(conds, cond+token)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(datasets) == 1 {
		conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)
	}

	// parse is_dataset_valid argument
	isValid, _ := getSingleValue(a.Params, "is_dataset_valid")
	if isValid == "" {
		isValid = "1"
	}
	cond := fmt.Sprintf("D.IS_DATASET_VALID = %s", placeholder("is_dataset_valid"))
	conds = append(conds, cond)
	args = append(args, isValid)

	// parse dataset_id argument
	dataset_access_type, _ := getSingleValue(a.Params, "dataset_access_type")
	//     if dataset_access_type != "" {
	oper := "="
	if dataset_access_type == "" {
		dataset_access_type = "VALID"
	} else if dataset_access_type == "*" {
		dataset_access_type = "%"
		oper = "like"
	}
	cond = fmt.Sprintf("DP.DATASET_ACCESS_TYPE %s %s", oper, placeholder("dataset_access_type"))
	conds = append(conds, cond)
	args = append(args, dataset_access_type)
	//     }

	// optional arguments
	if _, e := getSingleValue(a.Params, "parent_dataset"); e == nil {
		tmpl["ParentDataset"] = true
		conds, args = AddParam("parent_dataset", "PDS.DATASET PARENT_DATASET", a.Params, conds, args)
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
			cond := fmt.Sprintf(" D.CREATION_DATE BETWEEN %s and %s", placeholder("min_cdate"), placeholder("max_cdate"))
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
	if len(minDate) == 1 && len(maxDate) == 1 {
		_, minval := OperatorValue(minDate[0])
		_, maxval := OperatorValue(maxDate[0])
		if minval != "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE BETWEEN %s and %s", placeholder("min_ldate"), placeholder("max_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
			args = append(args, maxval)
		} else if minval != "0" && maxval == "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE > %s", placeholder("min_ldate"))
			conds = append(conds, cond)
			args = append(args, minval)
		} else if minval == "0" && maxval != "0" {
			cond := fmt.Sprintf(" D.CREATION_DATE < %s", placeholder("max_ldate"))
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
		return err
	}
	cols := []string{"dataset_id", "dataset", "prep_id", "xtcrosssection", "creation_date", "create_by", "last_modification_date", "last_modified_by", "primary_ds_name", "primary_ds_type", "processed_ds_name", "data_tier_name", "dataset_access_type", "acquisition_era_name", "processing_version", "physics_group_name"}
	vals := []interface{}{new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullFloat64), new(sql.NullInt64), new(sql.NullString), new(sql.NullInt64), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
	if strings.ToLower(detail) != "true" {
		//         stm = getSQL("datasets_short")
		cols = []string{"dataset"}
		vals = []interface{}{new(sql.NullString)}
	}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return execute(a.Writer, a.Separator, stm, cols, vals, args...)
}

// Datasets
type Datasets struct {
	DATASET_ID             int64   `json:"dataset_id"`
	DATASET                string  `json:"datset" validate:"required"`
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
	stm := getSQL("insert_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Datasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.DATASET_ID, r.DATASET, r.IS_DATASET_VALID, r.PRIMARY_DS_ID, r.PROCESSED_DS_ID, r.DATA_TIER_ID, r.DATASET_ACCESS_TYPE_ID, r.ACQUISITION_ERA_ID, r.PROCESSING_ERA_ID, r.PHYSICS_GROUP_ID, r.XTCROSSSECTION, r.PREP_ID, r.CREATION_DATE, r.CREATE_BY, r.LAST_MODIFICATION_DATE, r.LAST_MODIFIED_BY)
	return err
}

// Validate implementation of Datasets
func (r *Datasets) Validate() error {
	if err := CheckPattern("dataset", r.DATASET); err != nil {
		return err
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		return errors.New("invalid pattern for createion date")
	}
	if r.IS_DATASET_VALID != 0 {
		if r.IS_DATASET_VALID != 1 {
			return errors.New("wrong is_dataset_valid value")
		}
	}
	if r.CREATION_DATE == 0 {
		return errors.New("missing creation_date")
	}
	if r.CREATE_BY == "" {
		return errors.New("missing create_by")
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		return errors.New("missing last_modification_date")
	}
	if r.LAST_MODIFIED_BY == "" {
		return errors.New("missing last_modified_by")
	}
	if r.PRIMARY_DS_ID == 0 {
		return errors.New("incorrect primary_ds_id")
	}
	if r.PROCESSED_DS_ID == 0 {
		return errors.New("incorrect processed_ds_id")
	}
	if r.DATA_TIER_ID == 0 {
		return errors.New("incorrect data_tier_id")
	}
	if r.DATASET_ACCESS_TYPE_ID == 0 {
		return errors.New("incorrect dataset_access_type_id")
	}
	if r.ACQUISITION_ERA_ID == 0 {
		return errors.New("incorrect acquisition_era_id")
	}
	if r.PROCESSING_ERA_ID == 0 {
		return errors.New("incorrect processing_era_id")
	}
	if r.PHYSICS_GROUP_ID == 0 {
		return errors.New("incorrect physics_group_id")
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

// DatasetRecord we receive for InsertDatasets API
type DatasetRecord struct {
	DATASET                string  `json:"dataset"`
	PRIMARY_DS_NAME        string  `json:"primary_ds_name"`
	PROCESSED_DS           string  `json:"processed_ds"`
	DATA_TIER              string  `json:"data_tier"`
	ACQUISITION_ERA        string  `json:"acquisition_era"`
	DATASET_ACCESS_TYPE    string  `json:"dataset_access_type"`
	PROCESSING_VERSION     int64   `json:"processing_version"`
	PHYSICS_GROUP          string  `json:"physics_group"`
	XTCROSSSECTION         float64 `json:"xtcrosssection"`
	CREATION_DATE          int64   `json:"creation_date"`
	CREATE_BY              string  `json:"create_by"`
	LAST_MODIFICATION_DATE int64   `json:"last_modification_date"`
	LAST_MODIFIED_BY       string  `json:"last_modified_by"`
}

// InsertDatasets DBS API
func (a API) InsertDatasets() error {
	// implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSDataset.py
	// input values: dataset, primary_ds_name(name), processed_ds(name), data_tier(name),
	// acquisition_era(name), processing_version
	// optional:
	// physics_group(name), xtcrosssection, creation_date, create_by, last_modification_date, last_modified_by
	// dsdaoinput["dataset_id"] = self.sm.increment(conn, "SEQ_DS")
	// logic:
	// dsdaoinput["physics_group_id"] = self.phygrpid.execute(conn, businput["physics_group_name"])
	// dsdaoinput["processing_era_id"] = self.proceraid.execute(conn, businput["processing_version"])
	// dsdaoinput["acquisition_era_id"] = self.acqeraid.execute(conn, businput["acquisition_era_name"])
	// self.datasetin.execute(conn, dsdaoinput, tran)
	// dsoutconfdaoin["output_mod_config_id"] = self.outconfigid.execute ...
	// self.datasetoutmodconfigin.execute(conn, dsoutconfdaoin, tran

	//     args := make(Record)
	//     args["Owner"] = DBOWNER
	//     return InsertTemplateValues("insert_datasets", args, values)

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	rec := DatasetRecord{CREATE_BY: a.CreateBy, LAST_MODIFIED_BY: a.CreateBy}
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	// set dependent's records
	dsrec := Datasets{DATASET: rec.DATASET, XTCROSSSECTION: rec.XTCROSSSECTION, CREATION_DATE: rec.CREATION_DATE, CREATE_BY: rec.CREATE_BY, LAST_MODIFICATION_DATE: rec.LAST_MODIFICATION_DATE, LAST_MODIFIED_BY: rec.LAST_MODIFIED_BY, IS_DATASET_VALID: 1}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	// get all necessary IDs from different tables
	primId, err := GetID(tx, "PRIMARY_DATASETS", "primary_ds_id", "primary_ds_name", rec.PRIMARY_DS_NAME)
	if err != nil {
		log.Println("unable to find primary_ds_id for", rec.PRIMARY_DS_NAME)
		return err
	}
	procId, err := GetID(tx, "PROCESSED_DATASETS", "processed_ds_id", "processed_ds_name", rec.PROCESSED_DS)
	if err != nil {
		log.Println("unable to find processed_ds_id for", rec.PROCESSED_DS)
		return err
	}
	tierId, err := GetID(tx, "DATA_TIERS", "data_tier_id", "data_tier_name", rec.DATA_TIER)
	if err != nil {
		log.Println("unable to find data_tier_id for", rec.DATA_TIER)
		return err
	}
	daccId, err := GetID(tx, "DATASET_ACCESS_TYPES", "dataset_access_type_id", "dataset_access_type", rec.DATASET_ACCESS_TYPE)
	if err != nil {
		log.Println("unable to find dataset_access_type_id for", rec.DATASET_ACCESS_TYPE)
		return err
	}
	aeraId, err := GetID(tx, "ACQUISITION_ERAS", "acquisition_era_id", "acquisition_era_name", rec.ACQUISITION_ERA)
	if err != nil {
		log.Println("unable to find acquisition_era_id for", rec.ACQUISITION_ERA)
		return err
	}
	peraId, err := GetID(tx, "PROCESSING_ERAS", "processing_era_id", "processing_version", rec.PROCESSING_VERSION)
	if err != nil {
		log.Println("unable to find processing_era_id for", rec.PROCESSING_VERSION)
		return err
	}
	pgrpId, err := GetID(tx, "PHYSICS_GROUPS", "physics_group_id", "physics_group_name", rec.PHYSICS_GROUP)
	if err != nil {
		log.Println("unable to find physics_group_id for", rec.PHYSICS_GROUP)
		return err
	}

	// assign all Id's in dataset DB record
	dsrec.PRIMARY_DS_ID = primId
	dsrec.PROCESSED_DS_ID = procId
	dsrec.DATA_TIER_ID = tierId
	dsrec.DATASET_ACCESS_TYPE_ID = daccId
	dsrec.ACQUISITION_ERA_ID = aeraId
	dsrec.PROCESSING_ERA_ID = peraId
	dsrec.PHYSICS_GROUP_ID = pgrpId
	err = dsrec.Insert(tx)
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

// UpdateDatasets DBS API
func (a API) UpdateDatasets() error {

	// get accessTypeID from Access dataset types table
	var create_by string
	if v, ok := a.Params["create_by"]; ok {
		create_by = v.(string)
	}
	var dataset string
	var datasetAccessType string
	if v, ok := a.Params["dataset"]; ok {
		dataset = v.(string)
	}
	if v, ok := a.Params["dataset_access_type"]; ok {
		datasetAccessType = v.(string)
	}
	date := time.Now().Unix()

	// validate input parameters
	if dataset == "" {
		return errors.New("invalid dataset parameter")
	}
	if create_by == "" {
		return errors.New("invalid create_by parameter")
	}
	if datasetAccessType == "" {
		return errors.New("invalid datasetAccessType parameter")
	}

	// get SQL statement from static area
	stm := getSQL("update_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("update Datasets\n%s\n%+v", stm)
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return err
	}
	defer tx.Rollback()
	accessTypeID, err := GetID(tx, "DATASET_ACCESS_TYPES", "dataset_access_type_id", "dataset_access_type", datasetAccessType)
	if err != nil {
		log.Println("unable to find dataset_access_type_id for", datasetAccessType)
		return err
	}
	_, err = tx.Exec(stm, create_by, date, accessTypeID, dataset)
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
