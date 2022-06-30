package dbs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/dmwm/dbs2go/utils"
)

// BulkBlocks represents bulk block structure used by `/bulkblocks` DBS API
type BulkBlocks struct {
	DatasetConfigList []DatasetConfig    `json:"dataset_conf_list"`
	FileConfigList    []FileConfig       `json:"file_conf_list"`
	Files             []File             `json:"files"`
	ProcessingEra     ProcessingEra      `json:"processing_era"`
	PrimaryDataset    PrimaryDataset     `json:"primds"`
	Dataset           Dataset            `json:"dataset"`
	AcquisitionEra    AcquisitionEra     `json:"acquisition_era"`
	Block             Block              `json:"block"`
	FileParentList    []FileParentRecord `json:"file_parent_list"`
	BlockParentList   []BlockParent      `json:"block_parent_list"`
	DatasetParentList []string           `json:"dataset_parent_list"` // used by bulkblocks API
	DsParentList      []DatasetParent    `json:"ds_parent_list"`      // provided by bulkdump API
}

// DatasetConfig represents dataset config structure used in BulkBlocks structure
type DatasetConfig struct {
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	PsetName          string `json:"pset_name"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
	GlobalTag         string `json:"global_tag"`
	CreateBy          string `json:"create_by"`
	CreationDate      int64  `json:"creation_date"`
}

// FileConfig represents file config structure used in BulkBlocks structure
type FileConfig struct {
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	PsetName          string `json:"pset_name"`
	LFN               string `json:"lfn"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
	GlobalTag         string `json:"global_tag"`
	CreateBy          string `json:"create_by"`
	CreationDate      int64  `json:"creation_date"`
}

// FileLumi represents file lumi structure used in File structure of BulkBlocks structure
type FileLumi struct {
	LumiSectionNumber int64 `json:"lumi_section_num"`
	RunNumber         int64 `json:"run_num"`
	EventCount        int64 `json:"event_count"`
}

// File represents file structure used in BulkBlocks structure
type File struct {
	CheckSum             string     `json:"check_sum"`
	FileLumiList         []FileLumi `json:"file_lumi_list"`
	Adler32              string     `json:"adler32"`
	FileSize             int64      `json:"file_size"`
	EventCount           int64      `json:"event_count"`
	FileType             string     `json:"file_type"`
	LastModifiedBy       string     `json:"last_modified_by"`
	LastModificationDate int64      `json:"last_modification_date"`
	LogicalFileName      string     `json:"logical_file_name"`
	MD5                  string     `json:"md5"`
	AutoCrossSection     float64    `json:"auto_cross_section"`
	IsFileValid          int64      `json:"is_file_valid"`
}

// ProcessingEra represents processing era structure used in BulkBlocks structure
type ProcessingEra struct {
	CreateBy          string `json:"create_by"`
	CreationDate      int64  `json:"creation_date"`
	ProcessingVersion int64  `json:"processing_version"`
	Description       string `json:"description"`
}

// PrimaryDataset represents primary dataset structure used in BulkBlocks structure
type PrimaryDataset struct {
	PrimaryDSId   int64  `json:"primary_ds_id"`
	CreateBy      string `json:"create_by"`
	PrimaryDSType string `json:"primary_ds_type"`
	PrimaryDSName string `json:"primary_ds_name"`
	CreationDate  int64  `json:"creation_date"`
}

// Dataset represents dataset structure used in BulkBlocks structure
type Dataset struct {
	DatasetID            int64   `json:"dataset_id"`
	CreateBy             string  `json:"create_by"`
	CreationDate         int64   `json:"creation_date"`
	PhysicsGroupName     string  `json:"physics_group_name"`
	DatasetAccessType    string  `json:"dataset_access_type"`
	DataTierName         string  `json:"data_tier_name"`
	LastModifiedBy       string  `json:"last_modified_by"`
	ProcessedDSName      string  `json:"processed_ds_name"`
	Xtcrosssection       float64 `json:"xtcrosssection"`
	LastModificationDate int64   `json:"last_modification_date"`
	Dataset              string  `json:"dataset"`
	PrepID               string  `json:"prep_id"`
}

// AcquisitionEra represents AcquisitionEra structure use in BulkBlocks structure
type AcquisitionEra struct {
	AcquisitionEraName string `json:"acquisition_era_name"`
	StartDate          int64  `json:"start_date"`
	CreationDate       int64  `json:"creation_date"`
	EndDate            int64  `json:"end_date"`
	CreateBy           string `json:"create_by"`
	Description        string `json:"description"`
}

// Block represents Block structure used in BulkBlocks structure
type Block struct {
	BlockID              int64  `json:"block_id"`
	DatasetID            int64  `json:"dataset_id"`
	CreateBy             string `json:"create_by"`
	CreationDate         int64  `json:"creation_date"`
	OpenForWriting       int64  `json:"open_for_writing"`
	BlockName            string `json:"block_name"`
	FileCount            int64  `json:"file_count"`
	OriginSiteName       string `json:"origin_site_name"`
	BlockSize            int64  `json:"block_size"`
	LastModifiedBy       string `json:"last_modified_by"`
	LastModificationDate int64  `json:"last_modification_date"`
}

// BlockParent represents block parent structure used in BulkBlocks structure
type BlockParent struct {
	//     ThisBlockID     string `json:"this_block_id"`
	ParentBlock     string `json:"parent_block"`
	ParentBlockName string `json:"parent_block_name"`
	ThisBlockName   string `json:"this_block_name"`
}

// DatasetParent represents dataset parent structure used in BulkBlocks structure
type DatasetParent struct {
	ThisDatasetID string `json:"this_dataset_id"`
	ParentDataset string `json:"parent_dataset"`
}

// InsertBulkBlocks DBS API. It relies on BulkBlocks record which by itself
// contains series of other records. The logic of this API is the following:
// we read dataset_conf_list part of the record and insert output config data,
// then we insert recursively PrimaryDSTypes, PrimaryDataset, ProcessingEras,
// AcquisitionEras, ..., Datasets, Blocks, Files, FileLumis, FileCofig list,
// and dataset parent lists.
//gocyclo:ignore
func (a *API) InsertBulkBlocks() error {
	// read input data
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("unable to read bulkblock input", err)
		return Error(err, ReaderErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}
	// get our request hash ID to be able to trace concurrent requests
	hash := utils.GetHash(data)

	// unmarshal the data into BulkBlocks record
	var rec BulkBlocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("unable to unmarshal bulkblock record %s, error %v", string(data), err)
		return Error(err, UnmarshalErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}

	// prepare file parentage map, i.e. find out file ids we need for FileParentList
	parentFilesMap := make(map[string]int64)
	for _, r := range rec.FileParentList {
		// parent lfn should be already in DB
		plfn := r.ParentLogicalFileName
		pfid, err := QueryRow("FILES", "file_id", "logical_file_name", plfn)
		if err != nil {
			msg := fmt.Sprintf("unable to find parent lfn %s", plfn)
			return Error(err, DatabaseErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		parentFilesMap[plfn] = pfid
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}
	defer tx.Rollback()

	var reader *bytes.Reader
	api := &API{
		Reader:   reader,
		CreateBy: a.CreateBy,
		Params:   make(Record),
	}
	var isFileValid, datasetID, blockID, fileID, fileTypeID int64
	var primaryDatasetTypeID, primaryDatasetID, acquisitionEraID, processingEraID int64
	var dataTierID, physicsGroupID, processedDatasetID, datasetAccessTypeID int64
	creationDate := time.Now().Unix()

	// check if is_file_valid was present in request, if not set it to 1
	if !strings.Contains(string(data), "is_file_valid") {
		isFileValid = 1
	}
	// insert dataset configuration
	if utils.VERBOSE > 1 {
		log.Println("insert output configs")
	}
	for _, rrr := range rec.DatasetConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			log.Println("unable to marshal dataset config list", err)
			return Error(err, MarshalErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		api.Reader = bytes.NewReader(data)
		err = api.InsertOutputConfigsTx(tx)
		if err != nil {
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}

	// get primaryDatasetTypeID and insert record if it does not exists
	if utils.VERBOSE > 1 {
		log.Println("get primary dataset type ID")
	}
	pdstDS := PrimaryDSTypes{
		PRIMARY_DS_TYPE: rec.PrimaryDataset.PrimaryDSType,
	}
	primaryDatasetTypeID, err = GetRecID(
		tx,
		&pdstDS,
		"PRIMARY_DS_TYPES",
		"primary_ds_type_id",
		"primary_ds_type",
		rec.PrimaryDataset.PrimaryDSType,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find primary_ds_type_id for", rec.PrimaryDataset.PrimaryDSType)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}

	// get primarayDatasetID and insert record if it does not exists
	if utils.VERBOSE > 1 {
		log.Println("get primary dataset ID")
	}
	if rec.PrimaryDataset.CreateBy == "" {
		rec.PrimaryDataset.CreateBy = a.CreateBy
	}
	primDS := PrimaryDatasets{
		PRIMARY_DS_NAME:    rec.PrimaryDataset.PrimaryDSName,
		PRIMARY_DS_TYPE_ID: primaryDatasetTypeID,
		CREATION_DATE:      rec.PrimaryDataset.CreationDate,
		CREATE_BY:          rec.PrimaryDataset.CreateBy,
	}
	primaryDatasetID, err = GetRecID(
		tx,
		&primDS,
		"PRIMARY_DATASETS",
		"primary_ds_id",
		"primary_ds_name",
		rec.PrimaryDataset.PrimaryDSName,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find primary_ds_id for", rec.PrimaryDataset.PrimaryDSName)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}

	// get processing era ID and insert record if it does not exists
	if utils.VERBOSE > 1 {
		log.Println("get processing era ID")
	}
	if rec.ProcessingEra.CreateBy == "" {
		rec.ProcessingEra.CreateBy = a.CreateBy
	}
	pera := ProcessingEras{
		PROCESSING_VERSION: rec.ProcessingEra.ProcessingVersion,
		CREATION_DATE:      creationDate,
		CREATE_BY:          rec.ProcessingEra.CreateBy,
		DESCRIPTION:        rec.ProcessingEra.Description,
	}
	processingEraID, err = GetRecID(
		tx,
		&pera,
		"PROCESSING_ERAS",
		"processing_era_id",
		"processing_version",
		rec.ProcessingEra.ProcessingVersion,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find processing_era_id for", rec.ProcessingEra.ProcessingVersion)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}

	// insert acquisition era if it does not exists
	if utils.VERBOSE > 1 {
		log.Println("get acquisition era ID")
	}
	if rec.AcquisitionEra.CreateBy == "" {
		rec.AcquisitionEra.CreateBy = a.CreateBy
	}
	endDate := rec.AcquisitionEra.EndDate
	if endDate == 0 {
		endDate = time.Now().Unix() // TODO: figure out logic of endDate
	}
	aera := AcquisitionEras{
		ACQUISITION_ERA_NAME: rec.AcquisitionEra.AcquisitionEraName,
		START_DATE:           rec.AcquisitionEra.StartDate,
		END_DATE:             endDate,
		CREATION_DATE:        creationDate,
		CREATE_BY:            rec.AcquisitionEra.CreateBy,
		DESCRIPTION:          rec.AcquisitionEra.Description,
	}
	acquisitionEraID, err = GetRecID(
		tx,
		&aera,
		"ACQUISITION_ERAS",
		"acquisition_era_id",
		"acquisition_era_name",
		rec.AcquisitionEra.AcquisitionEraName,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find acquisition_era_id for", rec.AcquisitionEra.AcquisitionEraName)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}

	// get dataTierID
	if utils.VERBOSE > 1 {
		log.Println("get data tier ID")
	}
	tier := DataTiers{
		DATA_TIER_NAME: rec.Dataset.DataTierName,
		CREATION_DATE:  creationDate,
		CREATE_BY:      a.CreateBy,
	}
	dataTierID, err = GetRecID(
		tx,
		&tier,
		"DATA_TIERS",
		"data_tier_id",
		"data_tier_name",
		rec.Dataset.DataTierName,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find data_tier_id for", rec.Dataset.DataTierName)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}
	// get physicsGroupID
	if utils.VERBOSE > 1 {
		log.Println("get physics group ID")
	}
	pgrp := PhysicsGroups{
		PHYSICS_GROUP_NAME: rec.Dataset.PhysicsGroupName,
	}
	physicsGroupID, err = GetRecID(
		tx,
		&pgrp,
		"PHYSICS_GROUPS",
		"physics_group_id",
		"physics_group_name",
		rec.Dataset.PhysicsGroupName,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find physics_group_id for", rec.Dataset.PhysicsGroupName)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}
	// get datasetAccessTypeID
	if utils.VERBOSE > 1 {
		log.Println("get dataset access type ID")
	}
	dat := DatasetAccessTypes{
		DATASET_ACCESS_TYPE: rec.Dataset.DatasetAccessType,
	}
	datasetAccessTypeID, err = GetRecID(
		tx,
		&dat,
		"DATASET_ACCESS_TYPES",
		"dataset_access_type_id",
		"dataset_access_type",
		rec.Dataset.DatasetAccessType,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find dataset_access_type_id for", rec.Dataset.DatasetAccessType)
		}
		return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}
	if utils.VERBOSE > 1 {
		log.Println("get processed dataset ID")
	}
	procDS := ProcessedDatasets{
		PROCESSED_DS_NAME: rec.Dataset.ProcessedDSName,
	}
	processedDatasetID, err = GetRecID(
		tx,
		&procDS,
		"PROCESSED_DATASETS",
		"processed_ds_id",
		"processed_ds_name",
		rec.Dataset.ProcessedDSName,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find processed_ds_id for", rec.Dataset.ProcessedDSName)
		}
		err := procDS.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert processed dataset name record", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		processedDatasetID, err = GetID(
			tx,
			"PROCESSED_DATASETS",
			"processed_ds_id",
			"processed_ds_name",
			rec.Dataset.ProcessedDSName,
		)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("unable to find processed_ds_id %s error %v", rec.Dataset.ProcessedDSName, err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}

	// insert dataset
	if utils.VERBOSE > 1 {
		log.Println("insert dataset")
	}
	if rec.Dataset.CreateBy == "" {
		rec.Dataset.CreateBy = a.CreateBy
	}
	dataset := Datasets{
		DATASET:                rec.Dataset.Dataset,
		IS_DATASET_VALID:       1,
		PRIMARY_DS_ID:          primaryDatasetID,
		PROCESSED_DS_ID:        processedDatasetID,
		DATA_TIER_ID:           dataTierID,
		DATASET_ACCESS_TYPE_ID: datasetAccessTypeID,
		ACQUISITION_ERA_ID:     acquisitionEraID,
		PROCESSING_ERA_ID:      processingEraID,
		PHYSICS_GROUP_ID:       physicsGroupID,
		XTCROSSSECTION:         rec.Dataset.Xtcrosssection,
		PREP_ID:                rec.Dataset.PrepID,
		CREATION_DATE:          rec.Dataset.CreationDate,
		CREATE_BY:              rec.Dataset.CreateBy,
		LAST_MODIFICATION_DATE: creationDate,
		LAST_MODIFIED_BY:       rec.Dataset.CreateBy,
	}
	// get datasetID
	if utils.VERBOSE > 1 {
		log.Println("get dataset ID")
	}
	datasetID, err = GetID(tx, "DATASETS", "dataset_id", "dataset", rec.Dataset.Dataset)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find dataset_id for", rec.Dataset.Dataset, "will insert")
		}
		err = dataset.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert dataset record", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		datasetID, err = GetID(tx, "DATASETS", "dataset_id", "dataset", rec.Dataset.Dataset)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("unable to get dataset_id for dataset %s error %v", rec.Dataset.Dataset, err)
			}
			return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}
	// get outputModConfigID using datasetID
	// since we already inserted records from DatasetConfigList
	for _, r := range rec.DatasetConfigList {
		var vals []interface{}
		vals = append(vals, r.AppName)
		vals = append(vals, r.PsetHash)
		vals = append(vals, r.ReleaseVersion)
		vals = append(vals, r.OutputModuleLabel)
		vals = append(vals, r.GlobalTag)
		stm := getSQL("datasetoutmodconfigs")
		var oid float64
		err := tx.QueryRow(stm, vals...).Scan(&oid)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("fail to get id for %s, %v, error %v", stm, vals, err)
			}
		}
		// insert into DATASET_OUTPUT_MOD_CONFIGS
		dsoRec := DatasetOutputModConfigs{
			DATASET_ID:           datasetID,
			OUTPUT_MOD_CONFIG_ID: int64(oid),
		}
		err = dsoRec.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert dataset output mod configs record", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}

	// insert block
	if utils.VERBOSE > 1 {
		log.Println("insert block")
	}
	if rec.Block.CreateBy == "" {
		rec.Block.CreateBy = a.CreateBy
	}
	blk := Blocks{
		BLOCK_NAME:             rec.Block.BlockName,
		DATASET_ID:             datasetID,
		OPEN_FOR_WRITING:       rec.Block.OpenForWriting,
		ORIGIN_SITE_NAME:       rec.Block.OriginSiteName,
		BLOCK_SIZE:             rec.Block.BlockSize,
		FILE_COUNT:             rec.Block.FileCount,
		CREATION_DATE:          rec.Block.CreationDate,
		CREATE_BY:              rec.Block.CreateBy,
		LAST_MODIFICATION_DATE: rec.Block.CreationDate,
		LAST_MODIFIED_BY:       rec.Block.CreateBy,
	}
	// get blockID
	blockID, err = GetID(tx, "BLOCKS", "block_id", "block_name", rec.Block.BlockName)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to find block_id for", rec.Block.BlockName, "will insert")
		}
		err = blk.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert block record", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		blockID, err = GetID(tx, "BLOCKS", "block_id", "block_name", rec.Block.BlockName)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("unable to find block_id for %s, error %v", rec.Block.BlockName, err)
			}
			return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}

	// insert files
	if utils.VERBOSE > 1 {
		log.Println("insert files")
	}
	tempTable := fmt.Sprintf("ORA$PTT_TEMP_FILE_LUMIS_%d", time.Now().UnixMicro())
	if DBOWNER == "sqlite" {
		tempTable = "FILE_LUMIS"
	}
	filesMap := make(map[string]int64)
	for _, rrr := range rec.Files {
		// get fileTypeID and insert record if it does not exists
		ftype := FileDataTypes{FILE_TYPE: rrr.FileType}
		fileTypeID, err = GetRecID(
			tx,
			&ftype,
			"FILE_DATA_TYPES",
			"file_type_id",
			"file_type",
			rrr.FileType,
		)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to find file_type_id for", rrr.FileType)
			}
			return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		// get branch hash ID and insert record if it does not exists
		//         if rrr.BranchHash == "" {
		//             rrr.BranchHash = "branch-hash"
		//         }

		cBy := rrr.LastModifiedBy
		if cBy == "" {
			cBy = a.CreateBy
		}
		lBy := rrr.LastModifiedBy
		if lBy == "" {
			lBy = a.CreateBy
		}
		// if the data string does contain the is_file_valid field, use value from request
		if isFileValid == 0 {
			if rrr.IsFileValid != 0 && rrr.IsFileValid != 1 {
				msg := "wrong is_file_valid value"
				return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocks")
			}
			isFileValid = rrr.IsFileValid
		}
		r := Files{
			LOGICAL_FILE_NAME:      rrr.LogicalFileName,
			IS_FILE_VALID:          isFileValid,
			DATASET_ID:             datasetID,
			BLOCK_ID:               blockID,
			FILE_TYPE_ID:           fileTypeID,
			CHECK_SUM:              rrr.CheckSum,
			FILE_SIZE:              rrr.FileSize,
			EVENT_COUNT:            rrr.EventCount,
			ADLER32:                rrr.Adler32,
			MD5:                    rrr.MD5,
			AUTO_CROSS_SECTION:     rrr.AutoCrossSection,
			CREATION_DATE:          creationDate,
			CREATE_BY:              cBy,
			LAST_MODIFICATION_DATE: creationDate,
			LAST_MODIFIED_BY:       lBy,
		}
		// insert file lumi list
		fileID, err = GetID(tx, "FILES", "file_id", "logical_file_name", rrr.LogicalFileName)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to find file_id for", rrr.LogicalFileName, "will insert")
			}
			err = r.Insert(tx)
			if err != nil {
				if utils.VERBOSE > 1 {
					log.Println("unable to insert File record", err)
				}
				return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
			}
			fileID, err = GetID(tx, "FILES", "file_id", "logical_file_name", rrr.LogicalFileName)
			if err != nil {
				if utils.VERBOSE > 1 {
					log.Printf("unable to find block_id for %s, error %v", rec.Block.BlockName, err)
				}
				return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
			}
		}
		filesMap[rrr.LogicalFileName] = fileID

		err = api.SelectFileLumiListInsert(tx, rrr.FileLumiList, tempTable, fileID, "dbs.bulkblocks.InsertBulkBlocks")
		if err != nil {
			return err
		}
	}

	// insert file configuration
	for _, rrr := range rec.FileConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to marshal file config list", err)
			}
			return Error(err, MarshalErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		api.Reader = bytes.NewReader(data)
		err = api.InsertFileOutputModConfigs(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert file output mod config", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}

	// find out file ids we need for FileParentList
	for _, r := range rec.FileParentList {
		rrr := FileParents{}
		lfn := r.LogicalFileName
		if lfn == "" {
			lfn = r.ThisLogicalFileName
		}
		if lfn == "" {
			err := errors.New("mailformed file parent record")
			msg := fmt.Sprintf("file parent record %+v does not contain LFN", r)
			log.Println(msg)
			return Error(err, NotImplementedApiCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		if fileID, ok := filesMap[lfn]; ok {
			rrr.THIS_FILE_ID = fileID
		} else {
			err := errors.New("unable to locate LFN file id")
			msg := fmt.Sprintf("no file id found for '%s'", lfn)
			log.Println(msg)
			return Error(err, SessionErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		// parent lfn should be already in DB
		plfn := r.ParentLogicalFileName
		if pfid, ok := parentFilesMap[plfn]; ok {
			rrr.PARENT_FILE_ID = pfid
			//             log.Println("### parent_logical_file_name", plfn, pfid)
		} else {
			err := errors.New("unable to locate parent file id")
			msg := fmt.Sprintf("no file id found for parent '%s'", lfn)
			log.Println(msg)
			return Error(err, DatabaseErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		err := rrr.Insert(tx)
		if err != nil {
			msg := fmt.Sprintf("%s unable to insert file parents record %+v, error %v", hash, rrr, err)
			log.Println(msg)
			return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
	}

	/*
		// insert file parent list
		data, err = json.Marshal(rec.FileParentList)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to marshal file parent list", err)
			}
			return Error(err, MarshalErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		api.Reader = bytes.NewReader(data)
		api.Params = make(Record)
		err = api.InsertFileParentsTxt(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert file parents", err)
			}
			msg := fmt.Sprintf("failed record %+v", rec)
			return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocks")
		}
	*/

	// insert dataset parent list
	datasetParentList := rec.DatasetParentList
	// use both DatasetParentList and DsParentList (for backward compatibility)
	// and compose unique set of dataset parents
	for _, d := range rec.DsParentList {
		datasetParentList = append(datasetParentList, d.ParentDataset)
	}
	datasetParentList = utils.Set(datasetParentList)
	for _, ds := range datasetParentList {
		// get file id for parent dataset
		pid, err := GetID(tx, "DATASETS", "dataset_id", "dataset", ds)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to find dataset_id for", ds)
			}
			return Error(err, GetIDErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
		r := DatasetParents{THIS_DATASET_ID: datasetID, PARENT_DATASET_ID: pid}
		err = r.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert parent dataset record", err)
			}
			return Error(err, InsertErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("fail to commit transaction", err)
		}
		return Error(err, CommitErrorCode, "", "dbs.bulkblocks.InsertBulkBlocks")
	}

	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}
