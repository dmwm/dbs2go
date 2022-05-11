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

// FileChunkSize controls size of chunk for []File insertion
var FileChunkSize int

// FilesMap keeps track of lfn names and their file ids
// type FilesMap map[string]int64
// type FilesMap *sync.Map

// TempFileRecord contains all relevant attribute to insert File records
type TempFileRecord struct {
	IsFileValid  int64
	DatasetID    int64
	BlockID      int64
	CreationDate int64
	CreateBy     string
	FilesMap     sync.Map
	//     FilesMap     FilesMap
	NErrors int
}

// helper function to insert dataset configurations
func insertDatasetConfigurations(api *API, datasetConfigList DatasetConfigList, hash string) error {
	if utils.VERBOSE > 1 {
		log.Println(hash, "insert output configs")
	}
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.insertDatasetConfigurations")
	}
	defer tx.Rollback()
	for _, rrr := range datasetConfigList {
		data, err := json.Marshal(rrr)
		if err != nil {
			log.Println(hash, "unable to marshal dataset config list", err)
			return Error(err, MarshalErrorCode, hash, "dbs.bulkblocks.insertDatasetConfigurations")
		}
		api.Reader = bytes.NewReader(data)
		err = api.InsertOutputConfigsTx(tx)
		if err != nil {
			return Error(err, InsertErrorCode, hash, "dbs.bulkblocks.insertDatasetConfigurations")
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Println(hash, "fail to commit transaction", err)
		return Error(err, CommitErrorCode, hash, "dbs.bulkblocks.insertDatasetConfigurations")
	}
	return nil
}

// helper function to get primary dataset type ID
func getPrimaryDatasetTypeID(primaryDSType, hash string) (int64, error) {
	if utils.VERBOSE > 1 {
		log.Println(hash, "get primary dataset type ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getPrimaryDatasetTypeID")
	}
	defer tx.Rollback()
	pdstDS := PrimaryDSTypes{
		PRIMARY_DS_TYPE: primaryDSType,
	}
	primaryDatasetTypeID, err := GetRecID(
		tx,
		&pdstDS,
		"PRIMARY_DS_TYPES",
		"primary_ds_type_id",
		"primary_ds_type",
		primaryDSType,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find primary_ds_type_id for primary ds type='%s'", hash, primaryDSType)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getPrimaryDatasetTypeID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getPrimaryDatasetTypeID")
	}
	return primaryDatasetTypeID, nil
}

// helper function to get primary dataset id
func getPrimaryDatasetID(
	primaryDSName string,
	primaryDatasetTypeID, cDate int64,
	cBy, hash string) (int64, error) {
	if utils.VERBOSE > 1 {
		log.Println(hash, "get primary dataset ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getPrimaryDatasetTypeID")
	}
	defer tx.Rollback()
	primDS := PrimaryDatasets{
		PRIMARY_DS_NAME:    primaryDSName,
		PRIMARY_DS_TYPE_ID: primaryDatasetTypeID,
		CREATION_DATE:      cDate,
		CREATE_BY:          cBy,
	}
	primaryDatasetID, err := GetRecID(
		tx,
		&primDS,
		"PRIMARY_DATASETS",
		"primary_ds_id",
		"primary_ds_name",
		primaryDSName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find primary_ds_id for primary ds name='%s'", hash, primaryDSName)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getPrimaryDatasetID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getPrimaryDatasetID")
	}
	return primaryDatasetID, nil
}

// helper function to get processing Era ID
func getProcessingEraID(
	processingVersion, cDate int64,
	cBy, description, hash string) (int64, error) {
	if utils.VERBOSE > 1 {
		log.Println(hash, "get processing era ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getProcessingEraID")
	}
	defer tx.Rollback()
	pera := ProcessingEras{
		PROCESSING_VERSION: processingVersion,
		CREATION_DATE:      cDate,
		CREATE_BY:          cBy,
		DESCRIPTION:        description,
	}
	processingEraID, err := GetRecID(
		tx,
		&pera,
		"PROCESSING_ERAS",
		"processing_era_id",
		"processing_version",
		processingVersion,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find processing_era_id for processing version='%v'", hash, processingVersion)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getProcessingEraID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getProcessingEraID")
	}
	return processingEraID, nil
}

// helper function to get acquisition era ID
func getAcquisitionEraID(
	acquisitionEraName string,
	startDate, endDate, creationDate int64,
	cBy, description, hash string) (int64, error) {

	if utils.VERBOSE > 1 {
		log.Println(hash, "get acquisition era ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getAcquisitionEraID")
	}
	defer tx.Rollback()
	aera := AcquisitionEras{
		ACQUISITION_ERA_NAME: acquisitionEraName,
		START_DATE:           startDate,
		END_DATE:             endDate,
		CREATION_DATE:        creationDate,
		CREATE_BY:            cBy,
		DESCRIPTION:          description,
	}
	acquisitionEraID, err := GetRecID(
		tx,
		&aera,
		"ACQUISITION_ERAS",
		"acquisition_era_id",
		"acquisition_era_name",
		acquisitionEraName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find acquisition_era_id for acq era name='%s'", hash, acquisitionEraName)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getAcquisitionEraID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getAcquisitionEraID")
	}
	return acquisitionEraID, nil
}

// helper function to get data tier ID
func getDataTierID(
	tierName string,
	cDate int64,
	cBy, hash string) (int64, error) {

	if utils.VERBOSE > 1 {
		log.Println(hash, "get data tier ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getDataTierID")
	}
	defer tx.Rollback()
	tier := DataTiers{
		DATA_TIER_NAME: tierName,
		CREATION_DATE:  cDate,
		CREATE_BY:      cBy,
	}
	dataTierID, err := GetRecID(
		tx,
		&tier,
		"DATA_TIERS",
		"data_tier_id",
		"data_tier_name",
		tierName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find data_tier_id for tier name='%s'", hash, tierName)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getDataTierID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getDataTierID")
	}
	return dataTierID, nil
}

// helper function to get physics group ID
func getPhysicsGroupID(physName, hash string) (int64, error) {
	if utils.VERBOSE > 1 {
		log.Println(hash, "get physics group ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getPhysicsGroupID")
	}
	defer tx.Rollback()
	pgrp := PhysicsGroups{
		PHYSICS_GROUP_NAME: physName,
	}
	physicsGroupID, err := GetRecID(
		tx,
		&pgrp,
		"PHYSICS_GROUPS",
		"physics_group_id",
		"physics_group_name",
		physName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s, unable to find physics_group_id for physics group name='%s'", hash, physName)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getPhysicsGroupID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getPhysicsGroupID")
	}
	return physicsGroupID, nil
}

// helper function to get dataset access type ID
func getDatasetAccessTypeID(
	datasetAccessType, hash string) (int64, error) {

	if utils.VERBOSE > 1 {
		log.Println(hash, "get dataset access type ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getDatasetAccessTypeID")
	}
	defer tx.Rollback()
	dat := DatasetAccessTypes{
		DATASET_ACCESS_TYPE: datasetAccessType,
	}
	datasetAccessTypeID, err := GetRecID(
		tx,
		&dat,
		"DATASET_ACCESS_TYPES",
		"dataset_access_type_id",
		"dataset_access_type",
		datasetAccessType,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find dataset_access_type_id for data access type='%s'", hash, datasetAccessType)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getDatasetAccesssTypeID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getDatasetAccessTypeID")
	}
	return datasetAccessTypeID, nil
}

// helper function to get processed dataset ID
func getProcessedDatasetID(
	processedDSName, hash string) (int64, error) {

	if utils.VERBOSE > 1 {
		log.Println(hash, "get processed dataset ID")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getProcessedDatasetID")
	}
	defer tx.Rollback()
	procDS := ProcessedDatasets{
		PROCESSED_DS_NAME: processedDSName,
	}
	processedDatasetID, err := GetRecID(
		tx,
		&procDS,
		"PROCESSED_DATASETS",
		"processed_ds_id",
		"processed_ds_name",
		processedDSName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find processed_ds_id for procDS='%s'", hash, processedDSName)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getProcessedDSName")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getProcessedDatasetID")
	}
	return processedDatasetID, nil
}

// helper function to get dataset ID
func getDatasetID(
	datasetName string,
	isDatasetValid int,
	primaryDatasetID int64,
	processedDatasetID int64,
	dataTierID int64,
	datasetAccessTypeID int64,
	acquisitionEraID int64,
	processingEraID int64,
	physicsGroupID int64,
	xtcrosssection float64,
	prepId string,
	creationDate int64,
	createBy string,
	lastModificationDate int64,
	lBy string,
	hash string,
) (int64, error) {

	if utils.VERBOSE > 1 {
		log.Println(hash, "insert dataset")
	}
	tx, err := DB.Begin()
	if err != nil {
		return 0, Error(err, TransactionErrorCode, hash, "dbs.bulkblocks.getDatasetID")
	}
	defer tx.Rollback()
	dataset := Datasets{
		DATASET:                datasetName,
		IS_DATASET_VALID:       isDatasetValid,
		PRIMARY_DS_ID:          primaryDatasetID,
		PROCESSED_DS_ID:        processedDatasetID,
		DATA_TIER_ID:           dataTierID,
		DATASET_ACCESS_TYPE_ID: datasetAccessTypeID,
		ACQUISITION_ERA_ID:     acquisitionEraID,
		PROCESSING_ERA_ID:      processingEraID,
		PHYSICS_GROUP_ID:       physicsGroupID,
		XTCROSSSECTION:         xtcrosssection,
		PREP_ID:                prepId,
		CREATION_DATE:          creationDate,
		CREATE_BY:              createBy,
		LAST_MODIFICATION_DATE: lastModificationDate,
		LAST_MODIFIED_BY:       lBy,
	}
	// get datasetID
	if utils.VERBOSE > 1 {
		log.Printf("get dataset ID for %+v", dataset)
	}
	datasetID, err := GetRecID(
		tx,
		&dataset,
		"DATASETS",
		"dataset_id",
		"dataset",
		datasetName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to insert dataset='%v'", hash, dataset)
		log.Println(msg)
		return 0, Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.getDatasetID")
	}
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return 0, Error(err, CommitErrorCode, msg, "dbs.bulkblocks.getDatasetID")
	}
	return datasetID, nil
}

// InsertBulkBlocksConcurrently DBS API provides concurrent bulk blocks
// insertion. It inherits the same logic as BulkBlocks API but perform
// Files and FileLumis injection concurrently via chunk of record.
// It relies on the following parameters:
//
// - FileChunkSize defines number of concurrent goroutines executing injection into
// FILES table
// - FileLumiChunkSize/FileLumiMaxSize defines concurrent injection into
// FILE_LUMIS table. The former specifies chunk size while latter total number of
// records to be inserted at once to ORABLE DB
// - FileLumiInsertMethod defines which method to use for workflow execution, so far
// we support temptable, chunks, and sequential methods. The temptable uses
// ORACLE TEMPTABLE approach, chunks uses direct tables, and sequential method
// fallback to record by record injection (no goroutines).
//
//gocyclo:ignore
func (a *API) InsertBulkBlocksConcurrently() error {
	// read input data
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("unable to read bulkblock input", err)
		return Error(err, ReaderErrorCode, "", "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}
	// get our request hash ID to be able to trace concurrent requests
	hash := utils.GetHash(data)

	if utils.VERBOSE > 1 {
		log.Println(hash, "start bulkblocks.InsertBulkBlocksConcurrently")
	}

	// unmarshal the data into BulkBlocks record
	var rec BulkBlocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("unable to unmarshal bulkblock record %s, error %v", string(data), err)
		return Error(err, UnmarshalErrorCode, "", "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}

	var reader *bytes.Reader
	api := &API{
		Reader:   reader,
		CreateBy: a.CreateBy,
		Params:   make(Record),
	}
	var isFileValid, datasetID, blockID int64
	var primaryDatasetTypeID, primaryDatasetID, acquisitionEraID, processingEraID int64
	var dataTierID, physicsGroupID, processedDatasetID, datasetAccessTypeID int64
	creationDate := time.Now().Unix()

	// check if is_file_valid was present in request, if not set it to 1
	if !strings.Contains(string(data), "is_file_valid") {
		isFileValid = 1
	}

	// insert dataset configuration
	if err = insertDatasetConfigurations(api, rec.DatasetConfigList, hash); err != nil {
		return err
	}

	// get primaryDatasetTypeID and insert record if it does not exists
	if primaryDatasetTypeID, err = getPrimaryDatasetTypeID(rec.PrimaryDataset.PrimaryDSType, hash); err != nil {
		return err
	}

	// get primarayDatasetID and insert record if it does not exists
	if rec.PrimaryDataset.CreateBy == "" {
		rec.PrimaryDataset.CreateBy = a.CreateBy
	}
	if primaryDatasetID, err = getPrimaryDatasetID(
		rec.PrimaryDataset.PrimaryDSName,
		primaryDatasetTypeID,
		rec.PrimaryDataset.CreationDate,
		rec.PrimaryDataset.CreateBy, hash); err != nil {
		return err
	}

	// get processing era ID and insert record if it does not exists
	if rec.ProcessingEra.CreateBy == "" {
		rec.ProcessingEra.CreateBy = a.CreateBy
	}
	if processingEraID, err = getProcessingEraID(
		rec.ProcessingEra.ProcessingVersion,
		creationDate,
		rec.ProcessingEra.CreateBy,
		rec.ProcessingEra.Description, hash); err != nil {
		return err
	}

	// insert acquisition era if it does not exists
	if rec.AcquisitionEra.CreateBy == "" {
		rec.AcquisitionEra.CreateBy = a.CreateBy
	}
	if acquisitionEraID, err = getAcquisitionEraID(
		rec.AcquisitionEra.AcquisitionEraName,
		rec.AcquisitionEra.StartDate,
		0,
		creationDate,
		rec.AcquisitionEra.CreateBy,
		rec.AcquisitionEra.Description, hash); err != nil {
		return err
	}

	// get dataTierID
	if dataTierID, err = getDataTierID(
		rec.Dataset.DataTierName, creationDate, a.CreateBy, hash); err != nil {
		return err
	}

	// get physicsGroupID
	if physicsGroupID, err = getPhysicsGroupID(
		rec.Dataset.PhysicsGroupName, hash); err != nil {
		return err
	}

	// get datasetAccessTypeID
	if datasetAccessTypeID, err = getDatasetAccessTypeID(
		rec.Dataset.DatasetAccessType, hash); err != nil {
		return err
	}

	// get processedDatasetID
	if processedDatasetID, err = getProcessedDatasetID(
		rec.Dataset.ProcessedDSName, hash); err != nil {
		return err
	}

	// get datasetID and insert dataset if necessary
	if rec.Dataset.CreateBy == "" {
		rec.Dataset.CreateBy = a.CreateBy
	}
	if datasetID, err = getDatasetID(
		rec.Dataset.Dataset,
		1,
		primaryDatasetID,
		processedDatasetID,
		dataTierID,
		datasetAccessTypeID,
		acquisitionEraID,
		processingEraID,
		physicsGroupID,
		rec.Dataset.Xtcrosssection,
		rec.Dataset.PrepID,
		rec.Dataset.CreationDate,
		rec.Dataset.CreateBy,
		creationDate,
		rec.Dataset.CreateBy,
		hash); err != nil {
		return err
	}

	// start transaction for the rest of the injection process
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "", "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}
	defer tx.Rollback()

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
			msg := fmt.Sprintf("%s unable to insert dataset output mod configs record, error %v", hash, err)
			log.Println(msg)
			return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
	}

	// insert block
	if utils.VERBOSE > 1 {
		log.Println(hash, "insert block")
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
	blockID, err = GetRecID(
		tx,
		&blk,
		"BLOCKS",
		"block_id",
		"block_name",
		rec.Block.BlockName,
	)
	if err != nil {
		msg := fmt.Sprintf("%s unable to find block_id for %s, error %v", hash, rec.Block.BlockName, err)
		log.Println(msg)
		return Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}

	// insert all FileDataTypes fow all lfns
	for _, rrr := range rec.Files {
		ftype := FileDataTypes{FILE_TYPE: rrr.FileType}
		//         err = ftype.Insert(tx)
		_, err = GetRecID(
			tx,
			&ftype,
			"FILE_DATA_TYPES",
			"file_type_id",
			"file_type",
			rrr.FileType,
		)
		if err != nil {
			msg := fmt.Sprintf("%s unable to find file_type_id for %s, error %v", hash, ftype, err)
			log.Println(msg)
			return Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
	}
	// insert files
	if utils.VERBOSE > 1 {
		log.Println(hash, "insert files")
	}
	trec := TempFileRecord{
		IsFileValid:  isFileValid,
		DatasetID:    datasetID,
		BlockID:      blockID,
		CreationDate: creationDate,
		CreateBy:     a.CreateBy,
		FilesMap:     sync.Map{},
		NErrors:      0,
	}
	err = insertFilesViaChunks(tx, rec.Files, &trec)
	if err != nil {
		msg := fmt.Sprintf("%s unable to insert files, error %v", hash, err)
		log.Println(msg)
		return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}
	if utils.VERBOSE > 1 {
		log.Printf("trec %+v", trec)
	}
	tempTable := fmt.Sprintf("ORA$PTT_TEMP_FILE_LUMIS_%d", time.Now().UnixMicro())
	if DBOWNER == "sqlite" {
		tempTable = "FILE_LUMIS"
	}
	for _, rrr := range rec.Files {
		lfn := rrr.LogicalFileName
		//         fileID, ok := trec.FilesMap[lfn]
		fileID, ok := trec.FilesMap.Load(lfn)
		if !ok {
			msg := fmt.Sprintf("%s unable to find fileID in FilesMap for %s", hash, lfn)
			log.Println(msg)
			return Error(RecordErr, QueryErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		// there are three methods to insert FileLumi list
		// - via temp table
		// - via INSERT ALL and using chunks
		// - sequential method, i.e. record by record
		// we apply the following rules:
		// - if number of records is less FileLumiChunkSize we use sequential inserts
		// - otherwise we choose between temptable and chunks methods, and only use
		// temp table name, e.g. ORA$PTT_TEMP_FILE_LUMIS, for ORACLE inserts

		// insert FileLumi list via temptable or chunks
		if len(rrr.FileLumiList) > FileLumiChunkSize {

			if utils.VERBOSE > 0 {
				log.Printf(
					"insert FileLumi list via %s method %d records",
					FileLumiInsertMethod, len(rrr.FileLumiList))
			}

			var fileLumiList []FileLumis
			for _, r := range rrr.FileLumiList {
				fl := FileLumis{
					FILE_ID:          fileID.(int64),
					RUN_NUM:          r.RunNumber,
					LUMI_SECTION_NUM: r.LumiSectionNumber,
					EVENT_COUNT:      r.EventCount,
				}
				fileLumiList = append(fileLumiList, fl)
			}
			err = InsertFileLumisTxViaChunks(tx, tempTable, fileLumiList)
			if err != nil {
				msg := fmt.Sprintf(
					"%s unable to insert FileLumis records for %s, fileID %d, error %v",
					hash, lfn, fileID, err)
				log.Println(msg)
				return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
			}

		} else {
			if utils.VERBOSE > 0 {
				log.Println(hash, "insert FileLumi list sequentially", len(rrr.FileLumiList), "records")
			}

			// insert FileLumi list via sequential insert of file lumi records
			for _, r := range rrr.FileLumiList {
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
					FILE_ID:          fileID.(int64),
					RUN_NUM:          r.RunNumber,
					LUMI_SECTION_NUM: r.LumiSectionNumber,
					EVENT_COUNT:      r.EventCount,
				}
				data, err = json.Marshal(fl)
				if err != nil {
					msg := fmt.Sprintf("%s unable to marshal dataset file lumi list, error %v", hash, err)
					log.Println(msg)
					return Error(err, MarshalErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
				}
				api.Reader = bytes.NewReader(data)
				err = api.InsertFileLumisTx(tx)
				if err != nil {
					msg := fmt.Sprintf("%s unable to insert FileLumis record, error %v", hash, err)
					log.Println(msg)
					return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
				}
			}
		}
	}

	// insert file configuration
	for _, rrr := range rec.FileConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			msg := fmt.Sprintf("%s unable to marshal file config list, error %v", hash, err)
			log.Println(msg)
			return Error(err, MarshalErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		api.Reader = bytes.NewReader(data)
		err = api.InsertFileOutputModConfigs(tx)
		if err != nil {
			msg := fmt.Sprintf("%s unable to insert file output mod config, error %v", hash, err)
			log.Println(msg)
			return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
	}

	// insert file parent list
	data, err = json.Marshal(rec.FileParentList)
	if err != nil {
		msg := fmt.Sprintf("%s unable to marshal file parent list, error %v", hash, err)
		log.Println(msg)
		return Error(err, MarshalErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}
	api.Reader = bytes.NewReader(data)
	api.Params = make(Record)
	err = api.InsertFileParentsTxt(tx)
	if err != nil {
		msg := fmt.Sprintf("%s unable to insert file parents record %+v, error %v", hash, rec, err)
		log.Println(msg)
		return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}

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
			msg := fmt.Sprintf("%s unable to find dataset_id for %s, error %v", hash, ds, err)
			log.Println(msg)
			return Error(err, GetIDErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
		r := DatasetParents{THIS_DATASET_ID: datasetID, PARENT_DATASET_ID: pid}
		err = r.Insert(tx)
		if err != nil {
			msg := fmt.Sprintf("%s unable to insert parent dataset record, error %v", hash, err)
			log.Println(msg)
			return Error(err, InsertErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		msg := fmt.Sprintf("%s fail to commit transaction, error %v", hash, err)
		log.Println(msg)
		return Error(err, CommitErrorCode, msg, "dbs.bulkblocks.InsertBulkBlocksConcurrently")
	}
	if utils.VERBOSE > 1 {
		log.Println(hash, "successfully finished bulkblocks.InsertBulkBlocksConcurrently")
	}

	if a.Writer != nil {
		a.Writer.Write([]byte(`[]`))
	}
	return nil
}

// helper function to insert files via chunks injection
func insertFilesViaChunks(tx *sql.Tx, records []File, trec *TempFileRecord) error {
	chunkSize := FileChunkSize // optimal value should be around 50
	t0 := time.Now()
	ngoroutines := 0
	var wg sync.WaitGroup
	var err error
	var chunk []File
	fileIds, err := IncrementSequences(tx, "SEQ_FL", len(records))
	if err != nil {
		msg := fmt.Sprintf("unable to get file ids, error %v", err)
		log.Println(msg)
		return Error(err, LastInsertErrorCode, "", "dbs.bulkblocks2.insertFilesViaChunks")
	}
	if utils.VERBOSE > 1 {
		log.Println("get new file Ids", fileIds)
	}
	var ids []int64
	for i := 0; i < len(records); i = i + chunkSize {
		if i+chunkSize < len(records) {
			chunk = records[i : i+chunkSize]
			ids = fileIds[i : i+chunkSize]
		} else {
			chunk = records[i:]
			ids = fileIds[i:len(records)]
		}
		//         ids := getFileIds(fileID, int64(i), int64(i+chunkSize))
		wg.Add(1)
		go insertFilesChunk(tx, &wg, chunk, trec, ids)
		ngoroutines += 1
	}
	if utils.VERBOSE > 0 {
		log.Printf(
			"insertFilesViaChunks processed %d goroutines with ids %v, elapsed time %v",
			ngoroutines, ids, time.Since(t0))
	}
	wg.Wait()
	if trec.NErrors != 0 {
		msg := fmt.Sprintf("fail to insert files chunks, trec %+v", trec)
		log.Println(msg)
		return Error(ConcurrencyErr, InsertErrorCode, "", "dbs.bulkblocks.insertFilesViaChunks")
	}
	return nil
}

// helper function to get range of files ids starting from initial file id
// and chunk boundaries
func getFileIds(fid, idx, limit int64) []int64 {
	var ids []int64
	for i := idx; i < limit+1; i++ {
		ids = append(ids, int64(fid+i))
	}
	return ids
}

// helper function to insert files via chunks injection
func insertFilesChunk(
	tx *sql.Tx,
	wg *sync.WaitGroup,
	records []File,
	trec *TempFileRecord, ids []int64) {

	defer wg.Done()
	//     var rwm sync.RWMutex
	for idx, rrr := range records {
		lfn := rrr.LogicalFileName
		fileTypeID, err := GetID(tx, "FILE_DATA_TYPES", "file_type_id", "file_type", rrr.FileType)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("### trec unable to find file_type_id for", rrr.FileType, "lfn", lfn, "error", err)
			}
			trec.NErrors += 1
			return
		}
		// get branch hash ID and insert record if it does not exists
		//         if rrr.BranchHash == "" {
		//             rrr.BranchHash = "branch-hash"
		//         }

		cBy := rrr.LastModifiedBy
		if cBy == "" {
			cBy = trec.CreateBy
		}
		lBy := rrr.LastModifiedBy
		if lBy == "" {
			lBy = trec.CreateBy
		}
		fileID := ids[idx]
		r := Files{
			FILE_ID:                fileID,
			LOGICAL_FILE_NAME:      lfn,
			IS_FILE_VALID:          trec.IsFileValid,
			DATASET_ID:             trec.DatasetID,
			BLOCK_ID:               trec.BlockID,
			FILE_TYPE_ID:           fileTypeID,
			CHECK_SUM:              rrr.CheckSum,
			FILE_SIZE:              rrr.FileSize,
			EVENT_COUNT:            rrr.EventCount,
			ADLER32:                rrr.Adler32,
			MD5:                    rrr.MD5,
			AUTO_CROSS_SECTION:     rrr.AutoCrossSection,
			CREATION_DATE:          trec.CreationDate,
			CREATE_BY:              cBy,
			LAST_MODIFICATION_DATE: trec.CreationDate,
			LAST_MODIFIED_BY:       lBy,
		}
		// insert file lumi list record
		err = r.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("### trec unable to insert File record for lfn %s, error %v", lfn, err)
			}
			trec.NErrors += 1
			return
		}
		trec.FilesMap.Store(lfn, fileID)
		if utils.VERBOSE > 1 {
			log.Printf("trec inserted %s with fileID %d", lfn, fileID)
		}
	}
}
