package dbs

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/vkuznet/dbs2go/utils"
)

/* NOTES: we should use bulk insert
 * see Server/Python/src/dbs/dao/Oracle/File/Insert2.py
 *     Server/Python/src/dbs/business/DBSBlockInsert.py
 *     Server/Python/src/dbs/dao/Oracle/FileLumi/Insert.py
 */

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

// InsertBulkBlocksConcurrently DBS API provides concurrent bulk blocks
// insertion. It inherits the same logic as BulkBlocks API but perform
// Files and FileLumis injection concurrently via chunk of record.
// It relies on the following parameters:
// - FileChunkSize defines number of concurrent goroutines executing injection into
//   FILES table
// - FileLumiChunkSize/FileLumiMaxSize defines concurrent injection into
//   FILE_LUMIS table. The former specifies chunk size while latter total number of
//   records to be inserted at once to ORABLE DB
// - FileLumiInsertMethod defines which method to use for workflow execution, so far
//   we support temptable, chunks, and sequential methods. The temptable uses
//   ORACLE TEMPTABLE approach, chunks uses direct tables, and sequential method
//   fallback to record by record injection (no goroutines).
//
// gocyclo:ignore
func (a *API) InsertBulkBlocksConcurrently() error {
	// read input data
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("unable to read bulkblock input", err)
		return err
	}

	// unmarshal the data into BulkBlocks record
	var rec BulkBlocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Printf("unable to unmarshal bulkblock record %s, error %v", string(data), err)
		return err
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()

	var reader *bytes.Reader
	api := &API{
		Reader:   reader,
		CreateBy: a.CreateBy,
		Params:   make(Record),
	}
	//     var isFileValid, datasetID, blockID, fileID, fileTypeID int64
	var isFileValid, datasetID, blockID int64
	var primaryDatasetTypeID, primaryDatasetID, acquisitionEraID, processingEraID int64
	var dataTierID, physicsGroupID, processedDatasetID, datasetAccessTypeID int64
	creationDate := time.Now().Unix()

	// insert dataset configuration
	if utils.VERBOSE > 1 {
		log.Println("insert output configs")
	}
	for _, rrr := range rec.DatasetConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			log.Println("unable to marshal dataset config list", err)
			return err
		}
		api.Reader = bytes.NewReader(data)
		err = api.InsertOutputConfigsTx(tx)
		if err != nil {
			return err
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
		return err
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
		return err
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
		return err
	}

	// insert acquisition era if it does not exists
	if utils.VERBOSE > 1 {
		log.Println("get acquisition era ID")
	}
	if rec.AcquisitionEra.CreateBy == "" {
		rec.AcquisitionEra.CreateBy = a.CreateBy
	}
	aera := AcquisitionEras{
		ACQUISITION_ERA_NAME: rec.AcquisitionEra.AcquisitionEraName,
		START_DATE:           rec.AcquisitionEra.StartDate,
		END_DATE:             0,
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
		return err
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
		return err
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
		return err
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
		return err
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
			return err
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
			return err
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
		CREATION_DATE:          rec.Dataset.CreationDate,
		CREATE_BY:              rec.Dataset.CreateBy,
		LAST_MODIFICATION_DATE: creationDate,
		LAST_MODIFIED_BY:       rec.Dataset.CreateBy,
	}
	// get datasetID
	if utils.VERBOSE > 1 {
		log.Printf("get dataset ID for %+v", dataset)
	}
	datasetID, err = GetRecID(
		tx,
		&dataset,
		"DATASETS",
		"dataset_id",
		"dataset",
		rec.Dataset.Dataset,
	)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to insert dataset record", err)
		}
		return err
	}
	//     datasetID, err = GetID(tx, "DATASETS", "dataset_id", "dataset", rec.Dataset.Dataset)
	//     if err != nil {
	//         if utils.VERBOSE > 1 {
	//             log.Println("unable to find dataset_id for", rec.Dataset.Dataset, "will insert")
	//         }
	//         err = dataset.Insert(tx)
	//         if err != nil {
	//             if utils.VERBOSE > 1 {
	//                 log.Println("unable to insert dataset record", err)
	//             }
	//             return err
	//         }
	//         datasetID, err = GetID(tx, "DATASETS", "dataset_id", "dataset", rec.Dataset.Dataset)
	//         if err != nil {
	//             if utils.VERBOSE > 1 {
	//                 log.Printf("unable to get dataset_id for dataset %s error %v", rec.Dataset.Dataset, err)
	//             }
	//             return err
	//         }
	//     }

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
			return err
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
			return err
		}
		blockID, err = GetID(tx, "BLOCKS", "block_id", "block_name", rec.Block.BlockName)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Printf("unable to find block_id for %s, error %v", rec.Block.BlockName, err)
			}
			return err
		}
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
			if utils.VERBOSE > 1 {
				log.Println("unable to find file_type_id for", ftype)
			}
			return err
		}
		//         if err != nil {
		//             if utils.VERBOSE > 0 {
		//                 log.Println("FileDataType insert error", err)
		//             }
		//         }
	}
	// insert files
	if utils.VERBOSE > 1 {
		log.Println("insert files")
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
		if utils.VERBOSE > 1 {
			log.Println("unable to insert files", err)
		}
		return err
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
			log.Printf("unable to find fileID in FilesMap for %s", lfn)
			return errors.New("unable to find fileID in filesMap")
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
				log.Printf("insert FileLumi list via %s method %d records", FileLumiInsertMethod, len(rrr.FileLumiList))
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
				if utils.VERBOSE > 1 {
					log.Printf("unable to insert FileLumis records for %s, fileID %d, error %v", lfn, fileID, err)
				}
				return err
			}

		} else {
			if utils.VERBOSE > 0 {
				log.Println("insert FileLumi list sequentially", len(rrr.FileLumiList), "records")
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
					if utils.VERBOSE > 1 {
						log.Println("unable to marshal dataset file lumi list", err)
					}
					return err
				}
				api.Reader = bytes.NewReader(data)
				err = api.InsertFileLumisTx(tx)
				if err != nil {
					if utils.VERBOSE > 1 {
						log.Println("unable to insert FileLumis record", err)
					}
					return err
				}
			}
		}
	}

	// insert file configuration
	for _, rrr := range rec.FileConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to marshal file config list", err)
			}
			return err
		}
		api.Reader = bytes.NewReader(data)
		err = api.InsertFileOutputModConfigs(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert file output mod config", err)
			}
			return err
		}
	}

	// insert file parent list
	data, err = json.Marshal(rec.FileParentList)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to marshal file parent list", err)
		}
		return err
	}
	api.Reader = bytes.NewReader(data)
	api.Params = make(Record)
	err = api.InsertFileParentsTxt(tx)
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("unable to insert file parents", err)
		}
		return err
	}

	// insert dataset parent list
	datasetParentList := rec.DatasetParentList
	// use both DatasetParentList and DsParentList (for backward compatibility)
	// and compose unique set of dataset parents
	for _, d := range rec.DsParentList {
		datasetParentList = append(datasetParentList, d.ParentDataset)
	}
	datasetParentList = utils.List2Set(datasetParentList)
	for _, ds := range datasetParentList {
		// get file id for parent dataset
		pid, err := GetID(tx, "DATASETS", "dataset_id", "dataset", ds)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to find dataset_id for", ds)
			}
			return err
		}
		r := DatasetParents{THIS_DATASET_ID: datasetID, PARENT_DATASET_ID: pid}
		err = r.Insert(tx)
		if err != nil {
			if utils.VERBOSE > 1 {
				log.Println("unable to insert parent dataset record", err)
			}
			return err
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		if utils.VERBOSE > 1 {
			log.Println("fail to commit transaction", err)
		}
		return err
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
	// get first available fileID to use
	//     fileID, err := getFileID(tx)
	//     if err != nil {
	//         log.Println("unable to getFileID", err)
	//         return err
	//     }
	fileIds, err := IncrementSequences(tx, "SEQ_FL", len(records))
	if err != nil {
		msg := fmt.Sprintf("unable to get file ids, error %v", err)
		log.Println(msg)
		return errors.New(msg)
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
			chunk = records[i:len(records)]
			ids = fileIds[i:len(records)]
		}
		//         ids := getFileIds(fileID, int64(i), int64(i+chunkSize))
		wg.Add(1)
		go insertFilesChunk(tx, &wg, chunk, trec, ids)
		ngoroutines += 1
	}
	if utils.VERBOSE > 0 {
		log.Printf("insertFilesViaChunks processed %d goroutines, elapsed time %v", ngoroutines, time.Since(t0))
	}
	wg.Wait()
	if trec.NErrors != 0 {
		msg := fmt.Sprintf("fail to insert files chunks, trec +%v", trec)
		log.Println(msg)
		return errors.New(msg)
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
func insertFilesChunk(tx *sql.Tx, wg *sync.WaitGroup, records []File, trec *TempFileRecord, ids []int64) {
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
		if rrr.BranchHash == "" {
			rrr.BranchHash = "branch-hash"
		}

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
		//         fileID, err := GetID(tx, "FILES", "file_id", "logical_file_name", lfn)
		//         if err != nil {
		//             if utils.VERBOSE > 1 {
		//                 log.Println("trec unable to find file_id for", lfn, "will insert")
		//             }
		//             err = r.Insert(tx)
		//             if err != nil {
		//                 if utils.VERBOSE > 1 {
		//                     log.Printf("### trec unable to insert File record for lfn %s, error %v", lfn, err)
		//                 }
		//                 trec.NErrors += 1
		//                 return
		//             }
		//         }
		//         rwm.Lock()
		//         trec.FilesMap[lfn] = fileID
		trec.FilesMap.Store(lfn, fileID)
		if utils.VERBOSE > 1 {
			log.Printf("trec inserted %s with fileID %d", lfn, fileID)
		}
		//         rwm.Unlock()
	}
}
