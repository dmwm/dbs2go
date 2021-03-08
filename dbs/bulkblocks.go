package dbs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"
)

// BulkBlocks represents bulk block JSON structure
type BulkBlocks struct {
	DatasetConfigList []DatasetConfig `json:"dataset_conf_list"`
	FileConfigList    []FileConfig    `json:"file_conf_list"`
	Files             []File          `json:"files"`
	ProcessingEra     ProcessingEra   `json:"processing_era"`
	PrimaryDataset    PrimaryDataset  `json:"primds"`
	Dataset           Dataset         `json:"dataset"`
	AcquisitionEra    AcquisitionEra  `json:"acquisition_era"`
	Block             Block           `json:"block"`
	FileParentList    []FileParent    `json:"file_parent_list"`
}

// DatasetConfig represents dataset config structure
type DatasetConfig struct {
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
	GlogalTag         string `json:"global_tag"`
}

// FileConfig represents file config structure
type FileConfig struct {
	ReleaseVersion    string `json:"release_version"`
	PsetHash          string `json:"pset_hash"`
	LFN               string `json:"lfn"`
	AppName           string `json:"app_name"`
	OutputModuleLabel string `json:"output_module_label"`
	GlogalTag         string `json:"global_tag"`
}

// FileLumi represents file lumi structure
type FileLumi struct {
	LumiSectionNumber int64 `json:"lumi_section_num"`
	RunNumber         int64 `json:"run_num"`
}

// File represents file structure
type File struct {
	CheckSum         string     `json:"check_sum"`
	FileLumiList     []FileLumi `json:"file_lumi_list"`
	Adler32          string     `json:"adler32"`
	FileSize         int64      `json:"file_size"`
	EventCount       int64      `json:"event_count"`
	FileType         string     `json:"file_type"`
	LastModifiedBy   string     `json:"last_modified_by"`
	LogicalFileName  string     `json:"logical_file_name"`
	MD5              string     `json:"md5"`
	AutoCrossSection float64    `json:"auto_cross_section"`
}

// ProcessingEra represents processing era structure
type ProcessingEra struct {
	CreateBy          string `json:"create_by"`
	ProcessingVersion int64  `json:"processing_version"`
	Description       string `json:"description"`
}

// PrimaryDataset represents primary dataset structure
type PrimaryDataset struct {
	CreateBy      string `json:"create_by"`
	PrimaryDSType string `json:"primary_ds_type"`
	PrimaryDSName string `json:"primary_ds_name"`
	CreationDate  int64  `json:"creation_date"`
}

// Dataset represents dataset structure
type Dataset struct {
	CreateBy             string  `json:"create_by"`
	CreationDate         int64   `json:"creation_date"`
	PhysicsGroupName     string  `json:"physics_group_name"`
	DatasetAccessType    string  `json:"dataset_access_type"`
	DataTierName         string  `json:"data_tier_name"`
	LastModifiedBy       string  `json:"last_modified_by"`
	ProcessedDSName      string  `json:"processed_ds_name"`
	Xtcrosssection       float64 `json:"xtcrosssection"`
	LastModificationDate int64   `json:"last_modification_date"`
	Dataset              string  `json:"dataset'`
}

// AcquisitionEra represents AcquisitionEra structure
type AcquisitionEra struct {
	AcquisitionEraName string `json:"acquisition_era_name"`
	StartDate          int64  `json:"start_date"`
}

// Block represents Block structure
type Block struct {
	CreateBy       string `json:"create_by"`
	CreationDate   int64  `json:"creation_date"`
	OpenForWriting int64  `json:"open_for_writing"`
	BlockName      string `json:"block_name"`
	FileCount      int64  `json:"file_count"`
	OriginSiteName string `json:"origin_site_name"`
	BlockSize      int64  `json:"block_size"`
}

// FileParent represents file parent structure
type FileParent struct {
	LogicalFileName       string `json:"logical_file_name"`
	ParentLogicalFileName string `json:"parent_logical_file_name"`
}

// BulkBlocks DBS API
// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSBlockInsert.py
// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/web/DBSWriterModel.py
/*
   #1 insert configuration
   configList = self.insertOutputModuleConfig(
                   blockcontent['dataset_conf_list'], migration)
   #2 insert dataset
   datasetId = self.insertDataset(blockcontent, configList, migration)
   #3 insert block & files
   self.insertBlockFile(blockcontent, datasetId, migration)
*/
func (API) InsertBulkBlocks(r io.Reader) (int64, error) {
	//     var rec BulkBlocks
	//     err := decoder.Decode(&rec)
	//     if err != nil {
	//         log.Println("BulkBlocks decoder error", err)
	//         return 0, err
	//     }
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Println("unable to read bulkblock input", err)
		return 0, err
	}
	size := int64(len(data))
	var rec BulkBlocks
	err = json.Unmarshal(data, &rec)
	if err != nil {
		log.Println("unable to unmarshal bulkblock record", err)
		return 0, err
	}

	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return 0, errors.New(msg)
	}
	defer tx.Rollback()

	var reader *bytes.Reader
	var api API
	var isFileValid, datasetID, blockID, fileTypeID, branchHashID int64
	var primaryDatasetTypeID, primaryDatasetID, acquisitionEraID, processingEraID int64
	var dataTierID, physicsGroupID, processingDatasetID, datasetAccessTypeID int64
	var createBy string
	creationDate := time.Now().Unix()

	// insert dataset configuration
	for _, rrr := range rec.DatasetConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			log.Println("unable to marshal dataset config list", err)
			return 0, err
		}
		reader = bytes.NewReader(data)
		_, err = api.InsertOutputConfigs(reader)
		if err != nil {
			return 0, err
		}
	}
	// TODO: get outputModConfigID

	// insert file configuration
	for _, rrr := range rec.FileConfigList {
		data, err = json.Marshal(rrr)
		if err != nil {
			log.Println("unable to marshal file config list", err)
			return 0, err
		}
		reader = bytes.NewReader(data)
		_, err = api.InsertFileOutputModConfigs(reader)
		if err != nil {
			return 0, err
		}
	}
	// TODO: get fileOutputModConfigID

	// insert primary dataset
	// TODO: get primaryDatasetTypeID from rec.PrimaryDataset.PrimaryDSTypeName
	primDS := PrimaryDatasets{
		PRIMARY_DS_NAME:    rec.PrimaryDataset.PrimaryDSName,
		PRIMARY_DS_TYPE_ID: primaryDatasetTypeID,
		CREATION_DATE:      rec.PrimaryDataset.CreationDate,
		CREATE_BY:          rec.PrimaryDataset.CreateBy,
	}
	err = primDS.Validate()
	if err != nil {
		log.Println("unable to validate primary dataset record", err)
		return 0, err
	}
	err = primDS.Insert(tx)
	if err != nil {
		log.Println("unable to insert primary dataset record", err)
		return 0, err
	}
	// TODO: get primaryDatasetID

	// insert processing era
	pera := ProcessingEras{
		PROCESSING_VERSION: rec.ProcessingEra.ProcessingVersion,
		CREATION_DATE:      creationDate,
		CREATE_BY:          rec.ProcessingEra.CreateBy,
		DESCRIPTION:        rec.ProcessingEra.Description,
	}
	err = pera.Validate()
	if err != nil {
		log.Println("unable to validate processing era record", err)
		return 0, err
	}
	err = pera.Insert(tx)
	if err != nil {
		log.Println("unable to insert processing era record", err)
		return 0, err
	}
	// TODO: get processingEraID

	// insert acquisition era
	aera := AcquisitionEras{
		ACQUISITION_ERA_NAME: rec.AcquisitionEra.AcquisitionEraName,
		START_DATE:           rec.AcquisitionEra.StartDate,
		END_DATE:             0,
		CREATION_DATE:        creationDate,
		CREATE_BY:            createBy,
	}
	err = aera.Validate()
	if err != nil {
		log.Println("unable to validate acquisition era record", err)
		return 0, err
	}
	err = aera.Insert(tx)
	if err != nil {
		log.Println("unable to insert acquisition era record", err)
		return 0, err
	}
	// TODO: get acquisitionEraID

	// insert dataset
	dataset := Datasets{
		DATASET:                rec.Dataset.Dataset,
		IS_DATASET_VALID:       1,
		PRIMARY_DS_ID:          primaryDatasetID,
		PROCESSED_DS_ID:        processingDatasetID,
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
	err = dataset.Validate()
	if err != nil {
		log.Println("unable to validate dataset record", err)
		return 0, err
	}
	err = dataset.Insert(tx)
	if err != nil {
		log.Println("unable to insert dataset record", err)
		return 0, err
	}
	// TODO: get datasetID

	// insert block
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
	err = blk.Validate()
	if err != nil {
		log.Println("unable to validate block record", err)
		return 0, err
	}
	err = blk.Insert(tx)
	if err != nil {
		log.Println("unable to insert block record", err)
		return 0, err
	}
	// TODO: get blockID

	// insert files
	for _, rrr := range rec.Files {
		r := Files{
			LOGICAL_FILE_NAME:      rrr.LogicalFileName,
			IS_FILE_VALID:          isFileValid,
			DATASET_ID:             datasetID,
			BLOCK_ID:               blockID,
			FILE_TYPE_ID:           fileTypeID,
			CHECK_SUM:              rrr.CheckSum,
			FILE_SIZE:              rrr.FileSize,
			EVENT_COUNT:            rrr.EventCount,
			BRANCH_HASH_ID:         branchHashID,
			ADLER32:                rrr.Adler32,
			MD5:                    rrr.MD5,
			AUTO_CROSS_SECTION:     rrr.AutoCrossSection,
			CREATION_DATE:          creationDate,
			CREATE_BY:              rrr.LastModifiedBy,
			LAST_MODIFICATION_DATE: creationDate,
			LAST_MODIFIED_BY:       rrr.LastModifiedBy,
		}
		err = r.Validate()
		if err != nil {
			log.Println("unable to validate File record", err)
			return 0, err
		}
		err = r.Insert(tx)
		if err != nil {
			log.Println("unable to insert File record", err)
			return 0, err
		}
	}

	// insert file parent list
	// insert file lumi list
	// insert file config object
	// insert dataset parent list

	//     data, err = json.MarshalIndent(rec, "", "    ")
	//     if err == nil {
	//         log.Printf("BulkBlocks record: %+v\n", string(data))
	//     }
	return size, nil
}

/*

// helper function to validate bulk block data
func validateBulkBlockData(rec BulkBlocks) error {
	return nil
}

// helper function to insert configuration
func insertConfiguration(rec BulkBlocks) {
}
*/
