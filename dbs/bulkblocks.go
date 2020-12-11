package dbs

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	OpenForWriting int    `json:"open_for_writing"`
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
func (API) BulkBlocks(decoder *json.Decoder) error {
	var rec BulkBlocks
	err := decoder.Decode(&rec)
	if err != nil {
		log.Println("BulkBlocks decoder error", err)
		return err
	}
	// API logic
	// validate input record
	if err := validateBulkBlockData(rec); err != nil {
		log.Println("ERROR: invalid BulkBlock record", err)
		return err
	}
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
	}
	defer tx.Rollback()
	// insert configuration and get back config list
	// insert dataset with config list and get back dataset_id
	// insert block with dataset_id
	// insert files
	// insert file parent list
	// insert file lumi list
	// insert file config object
	// insert dataset parent list
	data, err := json.MarshalIndent(rec, "", "    ")
	if err == nil {
		log.Printf("BulkBlocks record: %+v\n", string(data))
	}
	return nil
}

// helper function to validate bulk block data
func validateBulkBlockData(rec BulkBlocks) error {
	return nil
}

// helper function to insert configuration
func insertConfiguration(rec BulkBlocks) {
}
