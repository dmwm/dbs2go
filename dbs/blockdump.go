package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/dmwm/dbs2go/utils"
)

// helper function to get block information
func getBlock(blk string, wg *sync.WaitGroup, block *Block) {
	defer wg.Done()
	var args []interface{}
	args = append(args, blk)
	stm := getSQL("blockdump_block")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	err := DB.QueryRow(stm, args...).Scan(
		&block.BlockID,
		&block.DatasetID,
		&block.CreateBy,
		&block.CreationDate,
		&block.OpenForWriting,
		&block.BlockName,
		&block.FileCount,
		&block.OriginSiteName,
		&block.BlockSize,
		&block.LastModifiedBy,
		&block.LastModificationDate,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}

// helper function to get dataset information
func getDataset(blk string, wg *sync.WaitGroup, dataset *Dataset) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_dataset")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	var xt sql.NullFloat64
	var pid sql.NullString
	err := DB.QueryRow(stm, args...).Scan(
		&dataset.DatasetID,
		&dataset.CreateBy,
		&dataset.CreationDate,
		&dataset.PhysicsGroupName,
		&dataset.DatasetAccessType,
		&dataset.DataTierName,
		&dataset.LastModifiedBy,
		&dataset.ProcessedDSName,
		&xt,
		&dataset.LastModificationDate,
		&dataset.Dataset,
		&pid,
	)
	if xt.Valid {
		dataset.Xtcrosssection = xt.Float64
	}
	if pid.Valid {
		dataset.PrepID = pid.String
	}
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}

// helper function to get primary dataset information
func getPrimaryDataset(blk string, wg *sync.WaitGroup, primaryDataset *PrimaryDataset) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_primds")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	var cby sql.NullString
	err := DB.QueryRow(stm, args...).Scan(
		&primaryDataset.PrimaryDSId,
		&cby,
		&primaryDataset.PrimaryDSType,
		&primaryDataset.PrimaryDSName,
		&primaryDataset.CreationDate,
	)
	if cby.Valid {
		primaryDataset.CreateBy = cby.String
	}
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}

// helper function to get procesing era information
func getProcessingEra(blk string, wg *sync.WaitGroup, processingEra *ProcessingEra) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_procera")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	var cby, desc sql.NullString
	err := DB.QueryRow(stm, args...).Scan(
		&cby,
		&processingEra.ProcessingVersion,
		&desc,
	)
	if cby.Valid {
		processingEra.CreateBy = cby.String
	}
	if desc.Valid {
		processingEra.Description = desc.String
	}
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}

// helper function to get acquisition era information
func getAcquisitionEra(blk string, wg *sync.WaitGroup, acquisitionEra *AcquisitionEra) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_acqera")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	var cby, desc sql.NullString
	var cdate sql.NullInt64
	err := DB.QueryRow(stm, args...).Scan(
		&acquisitionEra.AcquisitionEraName,
		&acquisitionEra.StartDate,
		&cdate,
		&cby,
		&desc,
	)
	if cdate.Valid {
		acquisitionEra.CreationDate = cdate.Int64
	}
	if cby.Valid {
		acquisitionEra.CreateBy = cby.String
	}
	if desc.Valid {
		acquisitionEra.Description = desc.String
	}
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}

// FileList represents list of File records
type FileList []File

// helper function to get file list information
func getFileList(blk string, wg *sync.WaitGroup, files *FileList) {
	defer wg.Done()
	var args []interface{}
	args = append(args, blk)
	stm := getSQL("blockdump_files")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	rows, err := DB.Query(stm, args...)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		file := File{}
		var md5 sql.NullString
		var xt sql.NullFloat64
		err = rows.Scan(
			&file.CheckSum,
			&file.Adler32,
			&file.FileSize,
			&file.EventCount,
			&file.FileType,
			&file.LastModifiedBy,
			&file.LastModificationDate,
			&file.LogicalFileName,
			&md5,
			&xt,
			&file.IsFileValid,
		)
		if md5.Valid {
			file.MD5 = md5.String
		}
		if xt.Valid {
			file.AutoCrossSection = xt.Float64
		}
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}

		// get file lumis for given LFN
		var fargs []interface{}
		fargs = append(fargs, file.LogicalFileName)
		fstm := getSQL("blockdump_filelumis")
		fstm = CleanStatement(fstm)
		if utils.VERBOSE > 1 {
			utils.PrintSQL(fstm, fargs, "execute")
		}
		frows, err := DB.Query(fstm, fargs...)
		if err != nil {
			log.Printf("query='%s' args='%v' error=%v", fstm, fargs, err)
			return
		}
		defer frows.Close()
		// ensure that fileLumiList will be serialized as empty list [] and not as null
		fileLumiList := make([]FileLumi, 0)
		for frows.Next() {
			fileLumi := FileLumi{}
			var evt sql.NullInt64
			err = frows.Scan(
				&fileLumi.LumiSectionNumber,
				&fileLumi.RunNumber,
				&evt,
			)
			if evt.Valid {
				fileLumi.EventCount = evt.Int64
			}
			if err != nil {
				log.Println("unable to scan rows", err)
				return
			}
			fileLumiList = append(fileLumiList, fileLumi)
		}
		if err = frows.Err(); err != nil {
			log.Printf("rows error %v", err)
		}
		file.FileLumiList = fileLumiList
		*files = append(*files, file)
	}
	if err = rows.Err(); err != nil {
		log.Printf("rows error %v", err)
	}
}

// BlockParentList represents BlockParent records
type BlockParentList []BlockParent

// helper function to get block parents information
func getBlockParentList(blk string, wg *sync.WaitGroup, blockParentList *BlockParentList) {
	defer wg.Done()
	var args []interface{}
	args = append(args, blk)
	stm := getSQL("blockdump_blockparents")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	rows, err := DB.Query(stm, args...)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		blockParent := BlockParent{}
		err = rows.Scan(
			&blockParent.ThisBlockName,
			&blockParent.ParentBlockName,
		)
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}
		*blockParentList = append(*blockParentList, blockParent)
	}
	if err = rows.Err(); err != nil {
		log.Printf("rows error %v", err)
	}
}

// DatasetParentList represents list of dataset parents
type DatasetParentList []string

// helper function to get dataset parents information
func getDatasetParentList(blk string, wg *sync.WaitGroup, datasetParentList *DatasetParentList) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_datasetparents")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	rows, err := DB.Query(stm, args...)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var datasetParent string
		err = rows.Scan(&datasetParent)
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}
		*datasetParentList = append(*datasetParentList, datasetParent)
	}
	if err = rows.Err(); err != nil {
		log.Printf("rows error %v", err)
	}
}

// FileConfigList represents FileConfig records
type FileConfigList []FileConfig

func getFileConfigList(blk string, wg *sync.WaitGroup, fileConfigList *FileConfigList) {
	defer wg.Done()
	var args []interface{}
	args = append(args, blk)
	stm := getSQL("blockdump_fileconfigs")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	rows, err := DB.Query(stm, args...)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		fileConfig := FileConfig{}
		var pname sql.NullString
		err = rows.Scan(
			&fileConfig.ReleaseVersion,
			&fileConfig.PsetHash,
			&pname,
			&fileConfig.LFN,
			&fileConfig.AppName,
			&fileConfig.OutputModuleLabel,
			&fileConfig.GlobalTag,
			&fileConfig.CreateBy,
			&fileConfig.CreationDate,
		)
		if pname.Valid {
			fileConfig.PsetName = pname.String
		}
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}
		*fileConfigList = append(*fileConfigList, fileConfig)
	}
	if err = rows.Err(); err != nil {
		log.Printf("rows error %v", err)
	}
}

// FileParentList represents FileParent records
type FileParentList []FileParentRecord

func getFileParentList(blk string, wg *sync.WaitGroup, fileParentList *FileParentList) {
	defer wg.Done()
	var args []interface{}
	args = append(args, blk)
	stm := getSQL("blockdump_fileparents")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	rows, err := DB.Query(stm, args...)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		fileParent := FileParentRecord{}
		var pfn sql.NullString
		var pfnid sql.NullInt64
		// blockdump DBS API should yield this_logical_file_name
		// therefore, here we use it
		// NOTE2: when we perform switch from python based DBSMigrate/DBSMigration
		// to Go based one we do not need pfnid in this out, therefore we can
		// remove it here and blockdump_fileparents template
		err = rows.Scan(
			//             &fileParent.LogicalFileName,
			&fileParent.ThisLogicalFileName,
			&pfn,
			&pfnid,
		)
		if pfn.Valid {
			fileParent.ParentLogicalFileName = pfn.String
		}
		if pfnid.Valid {
			fileParent.ParentFileID = pfnid.Int64
		}
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}
		*fileParentList = append(*fileParentList, fileParent)
	}
	if err = rows.Err(); err != nil {
		log.Printf("rows error %v", err)
	}
}

// DatasetConfigList represents DatasetConfig records
type DatasetConfigList []DatasetConfig

func getDatasetConfigList(blk string, wg *sync.WaitGroup, datasetConfigList *DatasetConfigList) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_datasetconfigs")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	rows, err := DB.Query(stm, args...)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		datasetConfig := DatasetConfig{}
		var pname sql.NullString
		err = rows.Scan(
			&datasetConfig.ReleaseVersion,
			&pname,
			&datasetConfig.PsetHash,
			&datasetConfig.AppName,
			&datasetConfig.OutputModuleLabel,
			&datasetConfig.GlobalTag,
			&datasetConfig.CreateBy,
			&datasetConfig.CreationDate,
		)
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}
		if pname.Valid {
			datasetConfig.PsetName = pname.String
		}
		*datasetConfigList = append(*datasetConfigList, datasetConfig)
	}
	if err = rows.Err(); err != nil {
		log.Printf("rows error %v", err)
	}
}

// BlockDumpRecord represents input block record used in BlockDump and InsertBlockDump APIs
type BlockDumpRecord struct {
	BLOCK_ID            int64    `json:"block_id"`
	BLOCK_NAME          string   `json:"block_name"`
	DATASET             string   `json:"dataset"`
	PRIMARY_DATASET     string   `json:"prim_ds"`
	FILES               []string `json:"files"`
	BLOCK_PARENT_LIST   string   `json:"block_parent_list"`
	DATASET_PARENT_LIST string   `json:"dataset_parent_list"`
	DS_PARENT_LIST      string   `json:"ds_parent_list"` // for compatibility with Py server
	FILE_CONF_LIST      string   `json:"file_conf_list"`
	FILE_PARENT_LIST    string   `json:"file_parent_list"`
	DATASET_CONF_LIST   string   `json:"dataset_conf_list"`
}

// BlockDump DBS API
func (a *API) BlockDump() error {

	blk, err := getSingleValue(a.Params, "block_name")
	if err != nil {
		return Error(err, ParametersErrorCode, "", "dbs.blockdump.BlockDump")
	}

	// fill out BulkBlock record via async calls
	var datasetConfigList DatasetConfigList
	var fileConfigList FileConfigList
	var files FileList
	var processingEra ProcessingEra
	var primaryDataset PrimaryDataset
	var dataset Dataset
	var acquisitionEra AcquisitionEra
	var block Block
	// in order to get proper JSON serialization for empty list,
	// i.e. [] instead of null, we should make empty lists
	// https://apoorvam.github.io/blog/2017/golang-json-marshal-slice-as-empty-array-not-null/
	fileParentList := make(FileParentList, 0)
	blockParentList := make(BlockParentList, 0)
	datasetParentList := make(DatasetParentList, 0)

	// get concurrently all necessary information required for block dump
	var wg sync.WaitGroup
	wg.Add(11) // wait for 11 goroutines below
	go getBlock(blk, &wg, &block)
	go getDataset(blk, &wg, &dataset)
	go getPrimaryDataset(blk, &wg, &primaryDataset)
	go getProcessingEra(blk, &wg, &processingEra)
	go getAcquisitionEra(blk, &wg, &acquisitionEra)
	go getFileList(blk, &wg, &files)
	go getBlockParentList(blk, &wg, &blockParentList)
	go getDatasetParentList(blk, &wg, &datasetParentList)
	go getFileConfigList(blk, &wg, &fileConfigList)
	go getFileParentList(blk, &wg, &fileParentList)
	go getDatasetConfigList(blk, &wg, &datasetConfigList)
	wg.Wait()

	if utils.VERBOSE > 1 {
		log.Println("waited for all goroutines to finish")
	}
	// prepare dsParentList in form of []DatasetParent
	dsParentList := make([]DatasetParent, 0)
	for _, d := range datasetParentList {
		dsParentList = append(dsParentList, DatasetParent{ParentDataset: d})
	}

	// initialize BulkBlocks record
	rec := BulkBlocks{
		AcquisitionEra:    acquisitionEra,
		ProcessingEra:     processingEra,
		Block:             block,
		Dataset:           dataset,
		PrimaryDataset:    primaryDataset,
		Files:             files,
		BlockParentList:   blockParentList,
		DatasetParentList: datasetParentList, // used by bulkblocks API
		DsParentList:      dsParentList,      // provided by blockdump API
		FileConfigList:    fileConfigList,
		FileParentList:    fileParentList,
		DatasetConfigList: datasetConfigList,
	}

	// write BulkBlocks record
	data, err := json.Marshal(rec)
	if err == nil {
		a.Writer.Write(data)
		return nil
	}
	return Error(err, MarshalErrorCode, "", "dbs.blockdump.BlockDump")
}

// InsertBlockDump insert block dump record into DBS
func (r *BlockDumpRecord) InsertBlockDump() error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := "unable to get DB transaction"
		return Error(err, TransactionErrorCode, msg, "dbs.blockdump.InsertBlockDump")
	}
	defer tx.Rollback()

	var tid int64
	if r.BLOCK_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "BLOCKS", "block_id")
			r.BLOCK_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_BK")
			r.BLOCK_ID = tid
		}
		if err != nil {
			return Error(err, LastInsertErrorCode, "", "dbs.blockdump.InsertBlockDump")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.blockdump.InsertBlockDump")
	}
	// logic of insertion
	// - insert dataset_conf_list
	// - insert dataset
	// - insert block
	// - insert files
	// - insert file lumis
	// - insert file configuration
	// - insert block and dataset parentage

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("fail to commit transaction", err)
		return Error(err, CommitErrorCode, "", "dbs.blockdump.InsertBlockDump")
	}
	return nil
}

// Validate implementation of Blocks
func (r *BlockDumpRecord) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("block", r.BLOCK_NAME); err != nil {
		return Error(err, PatternErrorCode, "", "dbs.blockdump.Validate")
	}
	if strings.Contains(r.BLOCK_NAME, "*") || strings.Contains(r.BLOCK_NAME, "%") {
		msg := "block name contains pattern"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.blockdump.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Blocks
func (r *BlockDumpRecord) SetDefaults() {
}

// Decode implementation for Blocks
func (r *BlockDumpRecord) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.blockdump.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.blockdump.Decode")
	}
	return nil
}
