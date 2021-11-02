package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/vkuznet/dbs2go/utils"
)

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
		&block.CreateBy,
		&block.CreationDate,
		&block.OpenForWriting,
		&block.BlockName,
		&block.FileCount,
		&block.OriginSiteName,
		&block.BlockSize,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}
func getDataset(blk string, wg *sync.WaitGroup, dataset *Dataset) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_dataset")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	err := DB.QueryRow(stm, args...).Scan(
		&dataset.CreateBy,
		&dataset.CreationDate,
		&dataset.PhysicsGroupName,
		&dataset.DatasetAccessType,
		&dataset.DataTierName,
		&dataset.LastModifiedBy,
		&dataset.ProcessedDSName,
		&dataset.Xtcrosssection,
		&dataset.LastModificationDate,
		&dataset.Dataset,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}
func getPrimaryDataset(blk string, wg *sync.WaitGroup, primaryDataset *PrimaryDataset) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_primds")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	err := DB.QueryRow(stm, args...).Scan(
		&primaryDataset.CreateBy,
		&primaryDataset.PrimaryDSType,
		&primaryDataset.PrimaryDSName,
		&primaryDataset.CreationDate,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}
func getProcessingEra(blk string, wg *sync.WaitGroup, processingEra *ProcessingEra) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_procera")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	err := DB.QueryRow(stm, args...).Scan(
		&processingEra.CreateBy,
		&processingEra.ProcessingVersion,
		&processingEra.Description,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}
func getAcquisitionEra(blk string, wg *sync.WaitGroup, acquisitionEra *AcquisitionEra) {
	defer wg.Done()
	var args []interface{}
	args = append(args, strings.Split(blk, "#")[0])
	stm := getSQL("blockdump_acqera")
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		utils.PrintSQL(stm, args, "execute")
	}

	err := DB.QueryRow(stm, args...).Scan(
		&acquisitionEra.AcquisitionEraName,
		&acquisitionEra.StartDate,
		&acquisitionEra.CreateBy,
	)
	if err != nil {
		log.Printf("query='%s' args='%v' error=%v", stm, args, err)
		return
	}
}

type FileList []File

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
		var bhash sql.NullString
		err = rows.Scan(
			&file.CheckSum,
			&file.Adler32,
			&file.FileSize,
			&file.EventCount,
			&file.FileType,
			&bhash,
			&file.LastModifiedBy,
			&file.LogicalFileName,
			&file.MD5,
			&file.AutoCrossSection,
		)
		if bhash.Valid {
			file.BranchHash = bhash.String
		}
		if err != nil {
			log.Println("unable to scan rows", err)
			return
		}

		// get file lumis for given LFN
		var fargs []interface{}
		fargs = append(fargs, file.LogicalFileName)
		fstm := getSQL("blockdump_filelumis")
		fstm = CleanStatement(stm)
		if utils.VERBOSE > 1 {
			utils.PrintSQL(stm, args, "execute")
		}
		frows, err := DB.Query(fstm, fargs...)
		if err != nil {
			log.Printf("query='%s' args='%v' error=%v", fstm, fargs, err)
			return
		}
		defer frows.Close()
		var fileLumiList []FileLumi
		for frows.Next() {
			fileLumi := FileLumi{}
			err = rows.Scan(
				&fileLumi.LumiSectionNumber,
				&fileLumi.RunNumber,
			)
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

type BlockParentList []BlockParent

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
			&blockParent.ThisBlockID,
			&blockParent.ParentBlock,
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

type DatasetParentList []string

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
		err = rows.Scan(
			&fileConfig.ReleaseVersion,
			&fileConfig.PsetHash,
			&fileConfig.LFN,
			&fileConfig.AppName,
			&fileConfig.OutputModuleLabel,
			&fileConfig.GlogalTag,
		)
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

type FileParentList []FileParent

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
		fileParent := FileParent{}
		err = rows.Scan(
			&fileParent.LogicalFileName,
			&fileParent.ParentLogicalFileName,
		)
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
		err = rows.Scan(
			&datasetConfig.ReleaseVersion,
			&datasetConfig.PsetHash,
			&datasetConfig.AppName,
			&datasetConfig.OutputModuleLabel,
			&datasetConfig.GlogalTag,
		)
		if err != nil {
			log.Println("unable to scan rows", err)
			return
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
	FILE_CONF_LIST      string   `json:"file_conf_list"`
	FILE_PARENT_LIST    string   `json:"file_parent_list"`
	DATASET_CONF_LIST   string   `json:"dataset_conf_list"`
}

// TODO: see dumpBlock function in
// ../../Server/Python/src/dbs/business/DBSBlock.py (blockDump)
// ../../Server/Python/src/dbs/business/DBSBlockInsert.py (putBlock)
/*
The BlockDump python API returns the following dict
   result = dict(block=block, dataset=dataset, primds=primds,
                 files=files, block_parent_list=bparent,
                 ds_parent_list=dsparent, file_conf_list=fconfig_list,
                 file_parent_list=fparent_list2, dataset_conf_list=dconfig_list)
*/

// BlockDump DBS API
func (a *API) BlockDump() error {

	blk, err := getSingleValue(a.Params, "block_name")
	if err != nil {
		return err
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
	var fileParentList FileParentList
	var blockParentList BlockParentList
	var datasetParentList DatasetParentList
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

	log.Println("waited for all goroutines to finish")

	// initialize BulkBlocks record
	rec := BulkBlocks{
		AcquisitionEra:    acquisitionEra,
		ProcessingEra:     processingEra,
		Block:             block,
		Dataset:           dataset,
		PrimaryDataset:    primaryDataset,
		Files:             files,
		BlockParentList:   blockParentList,
		DatasetParentList: datasetParentList,
		FileConfigList:    fileConfigList,
		FileParentList:    fileParentList,
		DatasetConfigList: datasetConfigList,
	}

	// write BulkBlocks record
	data, err := json.Marshal(rec)
	if err == nil {
		a.Writer.Write(data)
	}
	return err
}

// InsertBlockDump insert block dump record into DBS
func (r *BlockDumpRecord) InsertBlockDump() error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		msg := fmt.Sprintf("unable to get DB transaction %v", err)
		return errors.New(msg)
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
		return err
	}
	return err
}

// Validate implementation of Blocks
func (r *BlockDumpRecord) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if err := CheckPattern("block", r.BLOCK_NAME); err != nil {
		return err
	}
	if strings.Contains(r.BLOCK_NAME, "*") || strings.Contains(r.BLOCK_NAME, "%") {
		return errors.New("block name contains pattern")
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
