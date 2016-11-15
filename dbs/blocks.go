package dbs

import (
	"fmt"
	"strings"
)

// blocks API
func (API) Blocks(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	blocks := getValues(params, "block_name")
	if len(blocks) > 1 {
		panic("Unsupported list of blocks")
	} else if len(blocks) == 1 {
		op, val := opVal(blocks[0])
		cond := fmt.Sprintf(" B.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	datasets := getValues(params, "dataset")
	if len(datasets) > 1 {
		panic("The files API does not support list of datasets")
	} else if len(datasets) == 1 {
		op, val := opVal(datasets[0])
		cond := fmt.Sprintf(" DS.DATASET %s %s", op, placeholder("dataset"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blocks")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// blockparent API
func (API) BlockParent(params Record) []Record {
	// variables we'll use in where clause
	var args []interface{}
	where := "WHERE "

	// parse dataset argument
	blockparent := getValues(params, "block_name")
	if len(blockparent) > 1 {
		panic("Unsupported list of blockparent")
	} else if len(blockparent) == 1 {
		op, val := opVal(blockparent[0])
		cond := fmt.Sprintf(" BP.BLOCK_NAME %s %s", op, placeholder("block_name"))
		where += addCond(where, cond)
		args = append(args, val)
	}
	// get SQL statement from static area
	stm := getSQL("blockparent")
	// use generic query API to fetch the results from DB
	return executeAll(stm+where, args...)
}

// blocksummaries API
func (API) BlockSummaries(params Record) []Record {
	// variables we'll use in where clause
	var stm, where_clause string
	var args []interface{}

	block_join := fmt.Sprintf("JOIN %s.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID", DBOWNER)
	dataset_join := fmt.Sprintf("JOIN %s.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID", DBOWNER)

	// parse dataset argument
	block := getValues(params, "block_name")
	genSQL := ""
	if len(block) > 0 {
		block_clause := "BS.BLOCK_NAME IN (SELECT TOKEN FROM TOKEN_GENERATOR) "
		where_clause = "WHERE block_clause"
		var vals []string
		genSQL, vals = tokens(block)
		fmt.Println("tokens", block, genSQL, vals)
		for _, d := range vals {
			args = append(args, d, d, d) // append three values since tokens generates placeholders for them
		}
		stm = getSQL("blocksummaries4block")
		stm = strings.Replace(stm, "block_join", block_join, -1)
		stm = strings.Replace(stm, "where_clause", where_clause, -1)
		stm = strings.Replace(stm, "block_clause", block_clause, -1)
	}
	dataset := getValues(params, "dataset")
	if len(dataset) > 1 {
		panic("Unsupported list of dataset")
	} else if len(dataset) == 1 {
		_, val := opVal(dataset[0])
		args = append(args, val)
		where_clause = fmt.Sprintf("WHERE DS.dataset=%s", placeholder("dataset"))
		stm = getSQL("blocksummaries4dataset")
		stm = strings.Replace(stm, "block_join", block_join, -1)
		stm = strings.Replace(stm, "dataset_join", dataset_join, -1)
		stm = strings.Replace(stm, "where_clause", where_clause, -1)
	}
	// use generic query API to fetch the results from DB
	return executeAll(genSQL+stm, args...)
}
