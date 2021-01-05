package dbs

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// BlockSummaries DBS API
func (API) BlockSummaries(params Record, w http.ResponseWriter) (int64, error) {
	// variables we'll use in where clause
	var stm, where_clause string
	var args []interface{}

	block_join := fmt.Sprintf("JOIN %s.BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID", DBOWNER)
	dataset_join := fmt.Sprintf("JOIN %s.DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID", DBOWNER)

	// parse arguments
	_, detailErr := getSingleValue(params, "detail")
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
		if detailErr == nil { // no details are required
			stm = getSQL("blocksummaries4block")
			stm = strings.Replace(stm, "block_join", block_join, -1)
			stm = strings.Replace(stm, "where_clause", where_clause, -1)
			stm = strings.Replace(stm, "block_clause", block_clause, -1)
		} else {
			stm = getSQL("blocksummaries4block_detail")
		}
	}
	dataset := getValues(params, "dataset")
	if len(dataset) > 1 {
		msg := "Unsupported list of dataset"
		return 0, errors.New(msg)
	} else if len(dataset) == 1 {
		_, val := OperatorValue(dataset[0])
		args = append(args, val)
		where_clause = fmt.Sprintf("WHERE DS.dataset=%s", placeholder("dataset"))
		if detailErr == nil {
			stm = getSQL("blocksummaries4dataset")
			stm = strings.Replace(stm, "block_join", block_join, -1)
			stm = strings.Replace(stm, "dataset_join", dataset_join, -1)
			stm = strings.Replace(stm, "where_clause", where_clause, -1)
		} else {
			stm = getSQL("blocksummaries4dataset_detail")
		}
	}
	// use generic query API to fetch the results from DB
	return executeAll(w, genSQL+stm, args...)
}
