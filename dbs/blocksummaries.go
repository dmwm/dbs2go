package dbs

import (
	"errors"
	"net/http"
)

// BlockSummaries DBS API
func (API) BlockSummaries(params Record, w http.ResponseWriter) (int64, error) {
	var stm string
	var args []interface{}

	// parse arguments
	_, detailErr := getSingleValue(params, "detail")
	block := getValues(params, "block_name")
	genSQL := ""
	if len(block) > 0 {
		var binds []string
		genSQL, binds = TokenGenerator(block, 100, "block_token") // 100 is max for # of allowed datasets
		for _, v := range binds {
			args = append(args, v)
		}
		if detailErr == nil { // no details are required
			stm = getSQL("blocksummaries4block")
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
		if detailErr == nil {
			stm = getSQL("blocksummaries4dataset")
			// blocksummaries4dataset contains three dataset bindings
			args = append(args, val)
			args = append(args, val)
			args = append(args, val)
		} else {
			stm = getSQL("blocksummaries4dataset_detail")
		}
	}
	// use generic query API to fetch the results from DB
	return executeAll(w, genSQL+stm, args...)
}
