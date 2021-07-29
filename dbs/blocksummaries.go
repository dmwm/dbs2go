package dbs

import (
	"errors"
	"net/http"
	"strings"
)

// BlockSummaries DBS API
func (API) BlockSummaries(params Record, w http.ResponseWriter) error {
	var stm string
	var args []interface{}
	var err error
	tmpl := make(Record)
	tmpl["TokenCondition"] = TokenCondition()
	tmpl["Owner"] = DBOWNER

	if len(params) == 0 {
		msg := "block_name or dataset is required for blocksummaries api"
		return errors.New(msg)
	}

	// parse arguments
	_, detailErr := getSingleValue(params, "detail")
	block := getValues(params, "block_name")
	genSQL := ""
	if len(block) > 0 {
		blk := block[0]
		if strings.Contains(blk, "*") {
			msg := "wild-card block value is not allowed"
			return errors.New(msg)
		}
		var blocks []string
		if strings.Contains(blk, "[") {
			// convert input to list of blocks
			blk = strings.Replace(blk, "[", "", -1)
			blk = strings.Replace(blk, "]", "", -1)
			blk = strings.Replace(blk, "'", "", -1)
			for _, v := range strings.Split(blk, ",") {
				blocks = append(blocks, strings.Trim(v, " "))
			}
		} else {
			blocks = append(blocks, blk)
		}
		var binds []string
		genSQL, binds = TokenGenerator(blocks, 100, "block_token") // 100 is max for # of allowed datasets
		for _, v := range binds {
			args = append(args, v)
		}
		if detailErr == nil { // no details are required
			stm, err = LoadTemplateSQL("blocksummaries4block", tmpl)
		} else {
			stm, err = LoadTemplateSQL("blocksummaries4block_detail", tmpl)
		}
		if err != nil {
			return err
		}
	}
	dataset := getValues(params, "dataset")
	if len(dataset) > 1 {
		msg := "Unsupported list of dataset"
		return errors.New(msg)
	} else if len(dataset) == 1 {
		if strings.Contains(dataset[0], "*") {
			msg := "wild-card dataset value is not allowed"
			return errors.New(msg)
		}
		_, val := OperatorValue(dataset[0])
		if detailErr == nil {
			stm, err = LoadTemplateSQL("blocksummaries4dataset", tmpl)
			// blocksummaries4dataset contains three dataset bindings
			args = append(args, val)
			args = append(args, val)
			args = append(args, val)
		} else {
			stm, err = LoadTemplateSQL("blocksummaries4dataset_detail", tmpl)
			args = append(args, val)
		}
		if err != nil {
			return err
		}
	}
	// use generic query API to fetch the results from DB
	return executeAll(w, genSQL+stm, args...)
}
