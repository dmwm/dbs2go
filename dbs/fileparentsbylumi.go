package dbs

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// FileParentsByLumi DBS API
func (API) FileParentsByLumi(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["ChildLfnList"] = false

	blockNames := getValues(params, "block_name")
	if len(blockNames) == 0 {
		return 0, errors.New("Missing block_name for listFileParentssByLumi")
	}
	blk := blockNames[0]
	dataset := strings.Split(blk, "#")[0]
	args = append(args, dataset)
	args = append(args, blk)

	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		tmpl["ChildLfnList"] = true
		token, binds := TokenGenerator(lfns, 300, "lfn_token") // 100 is max for # of allowed entries
		tmpl["LfnTokenGenerator"] = token
		for _, v := range binds {
			args = append(args, v)
		}
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("fileparentsbylumi", tmpl)
	if err != nil {
		return 0, err
	}
	stm = WhereClause(stm, conds)

	// fix binding variables
	for k, v := range params {
		key := fmt.Sprintf(":%s", strings.ToLower(k))
		if strings.Contains(stm, key) {
			stm = strings.Replace(stm, key, "?", -1)
			args = append(args, v)
		}
	}

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileParentsByLumi DBS API
func (API) InsertFileParentsByLumi(r io.Reader, cby string) error {
	return nil
}
