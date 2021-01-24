package dbs

import (
	"errors"
	"fmt"
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

	blockName := getValues(params, "block_name")
	if len(blockName) == 0 {
		return 0, errors.New("Missing block_name for listFileParentssByLumi")
	}

	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		tmpl["ChildLfnList"] = true
		token, binds := TokenGenerator(lfns, 100, "lfn_token") // 100 is max for # of allowed entries
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
func (API) InsertFileParentsByLumi(values Record) error {
	return InsertValues("insert_file_parent_by_lumis", values)
}
