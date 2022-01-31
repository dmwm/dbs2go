package dbs

import (
	"fmt"
	"strings"
)

// FileParentsByLumi DBS API
func (a *API) FileParentsByLumi() error {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["ChildLfnList"] = false
	tmpl["TokenCondition"] = TokenCondition()

	blockNames := getValues(a.Params, "block_name")
	if len(blockNames) == 0 {
		msg := "Missing block_name for listFileParentssByLumi"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.fileparentsbylumi.FileParentsByLumi")
	}
	blk := blockNames[0]
	dataset := strings.Split(blk, "#")[0]
	args = append(args, dataset)
	args = append(args, blk)

	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) > 1 {
		tmpl["ChildLfnList"] = true
		token, binds := TokenGenerator(lfns, 30, "lfn_token") // 100 is max for # of allowed entries
		tmpl["LfnTokenGenerator"] = token
		for _, v := range binds {
			args = append(args, v)
		}
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("fileparentsbylumi", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.fileparentsbylumi.FileParentsByLumi")
	}
	stm = WhereClause(stm, conds)

	// fix binding variables
	for k, v := range a.Params {
		key := fmt.Sprintf(":%s", strings.ToLower(k))
		if strings.Contains(stm, key) {
			stm = strings.Replace(stm, key, "?", -1)
			args = append(args, v)
		}
	}

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.fileparentsbylumi.FileParentsByLumi")
	}
	return nil
}

// InsertFileParentsByLumi DBS API
func (a *API) InsertFileParentsByLumi() error {
	return nil
}
