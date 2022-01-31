package dbs

import (
	"fmt"
)

// FileChildren API
func (a *API) FileChildren() error {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	if len(a.Params) == 0 {
		msg := "logical_file_name, block_id or block_name is required for fileparents api"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.filechildren.FileChildren")
	}

	blocks := getValues(a.Params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("filechildren", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.filechildren.FileChildren")
	}

	lfns := getValues(a.Params, "logical_file_name")
	if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", a.Params, conds, args)
	} else {
		token, binds := TokenGenerator(lfns, 30, "lfn_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME in %s", TokenCondition())
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	}

	bid := getValues(a.Params, "block_id")
	if len(bid) == 1 {
		conds, args = AddParam("block_id", "F.BLOCK_ID", a.Params, conds, args)
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.filechildren.FileChildren")
	}
	return nil
}

// InsertFileChildren DBS API
func (a *API) InsertFileChildren() error {
	return nil
}
