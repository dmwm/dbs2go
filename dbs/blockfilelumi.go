package dbs

import (
	"fmt"
)

// BlockFileLumiIds API
func (a *API) BlockFileLumiIds() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["ChildLfnList"] = false

	// create our SQL statement
	stm, err := LoadTemplateSQL("blockfilelumiids", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.blockfilelumi.BlockFileLumiIds")
	}

	// add block condition
	if v, _ := getSingleValue(a.Params, "block_name"); v != "" {
		args = append(args, v)
	}

	// add child_lfn_list condition
	lfns := getValues(a.Params, "child_lfn_list")
	if len(lfns) > 1 {
		tmpl["ChildLfnList"] = true
		token, binds := TokenGenerator(lfns, 30, "lfn_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := fmt.Sprintf(" F.LOGICAL_FILE_NAME in %s", TokenCondition())
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	}
	// add conditions
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.blockfilelumi.BlockFileLumiIds")
	}
	return nil
}
