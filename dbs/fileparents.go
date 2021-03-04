package dbs

import (
	"fmt"
	"net/http"
)

// FileParents API
func (API) FileParents(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	if len(params) == 0 {
		msg := "logical_file_name, block_id or block_name is required for fileparents api"
		return dbsError(w, msg)
	}

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	blocks := getValues(params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	}

	stm, err := LoadTemplateSQL("fileparent", tmpl)
	if err != nil {
		return 0, err
	}

	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		token, binds := TokenGenerator(lfns, 200, "lfn_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := " F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileParents DBS API
func (API) InsertFileParents(values Record) error {
	// TODO: implement the following logic
	// /Users/vk/CMS/DMWM/GIT/DBS/Server/Python/src/dbs/business/DBSFile.py
	/*
	   input block_name: is a child block name.
	   input chils_parent_id_list: is a list of file id of child, parent  pair: [[cid1, pid1],[cid2,pid2],[cid3,pid3],...]
	   The requirment for this API is
	   1. All the child files belong to the block.
	   2. All the child-parent pairs are not already in DBS.
	   3. The dataset parentage is already in DBS.
	*/
	return nil
}
