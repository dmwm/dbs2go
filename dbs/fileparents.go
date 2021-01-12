package dbs

import (
	"fmt"
	"net/http"
)

// FileParents API
func (API) FileParents(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

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
		token, binds := TokenGenerator(lfns, 100)
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
	return InsertValues("insert_file_parents", values)
}
