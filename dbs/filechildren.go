package dbs

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

// FileChildren API
func (API) FileChildren(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	if len(params) == 0 {
		msg := "logical_file_name, block_id or block_name is required for fileparents api"
		return 0, errors.New(msg)
	}

	blocks := getValues(params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	}

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("filechildren", tmpl)
	if err != nil {
		return 0, err
	}

	lfns := getValues(params, "logical_file_name")
	if len(lfns) == 1 {
		conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
	} else {
		token, binds := TokenGenerator(lfns, 30, "lfn_token")
		stm = fmt.Sprintf("%s %s", token, stm)
		cond := " F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
		conds = append(conds, cond)
		for _, v := range binds {
			args = append(args, v)
		}
	}
	/*
		if len(lfns) > 1 {
			token, binds := TokenGenerator(lfns, 30, "lfn_token")
			stm = fmt.Sprintf("%s %s", token, stm)
			cond := " F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
			conds = append(conds, cond)
			for _, v := range binds {
				args = append(args, v)
			}
		} else if len(lfns) == 1 {
			if strings.Contains(lfns[0], "[") || strings.Contains(lfns[0], "'") || strings.Contains(lfns[0], ",") {
				rrr := strings.Replace(lfns[0], "[", "", -1)
				rrr = strings.Replace(rrr, "]", "", -1)
				rrr = strings.Replace(rrr, "'", "", -1)
				rrr = strings.Replace(rrr, " ", "", -1)
				token, binds := TokenGenerator(strings.Split(rrr, ","), 30, "lfn_token")
				stm = fmt.Sprintf("%s %s", token, stm)
				cond := " F.LOGICAL_FILE_NAME in (SELECT TOKEN FROM TOKEN_GENERATOR)"
				conds = append(conds, cond)
				for _, v := range binds {
					args = append(args, v)
				}
			} else {
				conds, args = AddParam("logical_file_name", "F.LOGICAL_FILE_NAME", params, conds, args)
			}
		}
	*/

	bid := getValues(params, "block_id")
	if len(bid) == 1 {
		conds, args = AddParam("block_id", "F.BLOCK_ID", params, conds, args)
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertFileChildren DBS API
func (API) InsertFileChildren(r io.Reader, cby string) error {
	return nil
}
