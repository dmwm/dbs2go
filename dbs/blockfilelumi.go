package dbs

import (
	"fmt"
	"net/http"
)

// BlockFileLumiIds API
func (API) BlockFileLumiIds(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["ChildLfnList"] = false

	// create our SQL statement
	stm, err := LoadTemplateSQL("blockfilelumiids", tmpl)
	if err != nil {
		return 0, err
	}

	// add block condition
	if v, _ := getSingleValue(params, "block_name"); v != "" {
		args = append(args, v)
	}

	// add child_lfn_list condition
	lfns := getValues(params, "child_lfn_list")
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

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)

	/*
		// if we extract explicitly all info from rows then this API
		// will be very RAM hungry since it needs all results to construct
		// output dict
		// it should return {fileID: [(run, lumi), ...]}

		// execute SQL statement
		stm = CleanStatement(stm)
		if DRYRUN {
			utils.PrintSQL(stm, args, "")
			return 0, nil
		}
		if utils.VERBOSE > 1 {
			utils.PrintSQL(stm, args, "execute")
		}
		var size int64

		// execute transaction
		tx, err := DB.Begin()
		if err != nil {
			msg := fmt.Sprintf("unable to get DB transaction %v", err)
			return 0, errors.New(msg)
		}
		defer tx.Rollback()
		rows, err := tx.Query(stm, args...)
		if err != nil {
			msg := fmt.Sprintf("unable to query statement:\n%v\nerror=%v", stm, err)
			return 0, errors.New(msg)
		}
		defer rows.Close()

		// extract results from returned DB rows
		rmap := make(map[])
		var runNumber, lumiSectionNumber, fileID int64
		for rows.Next() {
			err := rows.Scan(&runNumber, &lumiSectionNumber, &fileID)
			if err != nil {
				return 0, err
			}
		}
		if err = rows.Err(); err != nil {
			msg := fmt.Sprintf("rows error %v", err)
			return 0, errors.New(msg)
		}
		return size, nil
	*/
}
