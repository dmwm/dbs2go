package dbs

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

// FileLumis API
func (API) FileLumis(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	tmpl["Lfn"] = false
	tmpl["LfnGenerator"] = ""
	tmpl["TokenGenerator"] = ""
	tmpl["LfnList"] = false
	tmpl["ValidFileOnly"] = 0
	tmpl["BlockName"] = false
	tmpl["Migration"] = false

	lfns := getValues(params, "logical_file_name")
	if len(lfns) > 1 {
		token, binds := TokenGenerator(lfns, 100, "lfns_token") // 100 is max for # of allowed entries
		tmpl["LfnGenerator"] = token
		tmpl["Lfn"] = true
		tmpl["LfnList"] = true
		conds = append(conds, token)
		for _, v := range binds {
			args = append(args, v)
		}
	} else if len(lfns) == 1 {
		tmpl["Lfn"] = true
		tmpl["LfnList"] = false
		args = append(args, lfns[0])
	}

	validFileOnly := getValues(params, "validFileOnly")
	if len(validFileOnly) == 1 {
		tmpl["ValidFileOnly"] = 1
	}

	blocks := getValues(params, "block_name")
	if len(blocks) == 1 {
		tmpl["BlockName"] = true
		conds, args = AddParam("block_name", "B.BLOCK_NAME", params, conds, args)
	}

	stm, err := LoadTemplateSQL("filelumis", tmpl)
	log.Println("### stm", stm)
	if err != nil {
		return 0, err
	}

	// generate run_num token
	runs, err := ParseRuns(getValues(params, "run_num"))
	if err != nil {
		return 0, err
	}
	if len(runs) > 0 {
		token, condRuns, bindsRuns := runsClause("FL", runs)
		log.Println("### FileLumis", token, condRuns, bindsRuns)
		stm = fmt.Sprintf("%s %s", token, stm)
		conds = append(conds, condRuns)
		for _, v := range bindsRuns {
			args = append(args, v)
		}
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

// InsertFileLumis DBS API
func (API) InsertFileLumis(params Record) error {
	if _, ok := params["event_count"]; ok {
		return InsertValues("insert_filelumi", params)
	}
	return InsertValues("insert_filelumi2", params)
}
