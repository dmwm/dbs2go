package dbs

import (
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// ParentDatasetFileLumiIds API
func (a *API) ParentDatasetFileLumiIds() error {
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	// create our SQL statement
	stm, err := LoadTemplateSQL("parentdatasetfilelumiids", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "unable to load parentdatasetfilelumiids sql template", "dbs.parentdatasetfilelumi.ParentDatasetFileLumiIds")
	}

	// add dataset condition
	conds, args = AddParam("dataset", "D.DATASET", a.Params, conds, args)

	// NOTE: the parentdatasetfilelumiids is already contains :dataset
	// binding clause, therefore we don't need to add where condition

	// add where clause
	//     stm = WhereClause(stm, conds)

	stm = CleanStatement(stm)
	if utils.VERBOSE > 0 {
		utils.PrintSQL(stm, args, "execute")
		log.Println("conds", conds)
	}

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query parent dataset filelumis", "dbs.parentdatasetfilelumi.ParentDatasetFileLumiIds")
	}
	return nil
}
