package dbs

// AcquisitionErasCi DBS API
func (a *API) AcquisitionErasCi() error {
	// variables we'll use in where clause
	var args []interface{}
	var conds, preSession, postSession []string

	// parse dataset argument
	acquisitioneras := getValues(a.Params, "acquisitionEra")
	if len(acquisitioneras) == 1 {
		conds, args = AddParam("acquisitionEra", "AE.ACQUISITION_ERA_NAME", a.Params, conds, args)
		preSession = append(preSession, "alter session set NLS_COMP=LINGUISTIC")
		preSession = append(preSession, "alter session set NLS_SORT=BINARY_CI")
		postSession = append(postSession, "alter session set NLS_COMP=BINARY")
		postSession = append(postSession, "alter session set NLS_SORT=BINARY")
	}

	// get SQL statement from static area
	stm := getSQL("acquisitionerasci")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	tx, err := DB.Begin()
	if err != nil {
		msg := "unable to get DB transaction"
		return Error(err, TransactionErrorCode, msg, "dbs.acquisitionerasci.AcquisitionErasCi")
	}
	defer tx.Rollback()
	if err := executeSessions(tx, preSession); err != nil {
		return Error(err, SessionErrorCode, "ORACLE session error", "dbs.acquisitionerasci.AcquisitionErasCi")
	}

	e := executeAll(a.Writer, a.Separator, stm, args...)
	if err := executeSessions(tx, postSession); err != nil {
		return Error(err, SessionErrorCode, "ORACLE session error", "dbs.acquisitionerasci.AcquisitionErasCi")
	}
	if e != nil {
		return Error(e, QueryErrorCode, "unable to get Acquisition Era", "dbs.acquisitionerasci.AcquisitionErasCi")
	}
	return nil
}
