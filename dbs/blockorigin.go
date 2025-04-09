package dbs

// BlockOrigin DBS API
func (a *API) BlockOrigin() error {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	// parse given parameters
	site := getValues(a.Params, "origin_site_name")
	if len(site) > 1 {
		msg := "Unsupported list of sites"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.blockorigin.BlockOrigin")
	} else if len(site) == 1 {
		conds, args = AddParam("origin_site_name", "B.ORIGIN_SITE_NAME", a.Params, conds, args)
	}
	block := getValues(a.Params, "block_name")
	if len(block) > 1 {
		msg := "Unsupported list of block"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.blockorigin.BlockOrigin")
	} else if len(block) == 1 {
		conds, args = AddParam("block_name", "B.BLOCK_NAME", a.Params, conds, args)
	}
	dataset := getValues(a.Params, "dataset")
	if len(dataset) > 1 {
		msg := "Unsupported list of dataset"
		return Error(InvalidParamErr, InvalidParameterErrorCode, msg, "dbs.blockorigin.BlockOrigin")
	} else if len(dataset) == 1 {
		conds, args = AddParam("dataset", "DS.DATASET", a.Params, conds, args)
	}

	// get SQL statement from static area
	stm := getSQL("blockorigin")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "unable to query block origin", "dbs.blockorigin.BlockOrigin")
	}
	return nil
}
