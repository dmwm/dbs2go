package dbs

// BlockChildren DBS API
func (a *API) BlockChildren() error {
	// variables we'll use in where clause
	var args []interface{}
	var conds []string

	conds, args = AddParam("block_name", "BP.BLOCK_NAME", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("blockchildren")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err := executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.blockchildren.BlockChildren")
	}
	return nil
}
