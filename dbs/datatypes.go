package dbs

// DataTypes DBS API
func (a API) DataTypes() error {
	var args []interface{}
	var conds []string

	conds, args = AddParam("datatype", "DT.DATATYPE", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("datatypes")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// InsertDataTypes DBS API
func (a API) InsertDataTypes() error {
	//     return InsertValues("insert_data_types", values)
	return nil
}
