package dbs

// InsertFileParentByLumis DBS API
func (API) InsertFileParentByLumis(values Record) error {
	return InsertData("insert_file_parent_by_lumis", values)
}