package dbs

// InsertFileArray DBS API
func (API) InsertFileArray(values Record) error {
	return InsertValues("insert_file_array", values)
}
