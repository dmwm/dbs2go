package dbs

// InsertFileArray DBS API
func (API) InsertFileArray(values Record) error {
	return InsertData("insert_file_array", values)
}
