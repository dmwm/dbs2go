package dbs

// InsertDatasetList DBS API
func (API) InsertDatasetList(values Record) error {
	return InsertData("insert_dataset_list", values)
}
