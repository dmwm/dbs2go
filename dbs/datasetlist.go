package dbs

// InsertDatasetList DBS API
func (API) InsertDatasetList(values Record) error {
	return InsertValues("insert_dataset_list", values)
}
