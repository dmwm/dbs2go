package dbs

import "log"

// dummy API
func (API) Dummy(params Record) []Record {
	datasets := getValues(params, "dataset")
	log.Printf("input args: %+v, datasets: %+v", params, datasets)
	var out []Record
	rec := make(Record)
	rec["foo"] = 1
	out = append(out, rec)
	return out
}
