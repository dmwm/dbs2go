package dbs

import "log"

// dummy API
func (a API) Dummy() []Record {
	datasets := getValues(a.Params, "dataset")
	log.Printf("input args: %+v, datasets: %+v", a.Params, datasets)
	var out []Record
	rec := make(Record)
	rec["foo"] = 1
	out = append(out, rec)
	return out
}
