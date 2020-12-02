package dbs

// dummy API
func (API) Dummy(params Record) []Record {
	var out []Record
	rec := make(Record)
	rec["foo"] = 1
	out = append(out, rec)
	return out
}
