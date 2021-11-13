package dbs

import (
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// Dummy API
func (a *API) Dummy() []Record {
	datasets := getValues(a.Params, "dataset")
	if utils.VERBOSE > 0 {
		log.Printf("input args: %+v, datasets: %+v", a.Params, datasets)
	}
	var out []Record
	rec := make(Record)
	rec["foo"] = 1
	out = append(out, rec)
	return out
}
