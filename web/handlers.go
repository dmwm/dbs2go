package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/vkuznet/dbs2go/dbs"
)

// LoggingHandlerFunc declares new handler function type which
// should return status (int) and error
type LoggingHandlerFunc func(w http.ResponseWriter, r *http.Request) (int, int64, error)

// LoggingHandler provides wrapper for any passed handler
// function. It executed given function and log its status and error
// to common logger
func LoggingHandler(h LoggingHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			atomic.AddUint64(&TotalPostRequests, 1)
		} else if r.Method == "GET" {
			atomic.AddUint64(&TotalGetRequests, 1)
		}
		start := time.Now()
		status, dataSize, err := h(w, r)
		if err != nil {
			log.Println("ERROR", err)
		}
		tstamp := int64(start.UnixNano() / 1000000) // use milliseconds for MONIT
		logRequest(w, r, start, status, tstamp, dataSize)
	}
}

// MetricsHandler provides metrics
func MetricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(promMetrics()))
	return
}

// DummyHandler provides example how to write GET/POST handler
func DummyHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	// example of handling POST request
	if r.Method == "POST" {
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		rec := make(dbs.Record)
		status := http.StatusOK
		err := decoder.Decode(&rec)
		if err != nil {
			status = http.StatusInternalServerError
		}
		return status, 0, err
	}

	// example of handling GET request
	status := http.StatusOK
	params := make(dbs.Record)
	log.Printf("http request %+v", r)
	for k, v := range r.URL.Query() {
		params[k] = v
	}
	var api dbs.API
	records := api.Dummy(params)
	data, err := json.Marshal(records)
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	w.WriteHeader(status)
	w.Write(data)
	size := int64(binary.Size(data))
	return status, size, nil
}

// StatusHandler provides basic functionality of status response
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	var records []dbs.Record
	rec := make(dbs.Record)
	rec["status"] = http.StatusOK
	records = append(records, rec)
	data, err := json.Marshal(records)
	if err != nil {
		log.Fatalf("Fail to marshal records, %v", err)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// DBSPostHandler
func DBSPostHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	status := http.StatusOK
	params := make(dbs.Record)
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	err = json.Unmarshal(data, &params)
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	var api dbs.API
	if a == "datatiers" {
		err = api.InsertDataTiers(params)
	}
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	return status, 0, nil
}

// DBSGetHandler
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	status := http.StatusOK
	params := make(dbs.Record)
	for k, v := range r.URL.Query() {
		params[k] = v
	}
	var api dbs.API
	var err error
	var size int64
	if a == "datatiers" {
		size, err = api.DataTiers(params, w)
	} else if a == "datasets" {
		size, err = api.Datasets(params, w)
	} else if a == "blocks" {
		size, err = api.Blocks(params, w)
	} else if a == "files" {
		size, err = api.Files(params, w)
	} else {
		err = errors.New(fmt.Sprintf("not implemented API %s", api))
	}
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	return status, size, nil
}

// DatatiersHandler
func DatatiersHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	if r.Method == "POST" {
		return DBSPostHandler(w, r, "datatiers")
	}
	return DBSGetHandler(w, r, "datatiers")
}

// DatasetsHandler
func DatasetsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasets")
}

// BlocksHandler
func BlocksHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blocks")
}

// BlockChildrenHandler
func BlockChildrenHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blockchildren")
}

// BlockSummariesHandler
func BlockSummariesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blocksummaries")
}

// BlockOriginHandler
func BlockOriginHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "blockorigin")
}

// FilesHandler
func FilesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "files")
}

// FileChildrenHandler
func FileChildrenHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "filechildren")
}

// FilePArentHandler
func FileParentHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "fileparent")
}

// FileSummariesHandler
func FileSummariesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "filesummaries")
}

// RunsHandler
func RunsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "runs")
}

// RunSummariesHandler
func RunSummariesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "runsummaries")
}

//ProcessingErasHandler
func ProcessingErasHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "processingeras")
}

// PrimarydstypesHandler
func PrimarydstypesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "primarydstypes")
}

// DataTypesHandler
func DataTypesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datatypes")
}

// ReleaseVersionsHandler
func ReleaseVersionsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "releaseversions")
}

// AcquisitionErasHandler
func AcquisitionErasHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "acquisitioneras")
}

// PrimaryDatasetHandler
func PrimaryDatasetsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "primarydatasets")
}

// DatasetParentHandler
func DatasetParentHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "dataasetparent")
}

// DatasetChildrenHandler
func DatasetChildrenHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasetchildren")
}

// DatasetAccessTypesHandler
func DatasetAccessTypesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "datasetaccesstypes")
}

// PhysicsGroupsHandler
func PhysicsGroupsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "physicsgroups")
}

// OutputConfigsHandler
func OutputConfigsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "outputconfigs")
}

// POST APIs

func BlockParentHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSPostHandler(w, r, "blockparent")
}
func FileLumisHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSPostHandler(w, r, "filelumis")
}
