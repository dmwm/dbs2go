package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

// DBSGetHandler
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) (int, int64, error) {
	status := http.StatusOK
	params := make(dbs.Record)
	for k, v := range r.URL.Query() {
		params[k] = v
	}
	var api dbs.API
	var err error
	if a == "datatiers" {
		err = api.DataTiers(params, w)
	} else if a == "datasets" {
		err = api.Datasets(params, w)
	} else if a == "blocks" {
		err = api.Blocks(params, w)
	} else if a == "files" {
		err = api.Files(params, w)
	} else {
		err = errors.New(fmt.Sprintf("not implemented API %s", api))
	}
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	var size int64
	return status, size, nil
}

// DatatiersHandler
func DatatiersHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
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

// FilesHandler
func FilesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	return DBSGetHandler(w, r, "files")
}
