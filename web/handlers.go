package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
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
		start := time.Now()
		status, dataSize, err := h(w, r)
		if err != nil {
			log.Println("ERROR", err)
		}
		tstamp := int64(start.UnixNano() / 1000000) // use milliseconds for MONIT
		logRequest(w, r, start, status, tstamp, dataSize)
	}
}

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
	var params dbs.Record
	for k, v := range r.Form {
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

// DatatiersHandler
func DatatiersHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	status := http.StatusOK
	var params dbs.Record
	for k, v := range r.Form {
		params[k] = v
	}
	var api dbs.API
	records := api.DataTiers(params)
	data, err := json.Marshal(records)
	if err != nil {
		return http.StatusInternalServerError, 0, err
	}
	w.WriteHeader(status)
	w.Write(data)
	size := int64(binary.Size(data))
	return status, size, nil
}

// DatasetsHandler
func DatasetsHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	status := http.StatusOK
	w.WriteHeader(status)
	return status, 0, nil
}

// BlocksHandler
func BlocksHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	status := http.StatusOK
	w.WriteHeader(status)
	return status, 0, nil
}

// FilesHandler
func FilesHandler(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	status := http.StatusOK
	w.WriteHeader(status)
	return status, 0, nil
}
