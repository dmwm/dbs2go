package web

// handlers.go - provides handlers examples for dbs2go server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/dbs"
)

// LoggingHandlerFunc declares new handler function type which
// should return status (int) and error
type LoggingHandlerFunc func(w http.ResponseWriter, r *http.Request) (int, error)

// LoggingHandler provides wrapper for any passed handler
// function. It executed given function and log its status and error
// to common logger
func LoggingHandler(h LoggingHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status, err := h(w, r)
		if err != nil {
			log.Println("ERROR", err, status)
		} else {
			log.Println("INFO", r.Host, status)
		}
	}
}

func DatatiersHandler(w http.ResponseWriter, r *http.Request) (int, error) {
	status := http.StatusOK
	var params dbs.Record
	for k, v := range r.Form {
		params[k] = v
	}
	var api dbs.API
	records := api.Datasets(params)
	data, err := json.Marshal(records)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	w.WriteHeader(status)
	w.Write(data)
	return status, nil
}
func DatasetsHandler(w http.ResponseWriter, r *http.Request) (int, error) {
	status := http.StatusOK
	w.WriteHeader(status)
	return status, nil
}
func BlocksHandler(w http.ResponseWriter, r *http.Request) (int, error) {
	status := http.StatusOK
	w.WriteHeader(status)
	return status, nil
}
func FilesHandler(w http.ResponseWriter, r *http.Request) (int, error) {
	status := http.StatusOK
	w.WriteHeader(status)
	return status, nil
}
