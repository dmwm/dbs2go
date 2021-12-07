package utils

import (
	"log"
	"net/http"
)

// StdoutWriter provides the same functionality as http.ResponseWriter
// to cover unit tests of DBS APIs. It prints given data directly to stdout.
type StdoutWriter string

// Header implements Header() API of http.ResponseWriter interface
func (s StdoutWriter) Header() http.Header {
	return http.Header{}
}

// Write implements Write API of http.ResponseWriter interface
func (s StdoutWriter) Write(b []byte) (int, error) {
	v := string(b)
	log.Println(v)
	return len(v), nil
}

// WriteHeader implements WriteHeader API of http.ResponseWriter interface
func (s StdoutWriter) WriteHeader(statusCode int) {
	log.Println("statusCode", statusCode)
}
