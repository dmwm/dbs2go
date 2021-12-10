package utils

import (
	"compress/gzip"
	"net/http"
)

// GzipWriter provides the same functionality as http.ResponseWriter
// It compresses data using compress/zip writer and provides headers
// from given http.ResponseWriter
type GzipWriter struct {
	GzipWriter *gzip.Writer
	Writer     http.ResponseWriter
}

// Header implements Header() API of http.ResponseWriter interface
func (g GzipWriter) Header() http.Header {
	return g.Writer.Header()
}

// Write implements Write API of http.ResponseWriter interface
func (g GzipWriter) Write(b []byte) (int, error) {
	return g.GzipWriter.Write(b)
}

// WriteHeader implements WriteHeader API of http.ResponseWriter interface
func (g GzipWriter) WriteHeader(statusCode int) {
	g.Writer.WriteHeader(statusCode)
}
