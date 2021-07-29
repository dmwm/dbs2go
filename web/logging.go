package web

// logging module provides various logging methods
//
// Copyright (c) 2020 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
)

// LogRecord represents data we can send to StompAMQ or HTTP endpoint
type LogRecord struct {
	Method         string  `json:"method"`           // http.Request HTTP method
	URI            string  `json:"uri"`              // http.RequestURI
	API            string  `json:"api"`              // http service API being used
	System         string  `json:"system"`           // cmsweb service name
	ClientIP       string  `json:"clientip"`         // client IP address
	BytesIn        int64   `json:"bytes_in"`         // number of bytes send with HTTP request
	BytesOut       int64   `json:"bytes_out"`        // number of bytes received with HTTP request
	Proto          string  `json:"proto"`            // http.Request protocol
	Status         int64   `json:"status"`           // http.Request status code
	ContentLength  int64   `json:"content_length"`   // http.Request content-length
	Referer        string  `json:"referer"`          // http referer
	UserAgent      string  `json:"user_agent"`       // http user-agent field
	XForwardedHost string  `json:"x_forwarded_host"` // http.Request X-Forwarded-Host
	XForwardedFor  string  `json:"x_forwarded_for"`  // http.Request X-Forwarded-For
	RemoteAddr     string  `json:"remote_addr"`      // http.Request remote address
	RequestTime    float64 `json:"request_time"`     // http request time
	Timestamp      int64   `json:"timestamp"`        // record timestamp
}

// HTTPRecord provides http record we send to logs endpoint
type HTTPRecord struct {
	Producer  string    `json:"producer"`  // name of the producer
	Type      string    `json:"type"`      // type of metric
	Timestamp int64     `json:"timestamp"` // UTC milliseconds
	Host      string    `json:"host"`      // used to add extra information about the node submitting your data
	Data      LogRecord `json:"data"`      // log record data
}

// helper function to produce UTC time prefixed output
func utcMsg(data []byte) string {
	var msg string
	if Config.UTC {
		msg = fmt.Sprintf("[" + time.Now().UTC().String() + "] " + string(data))
	} else {
		msg = fmt.Sprintf("[" + time.Now().String() + "] " + string(data))
		//     msg = fmt.Sprintf("[" + time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " UTC] " + string(data))
	}
	return msg
}

// custom rotate logger
type rotateLogWriter struct {
	RotateLogs *rotatelogs.RotateLogs
}

func (w rotateLogWriter) Write(data []byte) (int, error) {
	return w.RotateLogs.Write([]byte(utcMsg(data)))
}

// custom logger
type logWriter struct {
}

func (writer logWriter) Write(data []byte) (int, error) {
	return fmt.Print(utcMsg(data))
}

type (
	// struct for holding response details
	responseData struct {
		status int
		size   int
	}

	// our http.ResponseWriter implementation
	loggingResponseWriter struct {
		http.ResponseWriter // compose original http.ResponseWriter
		responseData        *responseData
	}
)

func (r *loggingResponseWriter) Write(b []byte) (int, error) {
	size, err := r.ResponseWriter.Write(b) // write response using original http.ResponseWriter
	r.responseData.size += size            // capture size
	return size, err
}

func (r *loggingResponseWriter) WriteHeader(statusCode int) {
	r.ResponseWriter.WriteHeader(statusCode) // write status code using original http.ResponseWriter
	r.responseData.status = statusCode       // capture status code
}

func loggingMiddleware(h http.Handler) http.Handler {
	loggingFn := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tstamp := int64(start.UnixNano() / 1000000) // use milliseconds for MONIT

		// initialize response data struct
		responseData := &responseData{
			status: http.StatusOK,
			size:   0,
		}
		lrw := loggingResponseWriter{
			ResponseWriter: w, // compose original http.ResponseWriter
			responseData:   responseData,
		}
		h.ServeHTTP(&lrw, r) // inject our implementation of http.ResponseWriter
		logRequest(w, r, start, responseData.status, tstamp, int64(responseData.size))

	}
	return http.HandlerFunc(loggingFn)
}

// helper function to log every single user request, here we pass pointer to status code
// as it may change through the handler while we use defer logRequest
func logRequest(w http.ResponseWriter, r *http.Request, start time.Time, status int, tstamp int64, bytesOut int64) {
	dataMsg := fmt.Sprintf("[data: %v in %v out]", r.ContentLength, bytesOut)
	referer := r.Referer()
	if referer == "" {
		referer = "-"
	}
	var clientip string
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		clientip = strings.Split(xff, ":")[0]
	} else if r.RemoteAddr != "" {
		clientip = strings.Split(r.RemoteAddr, ":")[0]
	}
	addr := r.RemoteAddr
	refMsg := fmt.Sprintf("[ref: \"%s\" \"%v\"]", referer, r.Header.Get("User-Agent"))
	respMsg := fmt.Sprintf("[req: %v]", time.Since(start))
	uri, err := url.QueryUnescape(r.RequestURI)
	if err != nil {
		log.Println("unable to unescape request uri", err)
		uri = r.RequestURI
	}
	log.Printf("%s %d %s %s %s %s %s %s\n", r.Proto, status, addr, r.Method, uri, dataMsg, refMsg, respMsg)
	rec := LogRecord{
		Method:         r.Method,
		URI:            uri,
		API:            getAPI(r.RequestURI),
		System:         getSystem(r.RequestURI),
		BytesIn:        r.ContentLength,
		BytesOut:       bytesOut,
		Proto:          r.Proto,
		Status:         int64(status),
		ContentLength:  r.ContentLength,
		Referer:        referer,
		UserAgent:      r.Header.Get("User-Agent"),
		XForwardedHost: r.Header.Get("X-Forwarded-Host"),
		XForwardedFor:  xff,
		ClientIP:       clientip,
		RemoteAddr:     r.RemoteAddr,
		RequestTime:    time.Since(start).Seconds(),
		Timestamp:      tstamp,
	}
	if Config.PrintMonitRecord {
		data, err := monitRecord(rec)
		if err == nil {
			fmt.Println(string(data))
		} else {
			log.Println("unable to produce record for MONIT, error", err)
		}
	}
}

// helper function to extract service API from the record URI
func getAPI(uri string) string {
	// /httpgo?test=bla
	arr := strings.Split(uri, "/")
	last := arr[len(arr)-1]
	arr = strings.Split(last, "?")
	return arr[0]
}

// helper function to extract service system from the record URI
func getSystem(uri string) string {
	// /httpgo?test=bla
	arr := strings.Split(uri, "/")
	system := "base"
	if len(arr) > 0 {
		if len(arr) > 1 {
			arr = strings.Split(arr[1], "?")
		}
		system = arr[0]
	}
	if system == "" {
		system = "base"
	}
	return system
}

// helper function to prepare record for MONIT
func monitRecord(rec LogRecord) ([]byte, error) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Println("Unable to get hostname", err)
	}
	ltype := "auth"      // defined by MONIT team
	producer := "cmsweb" // defined by MONIT team
	r := HTTPRecord{
		Producer:  producer,
		Type:      ltype,
		Timestamp: rec.Timestamp,
		Host:      hostname,
		Data:      rec,
	}
	data, err := json.Marshal(r)
	return data, err
}
