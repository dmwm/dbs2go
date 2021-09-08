package utils

// module to handle URL requests
// Copyright (c) 2015-2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"bytes"
	"io"
	"log"
	"net/http"

	"github.com/dmwm/cmsauth"
)

// global HTTP client
var _client = cmsauth.HttpClient()

// UrlCounter for profile output
var UrlCounter uint32

// ResponseType structure is what we expect to get for our URL call.
// It contains a request URL, the data chunk and possible error from remote
type ResponseType struct {
	Url        string // response url
	Data       []byte // response data, i.e. what we got with Body of the response
	Error      error  // http error, a non-2xx return code is not an error
	Status     string // http status string
	StatusCode int    // http status code
}

// FetchResponse fetches data for provided URL, args is a json dump of arguments
func FetchResponse(rurl string, args []byte) ResponseType {
	var response ResponseType
	response.Url = rurl
	var req *http.Request
	var e error
	if len(args) > 0 {
		req, e = http.NewRequest("POST", rurl, bytes.NewBuffer(args))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, e = http.NewRequest("GET", rurl, nil)
		if e != nil {
			log.Println("Unable to make GET request", e)
		}
		req.Header.Add("Accept", "*/*")
	}
	resp, err := _client.Do(req)
	if err != nil {
		log.Println("HTTP Error", err)
		response.Error = err
		return response
	}
	response.Status = resp.Status
	response.StatusCode = resp.StatusCode
	if err != nil {
		response.Error = err
		return response
	}
	defer resp.Body.Close()
	response.Data, err = io.ReadAll(resp.Body)
	if err != nil {
		response.Error = err
	}
	return response
}

// Response represents final response in a form of JSON structure
// we use custorm representation
func Response(rurl string, data []byte) []byte {
	b := []byte(`{"url":`)
	u := []byte(rurl)
	c := []byte(",")
	d := []byte(`"data":`)
	e := []byte(`}`)
	a := [][]byte{b, u, c, d, data, e}
	s := []byte(" ")
	r := bytes.Join(a, s)
	return r

}
