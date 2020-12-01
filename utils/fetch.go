// module to handle URL requests
// Copyright (c) 2015-2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package utils

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/user"

	"github.com/vkuznet/x509proxy"
)

// global HTTP client
var _client = HttpClient()

// global client's x509 certificates
var _certs []tls.Certificate

// client X509 certificates
func tlsCerts() ([]tls.Certificate, error) {
	if len(_certs) != 0 {
		return _certs, nil // use cached certs
	}
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")

	// check if /tmp/x509up_u$UID exists, if so setup X509_USER_PROXY env
	u, err := user.Current()
	if err == nil {
		fname := fmt.Sprintf("/tmp/x509up_u%s", u.Uid)
		if _, err := os.Stat(fname); err == nil {
			uproxy = fname
		}
	}

	if uproxy == "" && uckey == "" { // user doesn't have neither proxy or user certs
		log.Println("Neither proxy or user certs are found, to proceed use auth=false option otherwise setup X509 environment")
		return nil, nil
	}
	if uproxy != "" {
		// use local implementation of LoadX409KeyPair instead of tls one
		x509cert, err := x509proxy.LoadX509Proxy(uproxy)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy X509 proxy set by X509_USER_PROXY: %v", err)
		}
		_certs = []tls.Certificate{x509cert}
		return _certs, nil
	}
	x509cert, err := tls.LoadX509KeyPair(ucert, uckey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user X509 certificate: %v", err)
	}
	_certs = []tls.Certificate{x509cert}
	return _certs, nil
}

// HttpClient provides HTTP client
func HttpClient() *http.Client {
	// get X509 certs
	certs, err := tlsCerts()
	if err != nil {
		log.Println(err.Error())
		return &http.Client{}
	}
	if len(certs) == 0 {
		return &http.Client{}
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: certs,
			InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

// global URL counter for profile output
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
	response.Data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		response.Error = err
	}
	return response
}

// represent final response in a form of JSON structure
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
