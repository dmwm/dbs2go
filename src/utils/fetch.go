/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: module to handle URL requests
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 * Some links: http://www.alexedwards.net/bfmt/golang-response-snippets
 * http://bfmt.golang.org/json-and-go
 * http://golang.org/pkg/html/template/
 * https://labix.org/mgo
 */
package utils

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"x509proxy"
)

/*
 * Return array of certificates
 */
func Certs() (tls_certs []tls.Certificate) {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")
	if VERBOSE > 0 {
		fmt.Println("X509_USER_PROXY", uproxy)
		fmt.Println("X509_USER_KEY", uckey)
		fmt.Println("X509_USER_CERT", ucert)
	}
	if len(uproxy) > 0 {
		// use local implementation of LoadX409KeyPair instead of tls one
		x509cert, err := x509proxy.LoadX509Proxy(uproxy)
		if err != nil {
			fmt.Println("Fail to parser proxy X509 certificate", err)
			return
		}
		tls_certs = []tls.Certificate{x509cert}
	} else if len(uckey) > 0 {
		x509cert, err := tls.LoadX509KeyPair(ucert, uckey)
		if err != nil {
			fmt.Println("Fail to parser user X509 certificate", err)
			return
		}
		tls_certs = []tls.Certificate{x509cert}
	} else {
		return
	}
	return
}

/*
 * HTTP client for urlfetch server
 */
func HttpClient() (client *http.Client) {
	// create HTTP client
	certs := Certs()
	if VERBOSE > 1 {
		fmt.Println("Number of certificates", len(certs))
	}
	if len(certs) == 0 {
		client = &http.Client{}
		return
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: certs,
			InsecureSkipVerify: true},
	}
	if VERBOSE > 1 {
		fmt.Println("Create TLSClientConfig")
	}
	client = &http.Client{Transport: tr}
	return
}

// create global HTTP client and re-use it through the code
var client = HttpClient()

// global URL counter for profile output
var UrlCounter uint32

// ResponseType structure is what we expect to get for our URL call.
// It contains a request URL, the data chunk and possible error from remote
type ResponseType struct {
	Url   string
	Data  []byte
	Error error
}

// A URL fetch Worker. It has three channels: in channel for incoming requests
// (in a form of URL strings), out channel for outgoing responses in a form of
// ResponseType structure and quit channel
func Worker(in <-chan string, out chan<- ResponseType, quit <-chan bool) {
	for {
		select {
		case rurl := <-in:
			go Fetch(rurl, "", out)
		case <-quit:
			return
		default:
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

// Fetch data for provided URL
func FetchResponse(rurl, args string) ResponseType {
	if PROFILE {
		atomic.AddUint32(&UrlCounter, 1)
	}
	var response ResponseType
	response.Url = rurl
	response.Data = []byte{}
	if validate_url(rurl) == false {
		response.Error = errors.New("Invalid URL")
		return response
	}
	var req *http.Request
	if len(args) > 0 {
		var jsonStr = []byte(args)
		req, _ = http.NewRequest("POST", rurl, bytes.NewBuffer(jsonStr))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, _ = http.NewRequest("GET", rurl, nil)
		req.Header.Add("Accept-Encoding", "identity")
		if strings.Contains(rurl, "sitedb") {
			req.Header.Add("Accept", "application/json")
		}
	}
	if VERBOSE > 2 {
		dump1, err1 := httputil.DumpRequestOut(req, true)
		fmt.Println("### HTTP request", string(dump1), err1)
	}
	resp, err := client.Do(req)
	if VERBOSE > 2 {
		dump2, err2 := httputil.DumpResponse(resp, true)
		fmt.Println("### HTTP response", string(dump2), err2)
	}
	if err != nil {
		response.Error = err
		return response
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		response.Error = err
		return response
	}
	response.Data = body
	return response
}

// Fetch data for provided URL and redirect results to given channel
func Fetch(rurl string, args string, ch chan<- ResponseType) {
	//    fmt.Println("Receive", rurl)
	var resp, r ResponseType
	retry := 3 // how many times we'll retry given url
	startTime := time.Now()
	resp = FetchResponse(rurl, args)
	if resp.Error != nil {
		fmt.Println("WARNING, fail to fetch data", rurl, "error", resp.Error)
		for i := 1; i <= retry; i++ {
			sleep := time.Duration(i) * time.Second
			time.Sleep(sleep)
			r = FetchResponse(rurl, args)
			if r.Error == nil {
				break
			}
			fmt.Println("WARNING", rurl, "retry", i, "error", r.Error)
		}
		resp = r
	}
	if resp.Error != nil {
		fmt.Println("ERROR, fail to fetch data", rurl, "retries", retry, "error", resp.Error)
	}
	endTime := time.Now()
	if VERBOSE > 0 {
		if args == "" {
			fmt.Println("GET", rurl, endTime.Sub(startTime))
		} else {
			fmt.Println("POST", rurl, args, endTime.Sub(startTime))
		}
	}
	ch <- resp
}

// Helper function which validates given URL
func validate_url(rurl string) bool {
	if len(rurl) > 0 {
		pat := "(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]"
		matched, err := regexp.MatchString(pat, rurl)
		if err == nil {
			if matched == true {
				return true
			}
		}
		fmt.Println("ERROR invalid URL:", rurl)
	}
	return false
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
