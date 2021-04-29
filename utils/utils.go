package utils

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// global variable for this module which we're going to use across
// many modules
var VERBOSE int
var STATICDIR string
var PROFILE bool
var ORACLE bool

// GzipReader struct to handle GZip'ed content of HTTP requests
type GzipReader struct {
	*gzip.Reader
	io.Closer
}

// helper function to close gzip reader
func (gz GzipReader) Close() error {
	return gz.Closer.Close()
}

// RecordSize
func RecordSize(v interface{}) (int64, error) {
	data, err := json.Marshal(v)
	if err == nil {
		return int64(binary.Size(data)), nil
	}
	return 0, err
}

// helper function to return Stack
func Stack() string {
	trace := make([]byte, 2048)
	count := runtime.Stack(trace, false)
	return fmt.Sprintf("\nStack of %d bytes: %s\n", count, trace)
}

// error helper function which can be used in defer ErrPropagate()
func ErrPropagate(api string) {
	if err := recover(); err != nil {
		log.Println("ERROR", api, "error", err, Stack())
		panic(fmt.Sprintf("%s:%s", api, err))
	}
}

// error helper function which can be used in goroutines as
// ch := make(chan interface{})
// go func() {
//    defer ErrPropagate2Channel(api, ch)
//    someFunction()
// }()
func ErrPropagate2Channel(api string, ch chan interface{}) {
	if err := recover(); err != nil {
		log.Println("ERROR", api, "error", err, Stack())
		ch <- fmt.Sprintf("%s:%s", api, err)
	}
}

// Helper function to run any given function in defered go routine
func GoDeferFunc(api string, f func()) {
	ch := make(chan interface{})
	go func() {
		defer ErrPropagate2Channel(api, ch)
		f()
		ch <- "ok" // send to channel that we can read it later in case of success of f()
	}()
	err := <-ch
	if err != nil && err != "ok" {
		panic(err)
	}
}

// helper function to check item in a list
func InList(a string, list []string) bool {
	check := 0
	for _, b := range list {
		if b == a {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// helper function to return keys from a map
func MapKeys(rec map[string]interface{}) []string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// helper function to return keys from a map
func MapIntKeys(rec map[int]interface{}) []int {
	keys := make([]int, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// helper function to convert input list into set
func List2Set(arr []string) []string {
	var out []string
	for _, key := range arr {
		if !InList(key, out) {
			out = append(out, key)
		}
	}
	return out
}

// implement sort for []string type
type StringList []string

func (s StringList) Len() int           { return len(s) }
func (s StringList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StringList) Less(i, j int) bool { return s[i] < s[j] }

// helper function to list files in given directory
func Listfiles(dir string) []string {
	var out []string
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "du1: %v\n", err)
		return nil
	}
	for _, f := range entries {
		if !f.IsDir() {
			out = append(out, f.Name())
		}
	}
	return out
}

// CastString function to check and cast interface{} to string data-type
func CastString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	}
	msg := fmt.Sprintf("wrong data type for %v type %T", val, val)
	return "", errors.New(msg)
}

// CastInt function to check and cast interface{} to int data-type
func CastInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	}
	msg := fmt.Sprintf("wrong data type for %v type %T", val, val)
	return 0, errors.New(msg)
}

// CastInt64 function to check and cast interface{} to int64 data-type
func CastInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	}
	msg := fmt.Sprintf("wrong data type for %v type %T", val, val)
	return 0, errors.New(msg)
}

// CastFloat function to check and cast interface{} to int64 data-type
func CastFloat(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	}
	msg := fmt.Sprintf("wrong data type for %v type %T", val, val)
	return 0, errors.New(msg)
}

// ReplacePattern replaces given pattern in string
func ReplaceBinds(stm string) string {
	regexp, err := regexp.Compile(`:[a-zA-Z_0-9]+`)
	if err != nil {
		log.Fatal(err)
	}
	match := regexp.ReplaceAllString(stm, "?")
	return match
}

// helper function to convert string representation of float scientific number to string int
func ConvertFloat(val string) string {
	if strings.Contains(val, "e+") || strings.Contains(val, "E+") {
		// we got float number, should be converted to int
		v, e := strconv.ParseFloat(val, 64)
		if e != nil {
			log.Println("unable to convert", val, " to float, error", e)
			return val
		}
		return strings.Split(fmt.Sprintf("%f", v), ".")[0]
	}
	return val
}
