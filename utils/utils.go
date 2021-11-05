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

// VERBOSE controls verbosity level of the package
var VERBOSE int

// STATICDIR holds location of static directory for dbs2go
var STATICDIR string

// ORACLE represents a flag that underlying DB is oracle
var ORACLE bool

// BASE represents /base path of dbs2go end-point
var BASE string

// Localhost represents localhost name (with port) which can be used for local HTTP requests
var Localhost string

// GzipReader struct to handle GZip'ed content of HTTP requests
type GzipReader struct {
	*gzip.Reader
	io.Closer
}

// Close function closes gzip reader
func (gz GzipReader) Close() error {
	return gz.Closer.Close()
}

// RecordSize returns actual record size of given interface object
func RecordSize(v interface{}) (int64, error) {
	data, err := json.Marshal(v)
	if err == nil {
		return int64(binary.Size(data)), nil
	}
	return 0, err
}

// Stack returns full runtime stack
func Stack() string {
	trace := make([]byte, 2048)
	count := runtime.Stack(trace, false)
	return fmt.Sprintf("\nStack of %d bytes: %s\n", count, trace)
}

// ErrPropagate helper function which can be used in defer ErrPropagate()
func ErrPropagate(api string) {
	if err := recover(); err != nil {
		log.Println("ERROR", api, "error", err, Stack())
		panic(fmt.Sprintf("%s:%s", api, err))
	}
}

// ErrPropagate2Channel helper function which can be used in goroutines as
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

// GoDeferFunc runs any given function in defered go routine
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

// InList checks item in a list
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

// MapKeys returns string keys from a map
func MapKeys(rec map[string]interface{}) []string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// MapIntKeys returns int keys from a map
func MapIntKeys(rec map[int]interface{}) []int {
	keys := make([]int, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// List2Set converts input list into set
func List2Set(arr []string) []string {
	var out []string
	for _, key := range arr {
		if !InList(key, out) {
			out = append(out, key)
		}
	}
	return out
}

// StringList implements sort for []string type
type StringList []string

// Len provide length method of StringList
func (s StringList) Len() int { return len(s) }

// Swap provide swap method of StringList
func (s StringList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less provide less method of StringList
func (s StringList) Less(i, j int) bool { return s[i] < s[j] }

// ListFiles lists files in a given directory
func ListFiles(dir string) []string {
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

// ReplaceBinds replaces given pattern in string
func ReplaceBinds(stm string) string {
	regexp, err := regexp.Compile(`:[a-zA-Z_0-9]+`)
	if err != nil {
		log.Fatal(err)
	}
	match := regexp.ReplaceAllString(stm, "?")
	return match
}

// ConvertFloat converts string representation of float scientific number to string int
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

// PrintSQL prints SQL/args
func PrintSQL(stm string, args []interface{}, msg string) {
	if msg != "" {
		log.Println(msg)
	} else {
		log.Println("")
	}
	fmt.Printf("### SQL statement ###\n%s\n\n", stm)
	fmt.Printf("### SQL values ###\n")
	for _, v := range args {
		fmt.Printf("\t'%v'\n", v)
	}
}

// BasePath function provides end-point path for given api string
func BasePath(base, api string) string {
	if base != "" {
		if strings.HasPrefix(api, "/") {
			api = strings.Replace(api, "/", "", 1)
		}
		if strings.HasPrefix(base, "/") {
			return fmt.Sprintf("%s/%s", base, api)
		}
		return fmt.Sprintf("/%s/%s", base, api)
	}
	return api
}

// Insert inserts value into array at zero position
func Insert(arr []interface{}, val interface{}) []interface{} {
	arr = append(arr, val)
	copy(arr[1:], arr[0:])
	arr[0] = val
	return arr
}

// UpdateOrderedDict returns new ordered list from given ordered dicts
func UpdateOrderedDict(omap, nmap map[int][]string) map[int][]string {
	for idx, list := range nmap {
		if entries, ok := omap[idx]; ok {
			entries = append(entries, list...)
			omap[idx] = entries
		} else {
			omap[idx] = list
		}
	}
	return omap
}
