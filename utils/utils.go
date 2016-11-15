package utils

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
)

// global variable for this module which we're going to use across
// many modules
var VERBOSE int
var STATICDIR string
var PROFILE bool
var CMSAUTH int

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
