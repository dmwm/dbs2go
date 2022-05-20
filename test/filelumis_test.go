package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var totalRequests uint64

// helper function to imitate chunk insertion
func insertChunk(wg *sync.WaitGroup, idx, limit int) {
	defer wg.Done()
	stm := "INSERT ALL"
	for i := idx; i < limit; i++ {
		into := `INTO ORA$PTT_TEMP_FILE_PARENTS (THIS_FILE_ID, PARENT_FILE_ID) VALUES (:fval, :pval)`
		stm = fmt.Sprintf("%s\n%s", stm, into)
		atomic.AddUint64(&totalRequests, 1)
	}
	stm = fmt.Sprintf("%s\nSELECT * FROM dual", stm)
	log.Println("execute", stm)
	// here stm represents final statement
	// instead of doing tx.Exec we'll sleep
	time.Sleep(10 * time.Millisecond)
}

// TestFileLumisInjectionLoop API
func TestFileLumisInjectionLoop(t *testing.T) {
	nrec := 3000
	maxSize := 110
	chunkSize := 25
	// inject some data into temp table
	if maxSize > nrec {
		maxSize = nrec
	}
	for k := 0; k < nrec; k = k + maxSize {
		t0 := time.Now()
		var wg sync.WaitGroup
		ngoroutines := 0
		for i := k; i < k+maxSize; i = i + chunkSize {
			wg.Add(1)
			size := i + chunkSize
			if size > (k + maxSize) {
				size = k + maxSize
			}
			if size > nrec {
				size = nrec
			}
			//             log.Printf("k=%d i=%d size=%d max=%d nrec=%d", k, i, size, maxSize, nrec)
			go insertChunk(&wg, i, size)
			ngoroutines += 1
		}
		limit := k + maxSize
		if limit > nrec {
			limit = nrec
		}
		log.Printf("process %d goroutines, step %d-%d, elapsed time %v", ngoroutines, k, limit, time.Since(t0))
		wg.Wait()
	}
	if int64(totalRequests) != int64(nrec) {
		t.Errorf("wrong number of processed events nrec=%d tot=%d", nrec, totalRequests)
	}
}
