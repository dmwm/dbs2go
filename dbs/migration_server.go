package dbs

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/vkuznet/dbs2go/utils"
)

// MigrationRequests fetches migration requests from migration table
func MigrationRequests(mid int64) ([]MigrationRequest, error) {
	var out []MigrationRequest

	// query MigrationRequest table and fetch all non-completed requests
	var args []interface{}
	var conds []string
	stm := getSQL("migration_requests")
	if mid > 0 {
		cond := fmt.Sprintf(" MR.MIGRATION_REQUEST_ID = %s", placeholder("migration_request_id"))
		conds = append(conds, cond)
		args = append(args, mid)
		stm = WhereClause(stm, conds)
	}

	// define in-memory pipe for writing and reading our data from the server
	// see working example of pipe usage in test/utils_test.go
	pr, pw := io.Pipe()
	defer pr.Close()

	// execute SQL call within goroutine to allow it to write via pipe writer
	go func() {
		defer pw.Close()
		pw.Write([]byte("["))             // open JSON records
		executeAll(pw, ",", stm, args...) // write JSON records
		pw.Write([]byte("]"))             // close JSON records
	}()

	// read from our pipe reader
	data, err := io.ReadAll(pr)
	if err != nil {
		log.Println("fail to read data", err)
		return out, err
	}
	// unmarshal our data from byte string
	var records []MigrationRequest
	err = json.Unmarshal(data, &records)
	if err != nil {
		log.Println("fail to unmarshal data", err)
		return out, err
	}
	return records, nil
}

// MigrationServer represent migration server.
// it accepts migration process timeout used by ProcessMigration API and
// exit channel
func MigrationServer(interval, timeout int, ch <-chan bool) {
	log.Println("Start migration server")
	for {
		select {
		case v := <-ch:
			if v == true {
				log.Println("Recieved notification to stop migration server")
				return
			}
		default:
			time.Sleep(time.Duration(interval) * time.Second)
			// look-up all available migration requests
			records, err := MigrationRequests(-1)
			if err != nil {
			}
			for _, r := range records {
				if utils.VERBOSE > 0 {
					log.Printf("process %+v", r)
				}
				params := make(map[string]interface{})
				params["migration_request_url"] = r.MIGRATION_URL
				params["migration_request_id"] = r.MIGRATION_REQUEST_ID
				api := API{
					Params: params,
					Api:    "ProcessMigration",
				}
				log.Printf("start new migration process with %+v", params)
				go api.ProcessMigration(timeout, false)
			}
		}
	}
	log.Println("Exit migration server")
}
