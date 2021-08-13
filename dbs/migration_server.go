package dbs

import (
	"log"
	"time"
)

// MigrationRequests fetches migration requests from migration table
func MigrationRequests() []MigrationRequest {
	var out []MigrationRequest
	// query MigrationRequest table and fetch all non-completed requests
	return out
}

// MigrationServer represent migration server.
// it accepts migration process timeout used by ProcessMigration API and
// exit channel
func MigrationServer(timeout int, ch <-chan bool) {
	log.Println("Start migration server")
	for {
		select {
		case v := <-ch:
			if v == true {
				log.Println("Recieved notification to stop migration server")
				return
			}
		default:
			for _, req := range MigrationRequests() {
				params := make(map[string]interface{})
				params["migration_request_url"] = req.MIGRATION_URL
				params["migration_request_id"] = req.MIGRATION_REQUEST_ID
				api := API{
					Params: params,
					Api:    "ProcessMigration",
				}
				log.Printf("start new migration process with %+v", params)
				go api.ProcessMigration(timeout, false)
			}
			time.Sleep(time.Duration(1) * time.Millisecond) // wait for response
		}
	}
	log.Println("Exit migration server")
}
