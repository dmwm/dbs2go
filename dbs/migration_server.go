package dbs

import (
	"database/sql"
	"log"
	"time"

	"github.com/vkuznet/dbs2go/utils"
)

// MigrationProcessTimeout defines migration process timeout
var MigrationProcessTimeout int

// MigrationServerInterval defines migration process timeout
var MigrationServerInterval int

// MigrationDB points to migration DB
var MigrationDB *sql.DB

// MigrationCleanupInterval defines migration cleanup server interval
var MigrationCleanupInterval int

// MigrationCleanupOffset defines offset in seconds to delete migration requests
var MigrationCleanupOffset int64

// MigrationServer represent migration server.
// it accepts migration process timeout used by ProcessMigration API and
// exit channel
func MigrationServer(interval, timeout int, ch <-chan bool) {
	log.Println("Start migration server")
	api := API{Api: "ProcessMigration"}

	for {
		select {
		case v := <-ch:
			if v == true {
				log.Println("Received notification to stop migration server")
				return
			}
		default:
			time.Sleep(time.Duration(interval) * time.Second)
			// look-up all available migration requests
			records, err := MigrationRequests(-1)
			if err != nil {
				log.Printf("fail to fetch migration records from %s, error %v", MigrateURL, err)
				continue
			}
			if utils.VERBOSE > 0 {
				log.Printf("found %d migration requests", len(records))
			}
			for _, r := range records {
				if utils.VERBOSE > 0 {
					log.Printf("process %+v", r)
				}
				params := make(map[string]interface{})
				params["migration_request_url"] = r.MIGRATION_URL
				params["migration_request_id"] = r.MIGRATION_REQUEST_ID
				api.Params = params
				time0 := time.Now()
				metrics := utils.ProcFSMetrics()
				log.Printf("start new migration process with %+v metrics %+v", params, metrics)
				api.ProcessMigration()
				metrics = utils.ProcFSMetrics()
				log.Printf("migration process %+v finished in %v metrics %+v", params, time.Since(time0), metrics)
			}
		}
	}
	log.Println("Exit migration server")
}

// MigrationCleanupServer represents migration cleanup daemon..
func MigrationCleanupServer(interval int, offset int64, ch <-chan bool) {
	log.Println("Start migration cleanup server")
	api := API{Api: "CleanupMigrationRequests"}

	for {
		select {
		case v := <-ch:
			if v == true {
				log.Println("Received notification to stop migration cleanup server")
				return
			}
		default:
			time.Sleep(time.Duration(interval) * time.Second)
			// perform clean up query
			api.CleanupMigrationRequests(offset)
		}
	}
	log.Println("Exit migration cleanup server")
}
