package dbs

import (
	"database/sql"
	"log"
	"time"

	"github.com/dmwm/dbs2go/utils"
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

// MigrationRetries specifies total number of migration retries
var MigrationRetries int64

// MigrationServer represent migration server.
// it accepts migration process timeout used by ProcessMigration API and
// exit channel
func MigrationServer(interval, timeout int, ch <-chan bool) {
	log.Println("Start migration server with verbose mode", utils.VERBOSE)
	api := API{Api: "ProcessMigration"}

	if MigrationRetries == 0 {
		MigrationRetries = 3 // by default we'll allow only 3 retries
	}

	lastCall := time.Now()
	for {
		select {
		case v := <-ch:
			if v == true {
				log.Println("Received notification to stop migration server")
				return
			}
		default:
			time.Sleep(time.Duration(1) * time.Second)
			if time.Since(lastCall).Seconds() < float64(interval) {
				continue
			}
			if utils.VERBOSE > 0 {
				log.Println("call MigrationRequests")
			}
			lastCall = time.Now() // update last call time stamp
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
				// check if request already processed multiple times and give up after certin threshold
				if r.RETRY_COUNT > MigrationRetries {
					updateMigrationStatus(r, TERM_FAILED)
					continue
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
				log.Printf(
					"migration process %+v finished in %v metrics %+v",
					params, time.Since(time0), metrics)
			}
		}
	}
	log.Println("Exit migration server")
}

// MigrationCleanupServer represents migration cleanup daemon..
func MigrationCleanupServer(interval int, offset int64, ch <-chan bool) {
	log.Println("Start migration cleanup server")
	api := API{Api: "CleanupMigrationRequests"}

	lastCall := time.Now()
	for {
		select {
		case v := <-ch:
			if v == true {
				log.Println("Received notification to stop migration cleanup server")
				return
			}
		default:
			time.Sleep(time.Duration(1) * time.Second)
			if time.Since(lastCall).Seconds() < float64(interval) {
				continue // we did not exceed our interval since last call
			}
			if utils.VERBOSE > 0 {
				log.Println("call CleanupMigrationRequest")
			}
			// perform clean up query
			api.CleanupMigrationRequests(offset)
			lastCall = time.Now() // update last call time stamp
		}
	}
	log.Println("Exit migration cleanup server")
}
