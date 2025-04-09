VERSION=`git describe --tags`
flags=-ldflags="-s -w -X main.gitVersion=${VERSION}"
debug_flags=-ldflags="-X main.gitVersion=${VERSION}"

# we'll acquire architecture of the node and disable ORACLE libs for arm so far
# as there is no official ORACLE build on that architecture
arch:=$(shell uname -p)
ifeq ($(arch),arm)
odir=""
else
odir=`cat ${PKG_CONFIG_PATH}/oci8.pc | grep "libdir=" | sed -e "s,libdir=,,"`
endif

ifeq ($(arch),arm)
.IGNORE:
all: strip_oracle build restore_oracle
else
all: build
endif

vet:
	go vet .

ORAFILES =  web/server.go test/merge/main.go test/seq/seq.go test/http_test.go test/writer_test.go test/oracle_drivers_test.go test/seq/oracle_drivers_test.go test/merge/oracle_drivers_test.go cgotest/oracle_drivers.go

strip_oracle:
	$(info ### on $(arch) platform there is no ORALCE libs, we will disable their drivers from the build)
	for f in $(ORAFILES); do \
		sed -i -e "s,_ \"github.com/mattn/go-oci8\",//_ \"github.com/mattn/go-oci8\",g" $$f; \
		sed -i -e "s,_ \"gopkg.in/rana/ora.v4\",//_ \"gopkg.in/rana/ora.v4\",g" $$f; \
		rm $$f-e; \
	done

restore_oracle: $(ORAFILES)
	$(info ### on $(arch) platform there is no ORALCE libs, we will restore them after the build)
	for f in $(ORAFILES); do \
		sed -i -e "s,//_ \"github.com/mattn/go-oci8\",_ \"github.com/mattn/go-oci8\",g" $$f; \
		sed -i -e "s,//_ \"gopkg.in/rana/ora.v4\",_ \"gopkg.in/rana/ora.v4\",g" $$f; \
		rm $$f-e; \
	done

build:
	$(info ### building dbs2go executable on $(arch))
	go clean; rm -rf pkg dbs2go*; go build ${flags}
	@echo

.IGNORE:
build_no_oracle: strip_oracle build restore_oracle

build_debug:
	go clean; rm -rf pkg dbs2go*; go build -gcflags=all="-N -l" ${debug_flags}

build_all: build build_osx build_osx_arm64 build_linux build_power8 build_arm64

build_osx:
	go clean; rm -rf pkg dbs2go_osx; GOOS=darwin go build ${flags}
	mv dbs2go dbs2go_osx_x86

build_osx_arm64:
	go clean; rm -rf pkg dbs2go_osx; GOARCH=arm64 GOOS=darwin go build ${flags}
	mv dbs2go dbs2go_osx_arm64

build_linux:
	go clean; rm -rf pkg dbs2go_linux; GOOS=linux go build ${flags}
	mv dbs2go dbs2go_linux

build_power8:
	go clean; rm -rf pkg dbs2go_power8; GOARCH=ppc64le GOOS=linux go build ${flags}
	mv dbs2go dbs2go_power8

build_arm64:
	go clean; rm -rf pkg dbs2go_arm64; GOARCH=arm64 GOOS=linux go build ${flags}
	mv dbs2go dbs2go_arm64

install:
	go install

clean:
	go clean; rm -rf pkg

ifeq ($(arch),arm)
test_all: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-migrate test-writer test-integration test-lexicon bench
.IGNORE:
test: strip_oracle test_all restore_oracle
else
test: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-migrate test-writer test-integration test-lexicon bench
endif

test-github: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-writer test-lexicon test-integration test-migration-requests test-migration bench

test-lexicon: test-lexicon-writer-pos test-lexicon-writer-neg test-lexicon-reader-pos test-lexicon-reader-neg

test-errors:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} go test -v -run TestDBSError
test-errors-no-oracle: strip_oracle test-errors restore_oracle
test-dbs:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run TestDBS
test-bulk:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run Bulk
test-sql:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run SQL
test-validator:
	@set -e; \
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_DB_FILE=/tmp/dbs-test.db \
	go test -v -run Validator
test-http:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run HTTP
test-writer:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run DBSWriter
test-utils:
	@set -e; \
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run Utils
test-migrate:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run Migrate
test-filelumis:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run FileLumisInjection
test-lexicon-writer-pos:
	@set -e; \
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_writer_positive.json \
	go test -v -run LexiconPositive
test-lexicon-writer-neg:
	@set -e; \
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_writer_negative.json \
	go test -v -run LexiconNegative
test-lexicon-reader-pos:
	@set -e; \
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_reader_positive.json \
	go test -v -run LexiconPositive
test-lexicon-reader-neg:
	@set -e; \
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_reader_negative.json \
	go test -v -run LexiconNegative
test-integration:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	echo "\"sqlite3 /tmp/dbs-test.db sqlite\"" > ./dbfile && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_READER_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_WRITER_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_DB_FILE=/tmp/dbs-test.db \
	INTEGRATION_DATA_FILE=./data/integration/integration_data.json \
	BULKBLOCKS_DATA_FILE=./data/integration/bulkblocks_data.json \
	LARGE_BULKBLOCKS_DATA_FILE=./data/integration/largebulkblocks_data.json \
	FILE_LUMI_LIST_LENGTH=30 \
	go test -v -failfast -run Integration
test-migration:
	@set -e; \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	./bin/start_test_migration && \
	cd test && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	BULKBLOCKS_DATA_FILE=./data/migration/bulkblocks_data.json \
	go test -v -failfast -timeout 10m -run IntMigration
test-migration-requests:
	@set -e; \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	./bin/start_test_migration && \
	cd test && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	MIGRATION_REQUESTS_PATH=./data/migration/requests \
	go test -v -failfast -timeout 10m -run MigrationRequests
bench:
	@set -e; \
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -run Benchmark -bench=.
test-race:
	@set -e; \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	./bin/start_write_servers && \
	cd test && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	MIGRATION_REQUESTS_PATH=./data/migration/requests \
	INTEGRATION_DATA_FILE=./data/integration/integration_data.json \
	go test -v -failfast -timeout 10m -run RaceConditions
