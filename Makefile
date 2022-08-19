VERSION=`git describe --tags`
flags=-ldflags="-s -w -X main.gitVersion=${VERSION}"
debug_flags=-ldflags="-X main.gitVersion=${VERSION}"
odir=`cat ${PKG_CONFIG_PATH}/oci8.pc | grep "libdir=" | sed -e "s,libdir=,,"`

all: build

vet:
	go vet .

build:
	go clean; rm -rf pkg dbs2go*; go build ${flags}

build_debug:
	go clean; rm -rf pkg dbs2go*; go build -gcflags=all="-N -l" ${debug_flags}

build_all: build build_osx build_linux build_power8 build_arm64

build_osx:
	go clean; rm -rf pkg dbs2go_osx; GOOS=darwin go build ${flags}
	mv dbs2go dbs2go_osx

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

test: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-migrate test-writer test-integration test-lexicon bench

test-github: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-writer test-lexicon test-integration test-migration bench

test-lexicon: test-lexicon-writer-pos test-lexicon-writer-neg test-lexicon-reader-pos test-lexicon-reader-neg

test-errors:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} go test -v -run TestDBSError
test-dbs:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run TestDBS
test-bulk:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run Bulk
test-sql:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run SQL
test-validator:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_DB_FILE=/tmp/dbs-test.db \
	go test -v -run Validator
test-http:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run HTTP
test-writer:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run DBSWriter
test-utils:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run Utils
test-migrate:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run Migrate
test-filelumis:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	DBS_DB_FILE=/tmp/dbs-test.db \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -v -run FileLumisInjection
test-lexicon-writer-pos:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_writer_positive.json \
	go test -v -run LexiconPositive
test-lexicon-writer-neg:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_writer_negative.json \
	go test -v -run LexiconNegative
test-lexicon-reader-pos:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_reader_positive.json \
	go test -v -run LexiconPositive
test-lexicon-reader-neg:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_LEXICON_SAMPLE_FILE=../static/lexicon_reader_negative.json \
	go test -v -run LexiconNegative
test-integration:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	echo "\"sqlite3 /tmp/dbs-test.db sqlite\"" > ./dbfile && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_READER_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_WRITER_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_DB_FILE=./dbfile \
	DBS_DB_FILE=/tmp/dbs-test.db \
	INTEGRATION_DATA_FILE=./data/integration/integration_data.json \
	BULKBLOCKS_DATA_FILE=./data/integration/bulkblocks_data.json \
	LARGE_BULKBLOCKS_DATA_FILE=./data/integration/largebulkblocks_data.json \
	FILE_LUMI_LIST_LENGTH=30 \
	go test -v -failfast -run Integration
test-migration:
	cd test && rm -f /tmp/dbs-one.db && \
	sqlite3 /tmp/dbs-one.db < ../static/schema/sqlite-schema.sql && \
	echo sqlite3 /tmp/dbs-one.db sqlite > ./dbfile_1 && \
	rm -f /tmp/dbs-two.db && \
	sqlite3 /tmp/dbs-two.db < ../static/schema/sqlite-schema.sql && \
	echo sqlite3 /tmp/dbs-two.db sqlite > ./dbfile_2 && \
	cd .. && \
	./bin/start_test_migration && \
	cd test && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_READER_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_WRITER_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_DB_FILE_1=./dbfile_1 \
	DBS_DB_FILE_2=./dbfile_2 \
	DBS_DB_PATH_1=/tmp/dbs-one.db \
	DBS_DB_PATH_2=/tmp/dbs-two.db \
	BULKBLOCKS_DATA_FILE=./data/migration/bulkblocks_data.json \
	go test -v -failfast -run IntMigration
test-migration-requests:
	cd test && rm -f /tmp/dbs-one.db && \
	sqlite3 /tmp/dbs-one.db < ../static/schema/sqlite-schema.sql && \
	echo sqlite3 /tmp/dbs-one.db sqlite > ./dbfile_1 && \
	rm -f /tmp/dbs-two.db && \
	sqlite3 /tmp/dbs-two.db < ../static/schema/sqlite-schema.sql && \
	echo sqlite3 /tmp/dbs-two.db sqlite > ./dbfile_2 && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_READER_LEXICON_FILE=../static/lexicon_reader.json \
	DBS_WRITER_LEXICON_FILE=../static/lexicon_writer.json \
	DBS_DB_FILE_1=./dbfile_1 \
	DBS_DB_FILE_2=./dbfile_2 \
	DBS_DB_PATH_1=/tmp/dbs-one.db \
	DBS_DB_PATH_2=/tmp/dbs-two.db \
	BULKBLOCKS_DATA_FILE=./data/migration/bulkblocks_data.json \
	MIGRATION_REQUESTS_PATH=./data/migration/requests \
	go test -v -failfast -run MigrationRequests
bench:
	cd test && rm -f /tmp/dbs-test.db && \
	sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && \
	LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} \
	DBS_DB_FILE=/tmp/dbs-test.db \
	DBS_API_PARAMETERS_FILE=../static/parameters.json \
	DBS_LEXICON_FILE=../static/lexicon_writer.json \
	go test -run Benchmark -bench=.
