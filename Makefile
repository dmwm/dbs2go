VERSION=`git describe --tags`
flags=-ldflags="-s -w -X main.gitVersion=${VERSION}"
odir=`cat ${PKG_CONFIG_PATH}/oci8.pc | grep "libdir=" | sed -e "s,libdir=,,"`

all: build

vet:
	go vet .

build:
	go clean; rm -rf pkg dbs2go*; go build ${flags}

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

test: test-dbs test-sql test-validator test-bulk test-http test-utils test-migrate test-writer bench

test-lexicon: test-lexicon-writer-pos test-lexicon-writer-neg test-lexicon-reader-pos test-lexicon-reader-neg

test-dbs:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run TestDBS
test-bulk:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run Bulk
test-sql:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run SQL
test-validator:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run Validator
test-http:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run HTTP
test-writer:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run DBSWriter
test-utils:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run Utils
test-migrate:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run Migrate
test-filelumis:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -v -run FileLumisInjection
test-lexicon-writer-pos:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json DBS_LEXICON_SAMPLE_FILE=../static/lexicon_writer_positive.json go test -v -run LexiconPositive
test-lexicon-writer-neg:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json DBS_LEXICON_SAMPLE_FILE=../static/lexicon_writer_negative.json go test -v -run LexiconNegative
test-lexicon-reader-pos:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_reader.json DBS_LEXICON_SAMPLE_FILE=../static/lexicon_reader_positive.json go test -v -run LexiconPositive
test-lexicon-reader-neg:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_reader.json DBS_LEXICON_SAMPLE_FILE=../static/lexicon_reader_negative.json go test -v -run LexiconNegative
bench:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} DBS_LEXICON_FILE=../static/lexicon_writer.json go test -run Benchmark -bench=.
