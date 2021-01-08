VERSION=`git rev-parse --short HEAD`
flags=-ldflags="-s -w -X main.version=${VERSION}"

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

test : test-all

test-all:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v .
test-primds:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run PrimDS
test-procds:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run ProcDS
test-datasets:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run Dataset
test-blocks:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run Block
test-files:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run File && go test -v -run LFN
test-runs:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run Run
test-ae:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run AcquisitionEra
test-rel:
	cd test && rm -f /tmp/dbs-test.db && sqlite3 /tmp/dbs-test.db < ../static/schema/sqlite-schema.sql && go test -v -run Release
test-util:
	cd test && go test -v -run Util

bench:
	cd test; go test -bench=.
