VERSION=`git describe --tags`
flags=-ldflags="-s -w -X main.gitVersion=${VERSION}"
debug_flags=-ldflags="-X main.gitVersion=${VERSION}"

# we'll acquire architecture of the node and disable ORACLE libs for arm so far
# as there is no official ORACLE build on that architecture
arch:=$(shell uname -p)
DOCKER_STRICT := $(if $(filter docker oracle-env oracle-arch-check build-ora build_ora,$(firstword $(MAKECMDGOALS))),1,0)
ifeq ($(arch),arm)
odir=""
else
odir=`cat ${PKG_CONFIG_PATH}/oci8.pc | grep "libdir=" | sed -e "s,libdir=,,"`
endif

ifeq ($(arch),arm)
all: strip_oracle build restore_oracle
ifneq ($(DOCKER_STRICT),1)
.IGNORE:
endif
else
all: build
endif

# If the target is `release`, consider the second argument as the release version
ifeq (release,$(firstword $(MAKECMDGOALS)))
  RELEASE := $(word 2, $(MAKECMDGOALS) )
  $(eval $(RELEASE):;@true)
endif

.PHONY: release
release: ./buildRelease.sh
	./buildRelease.sh -t $(RELEASE)

TAG ?= $(shell git describe --tags --exact-match 2>/dev/null || git describe --tags)
REPO_FALLBACK ?= dmwm/dbs2go
REPO_URL ?= $(shell git remote get-url upstream 2>/dev/null || git remote get-url origin 2>/dev/null || echo https://github.com/$(REPO_FALLBACK).git)
REPO ?= $(shell echo $(REPO_URL) | sed -e 's,.*github.com[:/],,' -e 's,.git$$,,')
REGISTRY ?= registry.cern.ch
PROJECT ?= cmsweb
REPOSITORY ?= dbs2go
IMAGE ?= $(REGISTRY)/$(PROJECT)/$(REPOSITORY)
IMAGEBOT_NAMESPACE ?= dbs
IMAGEBOT_SERVICE ?= dbs2go
UPLOAD_BUILD_TARGET ?= build
DOCKER_BUILD_DIR ?= .docker.build
ORACLE_IMAGE ?= registry.cern.ch/cmsweb/oracle:21_5-stable
ORACLE_ENV_DIR := $(abspath $(DOCKER_BUILD_DIR)/oracle-env)
ORACLE_DIR := $(ORACLE_ENV_DIR)/oracle
ORACLE_ENV = PKG_CONFIG_PATH="$(ORACLE_ENV_DIR)" LD_LIBRARY_PATH="$(ORACLE_DIR)$${LD_LIBRARY_PATH:+:$${LD_LIBRARY_PATH}}" PATH="$(ORACLE_DIR):$${PATH}"
define check_oracle_arch
	host_os=$$(uname -s); \
	[ "$$host_os" = "Linux" ] || { \
		echo "Oracle host builds require Linux; detected $$host_os"; \
		exit 1; \
	}; \
	case "$$(uname -m)" in \
		x86_64|amd64) host_arch=amd64 ;; \
		aarch64|arm64) host_arch=arm64 ;; \
		ppc64le) host_arch=ppc64le ;; \
		*) echo "Unsupported host architecture: $$(uname -m)"; exit 1 ;; \
	esac; \
	oracle_arch=$$(docker image inspect --format '{{.Architecture}}' "$(ORACLE_IMAGE)"); \
	[ "$$host_arch" = "$$oracle_arch" ] || { \
		echo "Oracle architecture mismatch: host=$$host_arch image=$$oracle_arch ($(ORACLE_IMAGE))"; \
		exit 1; \
	}
endef
DOCKER_ACTION := $(word 2,$(MAKECMDGOALS))
DOCKER_REF := $(word 3,$(MAKECMDGOALS))
DOCKER_TAG_ARG := $(if $(DOCKER_REF),TAG=$(DOCKER_REF),)

ifneq (,$(filter upload docker k8deploy,$(firstword $(MAKECMDGOALS))))
  ifeq (no-oracle,$(word 2,$(MAKECMDGOALS)))
    UPLOAD_BUILD_TARGET := build-no-oracle
    $(eval no-oracle:;@true)
  endif
endif

.PHONY: upload upload-no-oracle docker docker-build docker-push push k8deploy k8deploy-no-oracle oracle-env oracle-arch-check build-ora build_ora
upload:
	$(MAKE) docker-build UPLOAD_BUILD_TARGET=$(UPLOAD_BUILD_TARGET)
	$(MAKE) docker-push

docker:
	@case "$(DOCKER_ACTION)" in \
		build) \
			[ -n "$(DOCKER_REF)" ] || { echo "Usage: make docker build {localtree|dev|<release-tag>}"; exit 1; }; \
			$(MAKE) docker-build DOCKER_STRICT=1 $(DOCKER_TAG_ARG) ;; \
		push) \
			[ -n "$(DOCKER_REF)" ] || { echo "Usage: make docker push {localtree|dev|<release-tag>}"; exit 1; }; \
			$(MAKE) docker-push DOCKER_STRICT=1 $(DOCKER_TAG_ARG) ;; \
		*) echo "Usage: make docker {build|push} {localtree|dev|<release-tag>}"; exit 1 ;; \
	esac

# The second word in `make docker build` or `make docker push` is also parsed
# by make as a goal. These targets prevent it from running a second action.
ifeq (docker,$(firstword $(MAKECMDGOALS)))
push:
	@true
%:
	@true
endif

docker-build:
	@set -e; \
	[ -n "$(TAG)" ] || { echo "TAG is required; use localtree, dev, or a release tag"; exit 1; }; \
	case "$(TAG)" in \
		localtree|dev) build_mode="dev" ;; \
		*) \
			echo "$(TAG)" | grep -Eq '^v?[0-9]+\.[0-9]+\.[0-9]+(rc[0-9]+)?$$' || { \
				echo "TAG=$(TAG) is not localtree, dev, or a release tag"; exit 1; \
			}; \
			build_mode="tag" ;; \
	esac; \
	mkdir -p "$(DOCKER_BUILD_DIR)"; \
	curl -kfsSL https://raw.githubusercontent.com/dmwm/CMSKubernetes/master/docker/dbs2go/Dockerfile -o "$(DOCKER_BUILD_DIR)/Dockerfile"; \
	curl -kfsSL https://raw.githubusercontent.com/dmwm/CMSKubernetes/master/docker/dbs2go/oci8.pc -o "$(DOCKER_BUILD_DIR)/oci8.pc"; \
	curl -kfsSL https://raw.githubusercontent.com/dmwm/CMSKubernetes/master/docker/dbs2go/config.json -o "$(DOCKER_BUILD_DIR)/config.json"; \
	curl -kfsSL https://raw.githubusercontent.com/dmwm/CMSKubernetes/master/docker/dbs2go/monitor.sh -o "$(DOCKER_BUILD_DIR)/monitor.sh"; \
	curl -kfsSL https://raw.githubusercontent.com/dmwm/CMSKubernetes/master/docker/dbs2go/run.sh -o "$(DOCKER_BUILD_DIR)/run.sh"; \
	chmod +x "$(DOCKER_BUILD_DIR)/run.sh"; \
	if [ "$$build_mode" = "dev" ]; then \
		# TODO: switch back to dmwm/CMSKubernetes master after Dockerfile.dev is merged. \
		curl -kfsSL https://raw.githubusercontent.com/todor-ivanov/CMSKubernetes/refs/heads/feature_dbs2goAddBuildsFromLocaltree_fix-153/docker/dbs2go/Dockerfile.dev -o "$(DOCKER_BUILD_DIR)/Dockerfile.dev"; \
		source_dir="$(DOCKER_BUILD_DIR)/src"; \
		source_tmp="$(DOCKER_BUILD_DIR)/src.tmp"; \
		source_archive="$(DOCKER_BUILD_DIR)/src.tar"; \
		rm -rf "$$source_dir" "$$source_tmp"; \
		rm -f "$$source_archive"; \
		mkdir -p "$$source_tmp"; \
		tar --exclude='./.git' --exclude='./.agents' --exclude='./.codex' --exclude='./.docker.build' \
			--exclude='./tmp' --exclude='./dbs2go' --exclude='./dbs2go_*' --exclude='./pkg' \
			-cf "$$source_archive" .; \
		tar -xf "$$source_archive" -C "$$source_tmp"; \
		rm -f "$$source_archive"; \
		mv "$$source_tmp" "$$source_dir"; \
		docker build -f "$(DOCKER_BUILD_DIR)/Dockerfile.dev" "$(DOCKER_BUILD_DIR)" --tag "$(IMAGE):$(TAG)"; \
	else \
		sed -i -e "s,ENV TAG=.*,ENV TAG=$(TAG),g" "$(DOCKER_BUILD_DIR)/Dockerfile"; \
		docker build "$(DOCKER_BUILD_DIR)" --tag "$(IMAGE):$(TAG)"; \
	fi; \
	docker image inspect "$(IMAGE):$(TAG)" >/dev/null

oracle-env:
	@set -eu; \
	mkdir -p "$(ORACLE_ENV_DIR)"; \
	cid=""; \
	oracle_tmp="$(ORACLE_ENV_DIR)/oracle.tmp"; \
	oci8_source="$(ORACLE_ENV_DIR)/oci8.source.pc"; \
	oci8_tmp="$(ORACLE_ENV_DIR)/oci8.host.pc"; \
	cleanup() { \
		status=$$?; \
		trap - EXIT HUP INT TERM; \
		if [ -n "$$cid" ]; then docker rm -f "$$cid" >/dev/null 2>&1 || true; fi; \
		rm -rf "$$oracle_tmp"; \
		rm -f "$$oci8_source" "$$oci8_tmp"; \
		exit $$status; \
	}; \
	trap cleanup EXIT HUP INT TERM; \
	curl -kfsSL https://raw.githubusercontent.com/dmwm/CMSKubernetes/master/docker/dbs2go/oci8.pc -o "$$oci8_source"; \
	docker pull "$(ORACLE_IMAGE)"; \
	$(check_oracle_arch); \
	cid=$$(docker create "$(ORACLE_IMAGE)"); \
	rm -rf "$$oracle_tmp"; \
	docker cp "$$cid:/usr/lib/oracle" "$$oracle_tmp"; \
	docker rm "$$cid"; \
	cid=""; \
	rm -rf "$(ORACLE_DIR)"; \
	mv "$$oracle_tmp" "$(ORACLE_DIR)"; \
	sed -e "s|^libdir=.*|libdir=$(ORACLE_DIR)|" \
		-e "s|^includedir=.*|includedir=$(ORACLE_DIR)/sdk/include|" \
		"$$oci8_source" > "$$oci8_tmp"; \
	mv "$$oci8_tmp" "$(ORACLE_ENV_DIR)/oci8.pc"; \
	PKG_CONFIG_PATH="$(ORACLE_ENV_DIR)" pkg-config --cflags oci8 >/dev/null; \
	PKG_CONFIG_PATH="$(ORACLE_ENV_DIR)" pkg-config --libs oci8 >/dev/null; \
	trap - EXIT HUP INT TERM; \
	rm -f "$$oci8_source"

oracle-arch-check:
	@set -eu; \
	$(check_oracle_arch)

build-ora: oracle-env
	@$(MAKE) oracle-arch-check DOCKER_STRICT=1
	@$(ORACLE_ENV) $(MAKE) build DOCKER_STRICT=1

build_ora: build-ora

docker-push:
	@set -e; \
	case "$(TAG)" in \
		v*.*.*rc*) stable="false" ;; \
		v*.*.*|*.*.*) stable="true" ;; \
		localtree|dev) stable="false" ;; \
		*) echo "TAG=$(TAG) is not localtree, dev, or a release tag"; exit 1 ;; \
	esac; \
	docker image inspect "$(IMAGE):$(TAG)" >/dev/null || { \
		echo "Docker image $(IMAGE):$(TAG) does not exist locally; run 'make docker build TAG=$(TAG)' first"; \
		exit 1; \
	}; \
	docker login "$(REGISTRY)"; \
	docker push "$(IMAGE):$(TAG)"; \
	if [ "$$stable" = "true" ]; then \
		docker tag "$(IMAGE):$(TAG)" "$(IMAGE):$(TAG)-stable"; \
		docker push "$(IMAGE):$(TAG)-stable"; \
	fi

upload-no-oracle:
	$(MAKE) upload no-oracle

k8deploy:
	@set -e; \
	[ -n "$${IMAGEBOT_URL}" ] || { echo "ERROR: IMAGEBOT_URL is not set"; exit 1; }; \
	curl -ksLO https://raw.githubusercontent.com/vkuznet/imagebot/main/imagebot.sh; \
	sed -i -e "s,COMMIT,$$(git rev-parse HEAD),g" \
		-e "s,REPOSITORY,$(REPO),g" \
		-e "s,NAMESPACE,$(IMAGEBOT_NAMESPACE),g" \
		-e "s,TAG,$(TAG),g" \
		-e "s,IMAGE,$(IMAGE),g" \
		-e "s,SERVICE,$(IMAGEBOT_SERVICE),g" \
		-e "s,HOST,$${IMAGEBOT_URL},g" imagebot.sh; \
	chmod +x imagebot.sh; \
	sh ./imagebot.sh

k8deploy-no-oracle: k8deploy


vet:
	go vet .

ORAFILES_ALL = web/server.go test/merge/main.go test/seq/seq.go test/http_test.go test/writer_test.go test/oracle_drivers_test.go test/seq/oracle_drivers_test.go test/merge/oracle_drivers_test.go cgotest/oracle_drivers.go
ORAFILES = $(wildcard $(ORAFILES_ALL))

strip_oracle:
	$(info ### on $(arch) platform there is no ORACLE libs, we will disable their drivers from the build)
	for f in $(ORAFILES); do \
		sed -i -e "s,_ \"github.com/mattn/go-oci8\",//_ \"github.com/mattn/go-oci8\",g" $$f; \
		sed -i -e "s,_ \"gopkg.in/rana/ora.v4\",//_ \"gopkg.in/rana/ora.v4\",g" $$f; \
		rm $$f-e; \
	done

restore_oracle: $(ORAFILES)
	$(info ### on $(arch) platform there is no ORACLE libs, we will restore them after the build)
	for f in $(ORAFILES); do \
		sed -i -e "s,//_ \"github.com/mattn/go-oci8\",_ \"github.com/mattn/go-oci8\",g" $$f; \
		sed -i -e "s,//_ \"gopkg.in/rana/ora.v4\",_ \"gopkg.in/rana/ora.v4\",g" $$f; \
		rm $$f-e; \
	done

ifeq (docker,$(firstword $(MAKECMDGOALS)))
build:
	@true
else
build:
	$(info ### building dbs2go executable on $(arch))
	go clean; rm -rf pkg dbs2go*; go build ${flags}
	@echo
endif

build_no_oracle: strip_oracle build restore_oracle
ifneq ($(DOCKER_STRICT),1)
.IGNORE:
endif

build-no-oracle: build_no_oracle

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
test: strip_oracle test_all restore_oracle
ifneq ($(DOCKER_STRICT),1)
.IGNORE:
endif
else
test: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-migrate test-writer test-integration test-lexicon bench
endif

test-github: test-dbs test-sql test-errors test-validator test-bulk test-http test-utils test-writer test-lexicon test-integration test-migration-requests test-migration bench

test-lexicon: test-lexicon-writer-pos test-lexicon-writer-neg test-lexicon-reader-pos test-lexicon-reader-neg

test-errors:
	cd test && LD_LIBRARY_PATH=${odir} DYLD_LIBRARY_PATH=${odir} go test -v -run TestDBSError
test-errors-no-oracle: strip_oracle test-errors restore_oracle
ifneq ($(DOCKER_STRICT),1)
.IGNORE:
endif
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
