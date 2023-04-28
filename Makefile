all: build

ifndef GOPATH
$(error Environment variable GOPATH is not set)
endif

SHELL := /bin/bash
.DEFAULT_GOAL := all
S3_PLUGIN=gpbackup_s3_plugin
DIR_PATH=$(shell dirname `pwd`)
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')

GIT_VERSION := $(shell git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
PLUGIN_VERSION_STR="-X github.com/greenplum-db/gpbackup-s3-plugin/s3plugin.Version=$(GIT_VERSION)"
GOLANG_LINTER=$(GOPATH)/bin/golangci-lint
GINKGO=$(GOPATH)/bin/ginkgo
GOIMPORTS=$(GOPATH)/bin/goimports
GO_ENV=GO111MODULE=on # ensure the project still compiles in $GOPATH/src using golang versions 1.12 and below
DEBUG=-gcflags=all="-N -l"

# Prefer gpsync as the newer utility, fall back to gpscp if not present (older installs)
ifeq (, $(shell which gpsync))
COPYUTIL=gpscp
else
COPYUTIL=gpsync
endif

LINTER_VERSION=1.16.0
$(GOLANG_LINTER) :
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v${LINTER_VERSION}

depend :
		$(GO_ENV) go mod download

$(GINKGO) :
		$(GO_ENV) go install github.com/onsi/ginkgo/v2/ginkgo@latest

$(GOIMPORTS) :
		$(GO_ENV) go install golang.org/x/tools/cmd/goimports

format : $(GOIMPORTS)
		goimports -w .
		gofmt -w -s .

lint : $(GOLANG_LINTER)
		golangci-lint run --tests=false

unit : depend $(GINKGO)
		$(GO_ENV) ginkgo -r --keep-going --randomize-suites --randomize-all --no-color s3plugin 2>&1

test : unit

debug : depend
		$(GO_ENV) go build $(DEBUG) -o $(BIN_DIR)/$(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

build : depend
		$(GO_ENV) go build -o $(BIN_DIR)/$(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

build_linux : depend
		env GOOS=linux GOARCH=amd64 $(GO_ENV) go build -o $(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

build_mac : depend
		env GOOS=darwin GOARCH=amd64 $(GO_ENV) go build -o $(BIN_DIR)/$(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

install : build
		@psql -t -d template1 -c 'select distinct hostname from gp_segment_configuration' > /tmp/seg_hosts 2>/dev/null; \
		if [ $$? -eq 0 ]; then \
			$(COPYUTIL) -f /tmp/seg_hosts $(BIN_DIR)/$(S3_PLUGIN) =:$(GPHOME)/bin/$(S3_PLUGIN); \
			if [ $$? -eq 0 ]; then \
				echo 'Successfully copied gpbackup_s3_plugin to $(GPHOME) on all segments'; \
			else \
				echo 'Failed to copy gpbackup_s3_plugin to $(GPHOME)'; \
			fi; \
		else \
			echo 'Database is not running, please start the database and run this make target again'; \
		fi; \
		rm /tmp/seg_hosts

clean :
		# Build artifacts
		rm -f $(BIN_DIR)/$(S3_PLUGIN)
		# Test artifacts
		rm -rf /tmp/go-build*
		rm -rf /tmp/gexec_artifacts*
		rm -rf /tmp/ginkgo*
