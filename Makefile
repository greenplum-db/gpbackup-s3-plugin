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
DEP=$(GOPATH)/bin/dep
GOLANG_LINTER=$(GOPATH)/bin/golangci-lint
GINKGO=$(GOPATH)/bin/ginkgo

LINTER_VERSION=1.16.0
$(GOLANG_LINTER) :
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v${LINTER_VERSION}

depend :
		go mod download

$(GINKGO) :
		go get github.com/onsi/ginkgo

format :
		goimports -w .
		gofmt -w -s .

lint : $(GOLANG_LINTER)
		golangci-lint run --tests=false

unit : depend $(GINKGO)
		ginkgo -r -randomizeSuites -noisySkippings=false -randomizeAllSpecs s3plugin 2>&1

test : unit

build : depend
		go build -tags '$(S3_PLUGIN)' -o $(BIN_DIR)/$(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

build_linux : depend
		env GOOS=linux GOARCH=amd64 go build -tags '$(S3_PLUGIN)' -o $(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

build_mac : depend
		env GOOS=darwin GOARCH=amd64 go build -tags '$(S3_PLUGIN)' -o $(BIN_DIR)/$(S3_PLUGIN) -ldflags $(PLUGIN_VERSION_STR)

install : build
		@$(MAKE) install_plugin

install_plugin :
		@psql -t -d template1 -c 'select distinct hostname from gp_segment_configuration' > /tmp/seg_hosts 2>/dev/null; \
		if [ $$? -eq 0 ]; then \
			gpscp -f /tmp/seg_hosts $(BIN_DIR)/$(S3_PLUGIN) =:$(GPHOME)/bin/$(S3_PLUGIN); \
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
