GOVENDORCOMMON := $(CURDIR)/../../../../xenon-common/src/main/go/_vendor/
GOVENDOR :=$(CURDIR)/_vendor
GOPATH := $(GOVENDOR):$(GOVENDORCOMMON):$(CURDIR)
GO ?= go
OS := $(shell uname)

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

export GOPATH

all: deps verify test

deps:
	$(GO) get -d github.com/ghodss/yaml
	$(GO) get -d github.com/cloudfoundry-incubator/candiedyaml

verify: goimports govet golint

goimports:
	$(GO) install golang.org/x/tools/cmd/goimports
	@echo checking go fmt...
	@! $(GOVENDORCOMMON)/bin/goimports -d src 2>&1 | egrep -v '^$$'

govet:
	@if [ -w "$(shell $(GO) env GOTOOLDIR)" ] ; then \
		echo "$(GO) install golang.org/x/tools/cmd/vet"; \
		$(GO) install golang.org/x/tools/cmd/vet; \
	fi;
	@echo checking go vet...
	@! $(GO) vet ./... 2>&1 | egrep -v '^$$'

golint:
	$(GO) install github.com/golang/lint/golint
	@echo checking golint...
	@$(GOVENDORCOMMON)/bin/golint ./... | sh -c '! grep -v comment'

errcheck:
	$(GO) install github.com/kisielk/errcheck
	@echo checking goerrcheck...
	@$(GOVENDORCOMMON)/bin/errcheck ./...

test:
	$(GO) test $(TEST_OPTS) ./...

xenon-client:
	$(GO) build -o bin/xenonc -pkgdir ./pkg xenon/client

build: xenon-client

package:
	mkdir -p ../resources/bin/
	cp bin/xenonc ../resources/bin/

check-static:
ifeq ($(OS), Linux)
	! ldd $(CURDIR)/bin/*
endif

clean:
	rm -rf pkg bin $(GOVENDOR)/pkg $(GOVENDOR)/bin $(DIST_DIR)

# For use by external tools such as emacs or for example:
# GOPATH=$(make gopath) go get ...
gopath:
	@echo -n $(GOPATH)
