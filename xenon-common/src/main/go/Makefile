GOVENDOR ?= $(CURDIR)/_vendor
GOPATH := $(GOVENDOR):$(CURDIR)
GO ?= go
GOVERSION ?= go1.5
GO_FOUND ?= 0
OS := $(shell uname)

ifneq ($(shell $(GO) version ; echo $?), 0)
GO_FOUND = 1
export GO_FOUND
endif

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

export GOPATH

all: goversion deps verify

verify: goversion goimports govet golint

deps:
	$(GO) get -d golang.org/x/tools/cmd/goimports
	@if [ -w "$(shell $(GO) env GOTOOLDIR)" ] ; then \
		echo "$(GO) get golang.org/x/tools/cmd/vet"; \
        $(GO) get -d golang.org/x/tools/cmd/vet; \
	fi;
	$(GO) get -d github.com/golang/lint/golint
	$(GO) get -d github.com/pborman/uuid

goversion:
  @( $(GO) version | grep -q $(GOVERSION) ) || ( echo "Please install $(GOVERSION) to build this module (found: $$($(GO) version))" && exit 1 )

goimports:
	$(GO) install golang.org/x/tools/cmd/goimports

govet:
	@if [ -w "$(shell $(GO) env GOTOOLDIR)" ] ; then \
		echo "$(GO) install golang.org/x/tools/cmd/vet"; \
		$(GO) install golang.org/x/tools/cmd/vet; \
	fi;

golint:
	$(GO) install github.com/golang/lint/golint

clean:
	rm -rf $(GOVENDOR)/pkg $(GOVENDOR)/bin

# For use by external tools such as emacs or for example:
# GOPATH=$(make gopath) go get ...
gopath:
	@echo -n $(GOPATH)
