GOVENDOR ?= $(CURDIR)/_vendor
GOPATH := $(GOVENDOR):$(CURDIR)
GO ?= go
GOVERSION ?= go1.5
GO_FOUND ?= 0
OS := $(shell uname)

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

FOUND_GO = $(shell $(GO) version)
export FOUND_GO
export GOPATH

all: goversion deps verify

ifneq ($(FOUND_GO),)
verify: goversion goimports govet golint
else
verify:
endif

ifneq ($(FOUND_GO),)
deps:
	$(GO) get -d golang.org/x/tools/cmd/goimports
	@if [ -w "$(shell $(GO) env GOTOOLDIR)" ] ; then \
		echo "$(GO) get golang.org/x/tools/cmd/vet"; \
		$(GO) get -d golang.org/x/tools/cmd/vet; \
	fi;
	$(GO) get -d github.com/golang/lint/golint
	$(GO) get -d github.com/pborman/uuid
else
deps:
endif

ifneq ($(FOUND_GO),)
goversion:
	@( $(GO) version | grep -q $(GOVERSION) ) || ( echo "Please install $(GOVERSION) to build this module (found: $$($(GO) version))" && exit 1 )
else
goversion:
endif

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
