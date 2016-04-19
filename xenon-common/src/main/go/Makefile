GOVENDOR ?= $(CURDIR)/_vendor
TMPDIR := $(CURDIR)/_tmp
GOPATH := $(GOVENDOR):$(CURDIR)
GO ?= go
GO1.5 ?= go1.5
GO1.6 ?= go1.6

GO_FOUND ?= 0
OS := $(shell uname)

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

FOUND_GO = $(shell $(GO) version)
export FOUND_GO
export GOPATH
export TMPDIR

all: goversion deps verify

ifneq ($(FOUND_GO),)
verify: goversion goimports golint
else
verify:
endif

ifneq ($(FOUND_GO),)
deps:
	$(GO) get -d golang.org/x/tools/cmd/goimports
	$(GO) get -d github.com/golang/lint/golint
	$(GO) get -d github.com/pborman/uuid
else
deps:
endif

ifneq ($(FOUND_GO),)
goversion:
	@( $(GO) version | grep -q '$(GO1.5)\|$(GO1.6)' ) || ( echo "Please install $(GO1.5) or $(GO1.6) to build this module (found: $$($(GO) version))" && exit 1 )
else
goversion:
endif

goimports:
	$(GO) install golang.org/x/tools/cmd/goimports

golint:
	$(GO) install github.com/golang/lint/golint

clean:
	rm -rf $(GOVENDOR)/pkg $(GOVENDOR)/bin

# For use by external tools such as emacs or for example:
# GOPATH=$(make gopath) go get ...
gopath:
	@echo -n $(GOPATH)
