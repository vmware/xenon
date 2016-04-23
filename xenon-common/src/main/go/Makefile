GOVENDOR ?= $(CURDIR)/_vendor
TMPDIR := $(CURDIR)/_tmp
GOPATH := $(GOVENDOR):$(CURDIR)
GOBIN := $(GOVENDOR)/bin
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

all: goversion deps verify test

ifneq ($(FOUND_GO),)
verify: goversion goimports govet golint
else
verify:
endif

ifneq ($(FOUND_GO),)
deps:
	$(GO) get -d golang.org/x/tools/cmd/goimports
	$(GO) get -d github.com/golang/lint/golint
	$(GO) get -d github.com/pborman/uuid
	$(GO) get -d github.com/golang/glog
	$(GO) get -d golang.org/x/net/context
	$(GO) get -d github.com/pborman/uuid
	$(GO) get -d golang.org/x/crypto/ssh
	$(GO) get -d golang.org/x/crypto/ssh/testdata
	$(GO) get -d github.com/golang/groupcache/singleflight
	$(GO) get -d github.com/stretchr/testify/assert
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
	@echo checking go imports
	@! $(GOVENDOR)/bin/goimports -d src 2>&1 | egrep -v '^$$'

golint:
	$(GO) install github.com/golang/lint/golint
	@echo checking golint...
	@$(GOVENDOR)/bin/golint ./... | sh -c '! grep -v comment'

govet:
	@echo checking go vet...
	@! $(GO) vet ./... 2>&1 | egrep -v '^$$'

ifneq ($(FOUND_GO),)
test:
	$(GO) test $(TEST_OPTS) ./...
else
test:
endif

ifneq ($(FOUND_GO),)
gobuild:
	GOOS=linux $(GO) build -o bin/xenon-host-linux -pkgdir ./pkg xenon/cmd/xenongo
	GOOS=darwin $(GO) build -o bin/xenon-host-darwin -pkgdir ./pkg xenon/cmd/xenongo
else
gobuild:
endif

ifneq ($(FOUND_GO),)
package:
	mkdir -p ../resources/linux/bin
	mkdir -p ../resources/darwin/bin
	cp bin/xenon-host-linux ../resources/linux/bin/xenon-host
	cp bin/xenon-host-darwin ../resources/darwin/bin/xenon-host
else
package:
endif

clean:
	rm -rf $(GOVENDOR)/pkg $(GOVENDOR)/bin

# For use by external tools such as emacs or for example:
# GOPATH=$(make gopath) go get ...
gopath:
	@echo -n $(GOPATH)
