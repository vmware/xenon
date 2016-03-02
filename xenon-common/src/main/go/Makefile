GOVENDOR ?= $(CURDIR)/_vendor
GOPATH := $(CURDIR):$(GOVENDOR)
GO ?= go
GOVERSION ?= go1.5
OS := $(shell uname)

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

export GOPATH

all: deps verify test

verify: goversion goimports govet golint

deps:
	GOPATH=$(GOVENDOR) $(GO) get golang.org/x/tools/cmd/goimports
	GOPATH=$(GOVENDOR) $(GO) get golang.org/x/tools/cmd/vet
	GOPATH=$(GOVENDOR) $(GO) get github.com/golang/lint/golint
	GOPATH=$(GOVENDOR) $(GO) get github.com/golang/glog
	GOPATH=$(GOVENDOR) $(GO) get golang.org/x/net/context
	GOPATH=$(GOVENDOR) $(GO) get github.com/pborman/uuid
	GOPATH=$(GOVENDOR) $(GO) get github.com/stretchr/testify/assert

goversion:
	@( $(GO) version | grep -q $(GOVERSION) ) || ( echo "Please install $(GOVERSION) (found: $$($(GO) version))" && exit 1 )

goimports:
	$(GO) install golang.org/x/tools/cmd/goimports
	@echo checking go fmt...
	@! $(GOVENDOR)/bin/goimports -d src 2>&1 | egrep -v '^$$'

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
	@$(GOVENDOR)/bin/golint ./... | sh -c '! grep -v comment'

errcheck:
	$(GO) install github.com/kisielk/errcheck
	@echo checking goerrcheck...
	@$(GOVENDOR)/bin/errcheck ./...

govc:
	$(GO) install github.com/vmware/govmomi/govc

test:
	$(GO) test $(TEST_OPTS) ./...

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
