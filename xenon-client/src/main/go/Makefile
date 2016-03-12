GOVENDORCOMMON := $(CURDIR)/../../../../xenon-common/src/main/go/_vendor/
GOVENDOR :=$(CURDIR)/_vendor
GOPATH := $(GOVENDOR):$(GOVENDORCOMMON):$(CURDIR)
GO ?= go
OS := $(shell uname)

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

FOUND_GO = $(shell $(GO) version)
export FOUND_GO
export GOPATH

all: deps verify test

ifneq ($(FOUND_GO),)
deps:
	$(GO) get -d github.com/ghodss/yaml
	$(GO) get -d github.com/cloudfoundry-incubator/candiedyaml
else
deps:
endif

ifneq ($(FOUND_GO),)
verify: goversion goimports govet golint
else
verify:
endif

ifneq ($(FOUND_GO),)
goversion:
	@( $(GO) version | grep -q $(GOVERSION) ) || ( echo "Please install $(GOVERSION) to build this module (found: $$($(GO) version))" && exit 1 )
else
goversion:
endif

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

ifneq ($(FOUND_GO),)
test:
	$(GO) test $(TEST_OPTS) ./...
else
test:
endif

ifneq ($(FOUND_GO),)
gobuild:
	GOOS=linux $(GO) build -o bin/xenonc-linux -pkgdir ./pkg xenon/client
	GOOS=darwin $(GO) build -o bin/xenonc-darwin -pkgdir ./pkg xenon/client
else
gobuild:
endif

ifneq ($(FOUND_GO),)
package:
	mkdir -p ../resources/linux/bin
	mkdir -p ../resources/darwin/bin
	cp bin/xenonc-linux ../resources/linux/bin/xenonc
	cp bin/xenonc-darwin ../resources/darwin/bin/xenonc
else
package:
endif

check-static:
ifeq ($(OS), Linux)
	! ldd $(CURDIR)/bin/*
endif

ifneq ($(FOUND_GO),)
clean:
	rm -rf pkg bin $(GOVENDOR)/pkg $(GOVENDOR)/bin $(DIST_DIR)
else
clean:
endif

# For use by external tools such as emacs or for example:
# GOPATH=$(make gopath) go get ...
gopath:
	@echo -n $(GOPATH)
