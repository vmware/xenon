GOVENDORCOMMON := $(CURDIR)/../../../../xenon-common/src/main/go/_vendor/
GOVENDOR :=$(CURDIR)/_vendor
GOPATH := $(GOVENDOR):$(GOVENDORCOMMON):$(CURDIR)/../../../../xenon-common/src/main/go/:$(CURDIR)
GO ?= go
OS := $(shell uname)

ifeq ($(OS), Linux)
CGO_ENABLED = 0
export CGO_ENABLED
endif

export GOPATH

all: check deps verify test

check:
	@if [ -w "$(shell command -v $(GO))" ] ; then \
		echo "GO found"; \
	fi;

deps:
	$(GO) get -d github.com/miekg/dns
	$(GO) get -d github.com/mitchellh/mapstructure

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

xenon-dns.darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(GO) build -o bin/xenon-dnsserver-darwin -pkgdir ./pkg xenon/dns/cmd

xenon-dns.linux-amd64:
	GOOS=linux GOARCH=amd64 $(GO) build -o bin/xenon-dnsserver-linux -pkgdir ./pkg xenon/dns/cmd

build: xenon-dns.darwin-amd64 xenon-dns.linux-amd64

package:
	mkdir -p ../resources/bin/
	cp bin/xenon-dnsserver-darwin bin/xenon-dnsserver-linux ../resources/bin/

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
