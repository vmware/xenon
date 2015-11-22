DCP_IMAGE=dcp
COMMIT_TAG ?= $(shell git rev-parse --short HEAD)

.PHONY: docker tag

docker:
	mvn package -DskipTests
	mkdir -p docker/base/lib
	cp xenon-host/target/*-with-dependencies.jar docker/base/lib/xenon.jar
	mkdir -p docker/base/bin
	mkdir -p docker/base/etc
	cp contrib/xenonHostLogging.config docker/base/etc/logging.config
	( cd docker && docker build --tag=$(DCP_IMAGE):$(COMMIT_TAG) . )

docker-tag:
	@echo $(COMMIT_TAG)
