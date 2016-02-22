DCP_IMAGE=dcp
COMMIT_TAG ?= $(shell git rev-parse --short HEAD)

.PHONY: mvn docker

mvn:
	mvn package -DskipTests

docker:
	mkdir -p docker/base/lib
	bash -c "cp xenon-host/target/xenon-host-*[^sources].jar docker/base/lib/xenon.jar"
	mkdir -p docker/base/bin
	mkdir -p docker/base/etc
	cp contrib/xenonHostLogging.config docker/base/etc/logging.config
	cd docker && docker build --tag=$(DCP_IMAGE):$(COMMIT_TAG) .

docker-tag:
	@echo $(COMMIT_TAG)
