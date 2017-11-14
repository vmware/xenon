TAG ?= latest
IMAGE ?= vmware/xenon-base
JAR ?= xenon-host-*-SNAPSHOT-jar-with-dependencies.jar
JAR_PATH ?= ../xenon-host/target/$(JAR)

build:
	cp $(JAR_PATH) .
	docker build -t $(IMAGE):$(TAG) .
	docker tag      $(IMAGE):$(TAG) $(IMAGE):latest
	rm $(JAR)

push: build
	docker push $(IMAGE):$(TAG)

shell:
	@docker run --rm -ti -v `pwd`:/src --entrypoint=/bin/sh $(IMAGE)

