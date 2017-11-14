TAG ?= latest
IMAGE ?= vmware/xenon-base
JAR ?= xenon-host-*-SNAPSHOT-jar-with-dependencies.jar
POM_PATH ?= ../xenon-host/
JAR_PATH ?= $(POM_PATH)/target/$(JAR)

build:
	../mvnw clean package -f $(POM_PATH)
	cp $(JAR_PATH) .
	docker build -t $(IMAGE):$(TAG) .
	docker tag      $(IMAGE):$(TAG) $(IMAGE):latest
	rm $(JAR)

push: build
	docker push $(IMAGE):$(TAG)

shell:
	@docker run --rm -ti -v `pwd`:/src --entrypoint=/bin/sh $(IMAGE)

