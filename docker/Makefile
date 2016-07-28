TAG ?= latest
IMAGE ?= vmware/xenon-base
HOSTPORT ?= 8000
XENONPORT ?= 8000
build:
	docker build -t $(IMAGE):$(TAG) .
	docker tag   $(IMAGE):$(TAG) $(IMAGE):latest

push: build
	docker push $(IMAGE):$(TAG)

shell:
	@docker run --rm -p $(HOSTPORT):$(XENONPORT)  -ti -v `pwd`:/src --entrypoint=/bin/sh $(IMAGE)

