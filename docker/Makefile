TAG ?= latest
IMAGE ?= vmware/xenon-base

build:
	docker build -t $(IMAGE):$(TAG) .
	docker tag -f   $(IMAGE):$(TAG) $(IMAGE):latest

push: build
	docker push $(IMAGE):$(TAG)

shell:
	@docker run --rm -ti -v `pwd`:/src --entrypoint=/bin/bash $(IMAGE)

