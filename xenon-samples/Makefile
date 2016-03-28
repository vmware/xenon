TAG ?= latest
IMAGE ?= vmware/xenon-samples
CONT ?= vmware_xenon-samples
REMOTE ?=


build:
	docker build -t $(IMAGE):$(TAG) .

mvn:
	cd .. && ./mvnw clean package -DskipTests=true -DskipGO=true -DskipAnalysis

push: build
	docker push $(IMAGE):$(TAG)

run: rm build
	docker run -td -p 8000 -e VERBOSE_GC=1 --name $(CONT) $(IMAGE) $(ARGS)
	@make info

debug: rm build
	docker run -td -p 8000 -p 5055 -p 1898 -e DEBUG_PORT=5055 -e JMX_PORT=1898 --name $(CONT) $(IMAGE) $(ARGS)
	@make info

rm: stop
	docker rm $(CONT) || true

attach:
	docker exec -ti $(CONT) /bin/bash

stop:
	docker stop -t 3 $(CONT) || docker kill $(CONT) || true

logs:
	docker logs -f $(CONT)

shell:
	@docker run --rm -ti -v `pwd`/..:/src --entrypoint=/bin/bash $(IMAGE)

inspect:
	@docker inspect $(CONT)

info:
	@echo -n "XENON: "
	@docker inspect --format='http://{{.NetworkSettings.IPAddress}}:8000/' $(CONT)
	@echo -n "XENON: http://localhost:"
	@docker inspect --format='{{(index (index .NetworkSettings.Ports "8000/tcp") 0).HostPort }}/' $(CONT)
	@echo -n DEBUG: localhost:
	@docker inspect --format='{{(index (index .NetworkSettings.Ports "5055/tcp") 0).HostPort }}' $(CONT) 2> /dev/null || echo -1
