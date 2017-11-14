FROM java:openjdk-8-jre-alpine

RUN apk add --update --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/ tini

RUN mkdir -p /opt/xenon
ADD PATH/TO/YOUR/JAR /opt/xenon/test.jar
ADD start.sh /opt/xenon
ADD logging.properties /opt/xenon/

WORKDIR /opt/xenon

ENTRYPOINT ["/sbin/tini", "--", "/start.sh"]
