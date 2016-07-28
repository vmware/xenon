FROM java:openjdk-8-jre-alpine

RUN apk add --update --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/ tini

ADD xenon-host-0.9.2-SNAPSHOT-jar-with-dependencies.jar ./opt/xenon/app.jar

ENTRYPOINT ["/sbin/tini", "--", "/start.sh"]

EXPOSE 8000
ENV XENON_HOME=/opt/xenon JAVA_OPTS= DEFAULT_OPTS= DEBUG_PORT= DEBUG_SUSPEND= VERBOSE_GC=

COPY logging.properties /opt/xenon/
COPY start.sh /
