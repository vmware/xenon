FROM java:openjdk-8-jre-alpine

ENTRYPOINT ["/start.sh"]
EXPOSE 8000
ENV XENON_HOME=/opt/xenon JAVA_OPTS= DEFAULT_OPTS= DEBUG_PORT= DEBUG_SUSPEND= VERBOSE_GC=

COPY logging.properties /opt/xenon/
COPY start.sh /
