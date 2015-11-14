# This image is built by Dockerfile in machines repository at docker/jre8.
FROM jre8-minimal:latest

ADD root /

RUN mkdir -p /opt/xenon
ADD base /opt/xenon

RUN echo "hosts: files dns" > /etc/nsswitch.conf

WORKDIR /opt/xenon
CMD bin/run.sh
