#!/bin/sh
#
# Start ServiceHost using maven exec plugin
#
# To pass system argument, use MAVEN_OPTS
#


# TODO: add usage

SANDBOX=$1
MAIN_CLASS=$2
PORT=$3
EXEC_ARGUMENTS=$4
MAVEN_PROFILE=$5

./mvnw -B -e exec:java \
  -pl xenon-stress-test \
  -Dexec.classpathScope=test \
  -Dexec.arguments="${EXEC_ARGUMENTS}" \
  -Dexec.mainClass="${MAIN_CLASS}" \
  -Dxenon.port=${PORT}  \
  -Dxenon.sandbox=${SANDBOX}  \
  -P "${MAVEN_PROFILE}"
