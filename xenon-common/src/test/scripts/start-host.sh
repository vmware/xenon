#!/bin/sh
#
# Start ServiceHost using maven exec plugin
# To pass system arguments, use "MAVEN_OPTS"
#
# command example:
#    export MAVEN_OPTS=" \
#       -server  \
#       -XX:+HeapDumpOnOutOfMemoryError   \
#       -XX:+CrashOnOutOfMemoryError   \
#       -XX:HeapDumpPath=${WORKSPACE}/hprof/  \
#       -XX:MaxMetaspaceSize=256m \
#    " \
#    ./xenon-common/src/main/scripts/start-host.sh   \
#       ${SANDBOX_DIR} \
#       "com.vmware.xenon.services.common.stress.MigrationStressTest\$MigrationHost" \
#       ${SOURCE_PORT}  \
#       "" \
#       "performance, ci" &
#


SANDBOX=$1
MAIN_CLASS=$2
PORT=$3
EXEC_ARGUMENTS=$4
MAVEN_PROFILE=$5

./mvnw -B -e exec:java \
  -pl xenon-common \
  -Dexec.classpathScope=test \
  -Dexec.arguments="${EXEC_ARGUMENTS}" \
  -Dexec.mainClass="${MAIN_CLASS}" \
  -Dxenon.port=${PORT}  \
  -Dxenon.sandbox=${SANDBOX}  \
  -P "${MAVEN_PROFILE}"
