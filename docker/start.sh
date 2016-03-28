#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

XENON_HOME=/opt/xenon

mkdir -p $XENON_HOME/log $XENON_HOME/hprof $XENON_HOME/sandbox || true

rm -fr $XENON_HOME/log/host.*.log.lck

# Be receptive to core dumps
ulimit -c unlimited

# Allow high connection count per process (raise file descriptor limit)
ulimit -n 65536

if [ "${DEFAULT_OPTS:-}" == "" ]; then
DEFAULT_OPTS="\
-server \
-showversion \
-XshowSettings \
-XX:+UseStringDeduplication \
-XX:-OmitStackTraceInFastThrow \
-XX:+UnlockExperimentalVMOptions \
-XX:+UnlockDiagnosticVMOptions \
-XX:+PrintCommandLineFlags \
-XX:+PrintFlagsFinal \
-XX:ErrorFile=$XENON_HOME/hprof/error_xenon_%p.log \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=$XENON_HOME/hprof \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=100 \
-XX:InitiatingHeapOccupancyPercent=60 \
-Dsun.io.useCanonCaches=false \
-Djava.awt.headless=true \
-Dfile.encoding=UTF-8 \
"
fi

if [ "${VERBOSE_GC:-}" != "" ];then
DEFAULT_OPTS="$DEFAULT_OPTS \
-Xloggc:$XENON_HOME/log/gc.log \
-verbose:gc \
-verbose:sizes \
-XX:+PrintStringDeduplicationStatistics \
-XX:+PrintAdaptiveSizePolicy \
-XX:+PrintTenuringDistribution \
-XX:GCLogFileSize=20M \
-XX:+UseGCLogFileRotation \
-XX:NumberOfGCLogFiles=10 \
-XX:+PrintGCTimeStamps \
-XX:+PrintGCDateStamps \
-XX:+PrintGCDetails \
"
fi

if [ "${DEBUG_PORT:-}" != "" ]; then
  DEFAULT_OPTS="$DEFAULT_OPTS -agentlib:jdwp=transport=dt_socket,server=y,address=$DEBUG_PORT"

  if [ ${DEBUG_SUSPEND:-n} == "n" ]; then
    DEFAULT_OPTS="$DEFAULT_OPTS,suspend=n"
  else
    DEFAULT_OPTS="$DEFAULT_OPTS,suspend=y"
  fi
fi

if [ "${JMX_PORT:-}" != "" ]; then
DEFAULT_OPTS="$DEFAULT_OPTS \
-Dcom.sun.management.jmxremote= \
-Dcom.sun.management.jmxremote.port=$JMX_PORT \
-Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
"
fi


java ${DEFAULT_OPTS} \
     ${JAVA_OPTS:-} \
     -Djava.util.logging.config.file=$XENON_HOME/logging.properties \
     -jar $XENON_HOME/app.jar \
     --bindAddress=0.0.0.0 --port=8000 --sandbox=$XENON_HOME/sandbox "$@"
