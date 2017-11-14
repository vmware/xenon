#!/bin/sh

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


if [ -z $BINDING_ADDRESS ]
then
  BINDING_ADDRESS="0.0.0.0"
fi
echo "Setting bind address to: $BINDING_ADDRESS"

if [ -z $BINDING_PORT ]
then
  BINDING_PORT=8000
fi

if [ -z $ADMIN_PASSWORD ]
then
  ADMIN_PASSWORD=vmw@r3
fi

echo "Setting port to: $BINDING_PORT"

if [ -n "$JMX_PORT" ]
then
  echo "Enabling JMX connection on port: $JMX_PORT"
  JAVA_OPTS="${JAVA_OPTS:-} -Dcom.sun.management.jmxremote \
                            -Dcom.sun.management.jmxremote.port=$JMX_PORT \
                            -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
                            -Djava.rmi.server.hostname=127.0.0.1 \
                            -Dcom.sun.management.jmxremote.ssl=false \
                            -Dcom.sun.management.jmxremote.local.only=false \
                            -Dcom.sun.management.jmxremote.authenticate=false"
fi

if [ -n "$JVM_HEAP" ]
then
  echo "Setting heap to $JVM_HEAP"
  JAVA_OPTS="${JAVA_OPTS:-} -Xms$JVM_HEAP -Xmx$JVM_HEAP"
fi

if [ -n "$JVM_METASPACE" ]
then
  echo "Setting metaspace to $JVM_METASPACE"
  JAVA_OPTS="${JAVA_OPTS:-} -XX:MetaspaceSize=$JVM_METASPACE -XX:MaxMetaspaceSize=$JVM_METASPACE"
fi

java ${DEFAULT_OPTS} \
     ${JAVA_OPTS:-} \
     -Djava.util.logging.config.file=$XENON_HOME/logging.properties \
     -jar $XENON_HOME/test.jar \
     --bindAddress=$BINDING_ADDRESS --port=$BINDING_PORT --sandbox=$XENON_HOME/sandbox "$@" \
     --adminPassword=$ADMIN_PASSWORD \
     --id=$HOSTNAME