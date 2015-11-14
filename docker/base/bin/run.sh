#!/bin/sh -e

# Workaround lock bug
rm -f /var/log/xenonHost.*.log.lck

# Be receptive to core dumps
ulimit -c unlimited

# Allow high connection count per process (raise file descriptor limit)
ulimit -n 65536

# Extend JAVA_OPTS with defaults
JAVA_OPTS="${JAVA_OPTS:-} -Dfile.encoding=UTF-8"

mkdir -p /tmp
cd /opt/xenon
export PATH=$PWD/bin:$PATH

echo "$(date): starting jvm"

status=0
set +e
/opt/jre/bin/java ${JAVA_OPTS} -jar lib/xenon.jar "$@"
status=$?
set -e

echo "$(date): jvm exited with status code ${status}"
exit ${status}
