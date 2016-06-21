#!/bin/bash

set -o pipefail
set -o nounset
set -o errexit

usage() {
  echo "Stops a Xenon cluster setup using ./cluster-setup.sh and deletes the environment."
  echo "\nUsage:\n cluster-cleanup.sh <hostAddresses> <port> <username> <password> <build-number>\n"
  echo "See help of ./cluster-setup.sh for more details about passed parameters."
}

if [ -z "$1" ] || [ -z $2 ] || [ -z $3 ] || [ -z $4 ] || [ -z $5 ]
then
  echo "one or more required parameters are missing"
  usage
  exit 1
fi

hosts="$1"
port=$2
username=$3
password=$4
buildNumber=$5
buildDir=xenon/${buildNumber}_${port}

for host in ${hosts}; do
# Determine if the xenon-host was already running on this host
# and check if the host was running in the same sandbox. If so,
# stop the xenon-host and delete the sandbox folder.
sshpass -p ${password} ssh -T ${username}@${host} << CLEAN_XENON_DIR
  ps -ef | grep "xenon-host" | grep ${buildDir} | grep -v "grep" | awk '{print \$2}' | xargs -r kill
  rm -rf ${buildDir}
CLEAN_XENON_DIR

echo "Xenon stopped on $host"
done
