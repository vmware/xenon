#!/bin/bash

set -o pipefail
set -o nounset
set -o errexit

usage() {
  echo "Deletes a Xenon cluster setup using ./setup-rpi-cluster.sh"
  echo "Usage:\n delete-rpi-cluster.sh <hostAddresses> <port> <username> <password> <build-number>\n"
}

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ]
then
  echo "one or more required parameters are missing"
  usage
  exit 1
fi

hosts=$1
port=$2
username=$3
password=$4
buildNumber=$5
buildDir=xenon/${buildNumber}_${port}

for host in ${hosts}; do
  sshpass -p ${password} ssh -T ${username}@${host} << CLEAN_XENON_DIR
    ps -ef | grep "xenon-host" | grep ${buildDir} | grep -v "grep" | awk '{print \$2}' | xargs -r kill
    rm -rf ${buildDir}
  CLEAN_XENON_DIR
done
