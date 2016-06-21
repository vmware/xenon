#!/bin/bash

set -o pipefail
set -o nounset
set -o errexit

usage() {
  echo "Creates a Xenon cluster on a set of hosts."
  echo "Usage:\n setup-rpi-cluster.sh <hostAddresses> <port> <username> <password> <build-number>\n"
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

peerNodes=""
for host in ${hosts}; do
  peerNodes+="http://$host:$port,"
done

for host in ${hosts}; do
  sshpass -p ${password} ssh -T ${username}@${host} << SETUP_XENON_DIR
    mkdir -p ${buildDir}
  SETUP_XENON_DIR

  sshpass -p ${password} scp \
    ./xenon-host/target/xenon-host-*-SNAPSHOT-jar-with-dependencies.jar \
    ${username}@${host}:${buildDir}/.

  sshpass -p ${password} ssh -T ${username}@${host} << START_XENON
    java -jar ${buildDir}/xenon-host-*-SNAPSHOT-jar-with-dependencies.jar \
         --port=${port} \
         --bindAddress=${host} \
         --sandbox=${buildDir} \
         --peerNodes=${peerNodes} &>/dev/null &
  START_XENON

  echo "Xenon started on $host"
done
