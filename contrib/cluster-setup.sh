#!/bin/bash

set -o pipefail
set -o nounset
set -o errexit

# The script creates a Xenon cluster by running xenon-host
# on the provided set of host addresses.
#
# The script makes the following assumptions about the
# environment it is being run on:
# - The script is kicked off from the xenon root directory
# - xenon-host package is built and can be found under
#   /xenon/xenon-host/target/.
# - The provided username/password credentials are valid
#   for all provided hosts.
# - The script is kicked off on a machine that has sshpass
#   installed locally.
# - Each host has ssh access enabled
# - Each host has NTP daemon installed and running.

usage() {
  echo "Creates a Xenon cluster on the provided set of hosts."
  echo "\nUsage:\n cluster-setup.sh <hostAddresses> <port> <username> <password> <buildNumber>\n"
  echo "hostAddresses: Space separated string of IP addresses"
  echo "port: Port number used for the xenon-host"
  echo "username: SSH username for the remote machines"
  echo "password: SSH password for the remote machines"
  echo "buildNumber: Used for sandboxing this cluster from any other cluster that may already exist"
  echo "\nSample:\n cluster-setup.sh \"10.0.0.5 10.0.0.6 10.0.0.7\" 8000 dars p@ssw0rd! 3245"
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
