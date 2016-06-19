#!/bin/bash

usage() {
  echo "Usage:\n $0 [host-address] [peer-address] [build-number]\n"
}

if [ -z "$1" ]
then
  echo "host address is not passed"
  usage
  exit 1
fi

if [ -z "$2" ]
then
  echo "peer host address is not passed"
  usage
  exit 1
fi

if [ -z "$3" ]
then
  echo "build-number is not passed"
  usage
  exit 1
fi

set -o pipefail
set -o nounset
set -o errexit

address=$1
peerHostAddress=$2
username=pi
password=VMware!
port=8000
build_dir=xenon/$3

### Create necessary directory structure ###
sshpass -p $password ssh -T $username@$address << CREATE_BUILD_FOLDER

  # Kill xenon-host just in-case it is running on the remote machine.
  ps -ef | grep "xenon-host" | grep -v "grep" | awk '{print \$2}' | xargs -r kill

  # Remove the build folder if it already exists.
  rm -rf $build_dir

  # Create a new folder for the build.
  mkdir -p $build_dir

CREATE_BUILD_FOLDER

### Copy Xenon bits on the remote machine ###
sshpass -p $password scp \
  ./xenon-host/target/xenon-host-*-SNAPSHOT-jar-with-dependencies.jar \
  $username@$address:$build_dir/.


### Kick-off Xenon-host ###
sshpass -p $password ssh -T $username@$address << START_XENON_HOST

  # Kill xenon-host just in-case it is running on the remote machine.
  java -jar $build_dir/xenon-host-*-SNAPSHOT-jar-with-dependencies.jar \
       --port=$port \
       --bindAddress=0.0.0.0 \
       --sandbox=$build_dir \
       --peerNodes=http://$peerHostAddress:$port &>/dev/null &

START_XENON_HOST

echo "Xenon running on $address"
