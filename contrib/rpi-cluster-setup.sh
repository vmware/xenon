#!/bin/bash

set -o pipefail
set -o nounset
set -o errexit

buildNumber=3245

host1=10.118.97.98
host2=10.118.96.132
host3=10.118.96.168

peerAddress=10.118.97.98

/bin/bash ./contrib/rpi-setup.sh $host1 $peerAddress $buildNumber
/bin/bash ./contrib/rpi-setup.sh $host2 $peerAddress $buildNumber
/bin/bash ./contrib/rpi-setup.sh $host3 $peerAddress $buildNumber
