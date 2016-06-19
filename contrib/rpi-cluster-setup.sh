#!/bin/bash

set -o pipefail
set -o nounset
set -o errexit

buildNumber=3245

host1=10.118.97.98
host2=10.118.96.132
host3=10.118.96.168

peerNodes="http://10.118.97.98:8000,http://10.118.96.132:8000,http://10.118.96.168:8000"

/bin/bash ./contrib/rpi-setup.sh $host1 $peerNodes $buildNumber
/bin/bash ./contrib/rpi-setup.sh $host2 $peerNodes $buildNumber
/bin/bash ./contrib/rpi-setup.sh $host3 $peerNodes $buildNumber
