#! /bin/sh
#
#  cURL commands:


prompt () {
  echo ""
  echo ""
  echo $1
  echo "[Press Any Key]"
  read
}

####

prompt "Joining two nodes:"
curl -X POST -H "Content-Type: application/json" http://localhost:8001/core/node-groups/default -d '{
  "memberGroupReference": "http://127.0.0.1:8000/core/node-groups/default",
  "kind": "com:vmware:xenon:services:common:NodeGroupService:JoinPeerRequest"
}



prompt "Updating Quorum:"
curl -X POST -H "Content-Type: application/json" http://localhost:8000/core/node-groups/default -d '{
  "isGroupUpdate": true,
  "membershipQuorum": 3,
  "kind": "com:vmware:xenon:services:common:NodeGroupService:UpdateQuorumRequest"
}`
