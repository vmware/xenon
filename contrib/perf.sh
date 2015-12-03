#!/bin/bash

##
# Sets up some specific experiments gathers results. The way this
# script is written, assumes you are probably going to source it
# (i.e., it's mostly functions, to allow different workflows and
# experiments)
# 
# Experiments run for different JVM heap sizes, request 
# numbers, isolation mechanisms, and service capabilities.
# 
# On benchmarking tools. It seems the current consensus is on
# wrk, an efficient benchmarking tool:
# * ab not highly regarded (no keep-alives, only HTTP1)
# * httperf gives good info -- but *very* slow
# * siege is a good alternative
##

BM="benchmarks"
RESULTS="results.log"
FAILS="failures.log"
XENON="http://localhost:8000/"
JSONH="Content-type: application/json"
INST="instance"

MEM_RANGE=(64 256 1024 4096 16384)
REQ_RANGE=(1000 10000 100000 1000000)
CAP_RANGE=("stateful" "stateless" "persisted" "replicated" "owner-selected" "quorum-enforced" "full-caps")

MEM_POINT_MB=${MEM_RANGE[2]}
REQ_POINT=${REQ_RANGE[2]}
REQ_POINT=${CAP_RANGE[2]}

function safe_rmdir {
  [ -d "$1" ] && rm -rf $1
}

function safe_rm {
  [ -f "$1" ] && rm $1
}

function prep_experiment {
  cd $(git rev-parse --show-toplevel)
  safe_rmdir $TMPDIR/xenon
  safe_rm $RESULTS
  safe_rm $FAILS
  touch $RESULTS
  # force pull latest dependencies from mvn central
  mvn clean install -U -DskipTests=true
}

##
# Set heap size -- does not require re-compilation!
# $1 heap size in MB
##
function change_heap_size {
  local _HS="testHeapSizeMb"
  local _FROM="<$_HS>[0-9]*</$_HS>"
  local _TO="<$_HS>$1</$_HS>"
  sed "s;$_FROM;$_TO;" pom.xml > pom.xml.bk && 
  mv pom.xml.bk pom.xml
}

function read_heap_size {
  local _HS="testHeapSizeMb"
  local _HSHS="<$_HS>[0-9]*</$_HS>"
  grep $_HSHS pom.xml | 
  sed "s;<$_HS>;;" | 
  sed "s;</$_HS>;;" |
  tr -d '[[:space:]]'
}



##
# This invokes test cases already written to pound on different
# parts of the implementation, and we leverage these to get an
# idea of where our numbers are. They share the same address 
# space as the rest of the framework.
#
# $1 Request Count
##
function in_process_experiment {
  echo "In-process $1" >> $RESULTS
  mvn test "-Dtest=TestStatefulService#throughputInMemoryServicePut" "-Dxenon.requestCount=$1" 2>&1 |
  tee >(grep "second" >> $RESULTS) |
  tee >(grep -i "not paired properly\|exception" > $FAILS)
}

##
# Start performance host
##
function start_perf_host {
  mvn exec:java -pl xenon-samples -Dexec.mainClass="com.vmware.xenon.performance.PerfHost"
}

##
# Prepare initial state (JSON) for a service
##
function s_init {
  echo "{'documentSelfLink':'$1','message':'$2'}"
}

function revert_heap_size {
  git checkout -- pom.xml
}

function create_instances {
  for cap_point in "${CAP_RANGE[@]}"; do
    curl -s -X POST -H "Content-type: application/json" -d '{"documentSelfLink":"instance","message":"1"}'   "${XENON}${BM}/${cap_point}"
  done
}

function get_factories {
  for cap_point in "${CAP_RANGE[@]}"; do
    curl -s "${XENON}${BM}/${cap_point}";
  done
}

function get_instances {
  for cap_point in "${CAP_RANGE[@]}"; do
    curl -s "${XENON}${BM}/${cap_point}/$INST"; 
  done
}

function get_instances {
  for cap_point in "${CAP_RANGE[@]}"; do
    curl -s -X POST -H "Content-type: application/json" -d '{"message":"1"}'  "${XENON}${BM}/${cap_point}/$INST"
  done
}


function print_test_uris {
  for cap_point in "${CAP_RANGE[@]}"; do
    echo "${XENON}${BM}/${cap_point}/$INST"; 
  done
}

function install_wrk {
  git clone git@github.com:wg/wrk.git
  cd wrk
  make
  echo "Attempting to place wrk into /usr/local/bin"
  sudo cp wrk /usr/local/bin
}

function jvm_warmup_get {
  for (( i = 0; i <100; i++ )); do
    get_instances  2>&1 >> /dev/null
  done
}

function jvm_warmup_put {
  for (( i = 0; i <100; i++ )); do
    put_instances  2>&1 >> /dev/null
  done
}

function jvm_warmup {
  for cap_point in "${CAP_RANGE[@]}"; do
    wrk -t1 -c1 -s contrib/perf/post.lua -d30 "${XENON}${BM}/${cap_point}/instance"
    wrk -t1 -c1 -s contrib/perf/alternate.lua -d30 "${XENON}${BM}/${cap_point}/instance"
  done
}

function experiment {
  prep_experiment
  change_heap_size 64
  in_process_experiment 1000
}
