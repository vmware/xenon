#!/bin/bash
# Script to build xenon in background for Travis CI.
# Travis CI does not allow output log to be more than 4MB. To overcome this
# limitation we run the mvn build command in background in this script
# and redirect the output in a log file. At the end of build we print last
# few lines from the log file on to the console for debugging purpose.

set -ex

export WORKING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export OUTPUT_FILE=$WORKING_DIR/output.out
touch $OUTPUT_FILE

print_output() {
   echo "Last 1000 lines of output:"
   tail -1000 $OUTPUT_FILE
}

handle_error() {
  echo "ERROR: Caught an error in the build."
  print_output
  exit 1
}

trap 'handle_error' ERR

bash -c "while true; do echo \$(date) - building xenon...; sleep 30s; done" &
LOOP_PID=$!

./mvnw install -P coverage >> $OUTPUT_FILE 2>&1

print_output
kill -9 $LOOP_PID
