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

prompt "Create some example services:"
curl -X POST -H "Content-Type: application/json" -d '{"name":"example1"}' http://127.0.0.1:8000/core/examples
curl -X POST -H "Content-Type: application/json" -d '{"name":"example2"}' http://127.0.0.1:8000/core/examples
curl -X POST -H "Content-Type: application/json" -d '{"name":"example3"}' http://127.0.0.1:8000/core/examples


prompt "Verify the example services exist:"
curl http://127.0.0.1:8000/core/examples


prompt "Create a new task service:"
curl -X POST -H "Content-Type: application/json" -d '{}' http://127.0.0.1:8000/core/demo-tasks


prompt "If your task has done everything right, your example services should no longer exist:"
curl http://127.0.0.1:8000/core/examples
