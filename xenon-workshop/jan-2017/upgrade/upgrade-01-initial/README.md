This project contains the initial (Day 0) code for the upgrade workshop, which includes a basic implementation for a stateful `Employee` service.

### Demo

1. Start up original version (running on port 8000):

```bash
cd xenon-upgrades
java -jar upgrade-01-initial/target/upgrade-initial-1.0-SNAPSHOT-jar-with-dependencies.jar --port=8000 --id=host8000 --sandbox=upgrade-01-initial/target/sandboxdb
```

2. Add some data to original version:

```bash
curl -X POST -H "Content-type: application/json" -d '{
    "documentSelfLink":"jon",
    "name":"Jon (CEO)"
}' http://localhost:8000/quickstart/employees

curl -X POST -H "Content-type: application/json" -d '{
    "documentSelfLink":"niki",
    "name":"niki",
    "managerLink":"/quickstart/employees/jon"
}' http://localhost:8000/quickstart/employees

curl http://localhost:8000/quickstart/employees?expand
```
