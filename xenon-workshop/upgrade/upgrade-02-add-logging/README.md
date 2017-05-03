## Upgrading with no functional changes

The only difference between this project and the `xenon-upgrades/upgrade-01-initial` project is we added logging messages (and a `Employee.toString()`). There are no functional changes.

Nevertheless, we still need to do a full Xenon upgrade to migrate the ServiceDocuments from the old version to the new version.

### Demo

1. Start up original version (running on port 8000):

```bash
cd xenon-upgrades
java -jar upgrade-01-initial/target/upgrade-initial-1.0-SNAPSHOT-jar-with-dependencies.jar --port=8000 --id=host8000 --sandbox=upgrade-01-initial/target/sandboxdb
```

2. Add some data to original version (if you haven't already):

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

3. Startup new version:

```bash
java -jar upgrade-02-add-logging/target/upgrade-add-logging-1.0-SNAPSHOT-jar-with-dependencies.jar --port=8001 --id=host8001 --sandbox=upgrade-02-add-logging/target/sandboxdb
```

4. Migrate employees from original version:

```bash
curl -X POST -H "Content-type: application/json" -d '{
    "sourceNodeGroupReference":"http://localhost:8000/core/node-groups/default",
    "sourceFactoryLink":"/quickstart/employees",
    "destinationNodeGroupReference":"http://localhost:8001/core/node-groups/default",
    "destinationFactoryLink":"/quickstart/employees",
    "migrationOptions":["USE_TRANSFORM_REQUEST"]
}' http://localhost:8001/management/migration-tasks | jq
```

> If you were to run the same `POST` again, the `MigrationTaskService` would fail because those `Employee` services have now already been started on the new node group. You could use `MigrationOption.DELETE_AFTER` to delete the existing ServiceDocument on the new node group as a fallback though, which would then make the migration idempotent.

```bash
curl -X POST -H "Content-type: application/json" -d '{
    "sourceNodeGroupReference":"http://localhost:8000/core/node-groups/default",
    "sourceFactoryLink":"/quickstart/employees",
    "destinationNodeGroupReference":"http://localhost:8001/core/node-groups/default",
    "destinationFactoryLink":"/quickstart/employees",
    "migrationOptions":["USE_TRANSFORM_REQUEST","DELETE_AFTER"]
}' http://localhost:8001/management/migration-tasks | jq

curl http://localhost:8001/quickstart/employees?expand
curl http://localhost:8001/management/migration-tasks?expand
```

> You could also specify a `latestSourceUpdateTimeMicros` in the body of the migration task to only migrate ServiceDocuments newer than the supplied timestamp.

Now all data has been migrated to new cluster. Can shutdown old node-group at this point.
