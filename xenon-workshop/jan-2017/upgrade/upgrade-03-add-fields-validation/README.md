## Upgrading *with* functional changes

This project adds the following functional changes to our `EmployeeService`:
- `managerLink` is verified to be a valid link
- `location` is a field to the ServiceDocument

This changes in the project show how to use a transformation service when running a `MigrationTaskService`

### Demo

1. Kill original version (running on port 8000) since all its data was migrated.

2. Add more data to the running application (on port 8001). It should already have `jon` and `niki`...

```bash
echo Creating employee with non-existent managerLink
curl -X POST -H "Content-type: application/json" -d '{
    "documentSelfLink":"sally",
    "name":"sally",
    "managerLink":"/quickstart/employees/ghost"
}' http://localhost:8001/quickstart/employees

curl http://localhost:8001/quickstart/employees?expand
```

3. Startup new version:
```bash
java -jar upgrade-03-add-fields-validation/target/upgrade-add-fields-validation-1.0-SNAPSHOT-jar-with-dependencies.jar --port=8002 --id=host8002 --sandbox=upgrade-03-add-fields-validation/target/sandboxdb
```

4. Migrate employees from original version:

Now that `managerLink` must exist first, we need to migrate managers over first using `querySpec`
```bash
curl -X POST -H "Content-type: application/json" -d '{
    "sourceNodeGroupReference":"http://localhost:8001/core/node-groups/default",
    "sourceFactoryLink":"/quickstart/employees",
    "destinationNodeGroupReference":"http://localhost:8002/core/node-groups/default",
    "destinationFactoryLink":"/quickstart/employees",
    "migrationOptions":["USE_TRANSFORM_REQUEST"],
    "transformationServiceLink":"/quickstart/employees-transform",
    "querySpec": {
          "query": {
            "occurance": "MUST_OCCUR",
            "booleanClauses": [
              {
                "occurance": "MUST_OCCUR",
                "term": {
                  "propertyName": "documentSelfLink",
                  "matchValue": "/quickstart/employees/jon",
                  "matchType": "TERM"
                }
              }
            ]
          }
      }
}' http://localhost:8002/management/migration-tasks | jq

curl http://localhost:8002/quickstart/employees?expand
```

Now, migrate everyone else.
```bash
curl -X POST -H "Content-type: application/json" -d '{
    "sourceNodeGroupReference":"http://localhost:8001/core/node-groups/default",
    "sourceFactoryLink":"/quickstart/employees",
    "destinationNodeGroupReference":"http://localhost:8002/core/node-groups/default",
    "destinationFactoryLink":"/quickstart/employees",
    "migrationOptions":["USE_TRANSFORM_REQUEST"],
    "transformationServiceLink":"/quickstart/employees-transform",
    "querySpec": {
          "query": {
            "occurance": "MUST_OCCUR",
            "booleanClauses": [
              {
                "occurance": "MUST_NOT_OCCUR",
                "term": {
                  "propertyName": "documentSelfLink",
                  "matchValue": "/quickstart/employees/jon",
                  "matchType": "TERM"
                }
              }
            ]
          }
      }
}' http://localhost:8002/management/migration-tasks | jq
```

> As mentioned before, you could also use `MigrationOption.DELETE_AFTER` once you migrate the "managers", and then you wouldn't have to provide the `querySpec` on the last `MigrationTaskService`.

All data has been migrated!
- The `location` field for previous records was defaulted using the transformation service, and all new records will enforce that this field is populated.
- New validation logic implemented for `managerLink`. Previous data was scrubbed if the old `managerLink` was invalid, and all new data will require that a `managerLink` be valid, if provided.

```bash
curl http://localhost:8002/management/migration-tasks?expand
curl http://localhost:8002/quickstart/employees?expand
```
