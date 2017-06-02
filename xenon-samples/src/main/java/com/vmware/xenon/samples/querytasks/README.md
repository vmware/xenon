# Employee API

This example shows seeding a service with data at startup through making requests to it.
In addition to that it exposes an endpoint that shows all the oracle employees by filtering the list of people from the seed data based on the email domain.
It expands the query for the oracle employees query.

## The command to run:

```bash
mvn -pl xenon-samples compile
mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.querytasks.Host"
```

## Output of server shell session

```console
git:(master) ✗ ! +? » mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.querytasks.Host"
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384m; support was removed in 8.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Oracle Employees API (Query) 0.1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ xenon-samples ---
[0][I][1444156857093][Host:8000][startImpl][com.vmware.xenon.common.ServiceHost/2f1928ae listening on 127.0.0.1:8000]
[1][I][1444156858162][Host:8000][lambda$onGetAllPeopleComplete$2][retrieved 1000 people]
```

## Output of client shell session

```
git:(master) ✗ ! » curl localhost:8000/oracle-employees
{
  "documentLinks": [
    "/people/1ce36ba4-1bed-48ff-930e-0af778431c28"
  ],
  "documents": {
    "/people/1ce36ba4-1bed-48ff-930e-0af778431c28": {
      "id": "fe4ddf87-b43e-4b0a-b1d3-4b745ee6890f",
      "name": "Nicole Coleman",
      "email": "ncolemanp0@oracle.com",
      "birthDate": "Jun 2, 1969 1:34:52 AM",
      "documentVersion": 0,
      "documentEpoch": 0,
      "documentKind": "com:vmware:xenon:samples:querytasks:services:PersonService:PersonState",
      "documentSelfLink": "/people/1ce36ba4-1bed-48ff-930e-0af778431c28",
      "documentUpdateTimeMicros": 1444511368849041,
      "documentUpdateAction": "POST",
      "documentExpirationTimeMicros": 0,
      "documentOwner": "58d263b7-7766-4774-af63-fb31772b24c8"
    }
  },
  "documentCount": 1,
  "queryTimeMicros": 2999,
  "documentVersion": 0,
  "documentUpdateTimeMicros": 0,
  "documentExpirationTimeMicros": 0,
  "documentOwner": "58d263b7-7766-4774-af63-fb31772b24c8"
}
```
