# Echo service

## The command to run

```bash
mvn -pl xenon-samples compile
mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.echo.ServiceHost"
```

## Output of the server shell session

```console
git:(master) ✗ ! » mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.echo.ServiceHost"
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384m; support was removed in 8.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Echo Service (stateful hello world) 0.1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ xenon-samples ---
[0][I][1443764549155][ServiceHost:8000][startImpl][com.vmware.xenon.common.ServiceHost/cd63e7d1 listening on 127.0.0.1:8000]
```

## Output of client shell session

```console
git:(master) » curl -i -X POST -H 'Content-Type: application/json' localhost:8000/echo -d '{ "message": "hello world" }'
HTTP/1.1 200 OK
content-type: application/json
content-length: 269
connection: keep-alive

{"message":"hello world","documentVersion":0,"documentKind":"com:vmware:xenon:samples:echo:services:EchoService:EchoServiceState","documentSelfLink":"/echo/8f3835cc-bf17-4ce8-9e59-6a18c7a5ed8d","documentUpdateTimeMicros":1443764416671000,"documentExpirationTimeMicros":0}
```
