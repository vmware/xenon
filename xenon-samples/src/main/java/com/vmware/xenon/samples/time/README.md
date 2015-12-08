# Time service

## The command to run:

```bash
mvn -pl xenon-samples compile
mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.time.ServiceHost"
```

## Output of server shell session

```console
git:(master) ✗ ! » mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.time.ServiceHost"
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384m; support was removed in 8.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Print Time Service (stateless hello world) 0.1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ xenon-samples ---
[0][I][1443758830103][ServiceHost:8000][startImpl][com.vmware.xenon.common.ServiceHost/cd63e7d1 listening on 127.0.0.1:8000]

^C[1][W][1443758851962][ServiceHost:8000][lambda$main$0][Host stopping ...]
```

## Output of client shell session

```console
git:(master) ✗ ! » curl -i localhost:8000/time
HTTP/1.1 200 OK
content-type: application/json
content-length: 28
connection: keep-alive

Thu Oct 01 21:07:21 PDT 2015
```
