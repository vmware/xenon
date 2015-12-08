# To do list service

## The command to run:

```bash
mvn -pl xenon-samples compile
mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.todolist.Host"
```

## Output of server shell session

```console
git:(master) » mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.todolist.Host"
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384m; support was removed in 8.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Todo list Service (CRUD) 0.1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ xenon-samples ---
[0][I][1443924770928][Host:8000][startImpl][com.vmware.xenon.common.ServiceHost/cd63e7d1 listening on 127.0.0.1:8000]
```

## Output of client shell session

```console
git:(master) ✗ ! » curl -i -H 'Content-Type: application/json' localhost:8000/todo
HTTP/1.1 200 OK
content-type: application/json
content-length: 224
connection: keep-alive

{
  "documentLinks": [],
  "documentCount": 0,
  "queryTimeMicros": 1,
  "documentVersion": 0,
  "documentUpdateTimeMicros": 0,
  "documentExpirationTimeMicros": 0,
  "documentOwner": "0b7a8063-9b46-4b97-9419-87946e59c9d9"
}                                                                                                                              

git:(master) ✗ ! » curl -i -X POST -H 'Content-Type: application/json' localhost:8000/todo -d '{"body":"the first task"}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 314
connection: keep-alive

{"body":"the first task","done":false,"documentVersion":0,"documentKind":"com:vmware:xenon:samples:todolist:services:TodoEntryService:TodoEntry","documentSelfLink":"/todo/5cb5e67a-51e9-4294-a7d5-1b943e2474c6","documentUpdateTimeMicros":1443925373603000,"documentUpdateAction":"POST","documentExpirationTimeMicros":0}%                                                                         

git:(master) ✗ ! » curl -i -X POST -H 'Content-Type: application/json' localhost:8000/todo -d '{"body":"the second task"}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 315
connection: keep-alive

{"body":"the second task","done":false,"documentVersion":0,"documentKind":"com:vmware:xenon:samples:todolist:services:TodoEntryService:TodoEntry","documentSelfLink":"/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35","documentUpdateTimeMicros":1443925386334000,"documentUpdateAction":"POST","documentExpirationTimeMicros":0}%                                                                        

git:(master) ✗ ! » curl -i -X POST -H 'Content-Type: application/json' localhost:8000/todo -d '{"body":"the third task"}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 314
connection: keep-alive

{"body":"the third task","done":false,"documentVersion":0,"documentKind":"com:vmware:xenon:samples:todolist:services:TodoEntryService:TodoEntry","documentSelfLink":"/todo/2b8fced9-ffa0-4ef0-a885-0d97e6459526","documentUpdateTimeMicros":1443925391014000,"documentUpdateAction":"POST","documentExpirationTimeMicros":0}%                                                                         

git:(master) ✗ ! » curl -i -X PUT -H 'Content-Type: application/json' localhost:8000/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35 -d '{"body":"the second task","done":true}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 284
connection: keep-alive

{"body":"the second task","done":true,"documentVersion":1,"documentKind":"com:vmware:xenon:samples:todolist:services:TodoEntryService:TodoEntry","documentSelfLink":"/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35","documentUpdateTimeMicros":1443925462867001,"documentExpirationTimeMicros":0}%                                                                                                       

git:(master) ✗ ! » curl -i -H 'Content-Type application/json' localhost:8000/todo
HTTP/1.1 200 OK
content-type: application/json
content-length: 379
connection: keep-alive

{
  "documentLinks": [
    "/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35",
    "/todo/5cb5e67a-51e9-4294-a7d5-1b943e2474c6",
    "/todo/2b8fced9-ffa0-4ef0-a885-0d97e6459526"
  ],
  "documentCount": 3,
  "queryTimeMicros": 4997,
  "documentVersion": 0,
  "documentUpdateTimeMicros": 0,
  "documentExpirationTimeMicros": 0,
  "documentOwner": "0b7a8063-9b46-4b97-9419-87946e59c9d9"
}
                                                                                                                
git:(master) ✗ ! » curl -i -H 'Content-Type application/json' localhost:8000/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35
HTTP/1.1 200 OK
content-type: application/json
content-length: 346
connection: keep-alive

{
  "body": "the second task",
  "done": true,
  "documentVersion": 1,
  "documentKind": "com:vmware:xenon:samples:todolist:services:TodoEntryService:TodoEntry",
  "documentSelfLink": "/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35",
  "documentUpdateTimeMicros": 1443925462867001,
  "documentUpdateAction": "PUT",
  "documentExpirationTimeMicros": 0
}

git:(master) ✗ ! » curl -i -X DELETE -H 'Content-Type application/json' localhost:8000/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35
HTTP/1.1 200 OK
content-type: application/json
content-length: 0
connection: keep-alive
git:(master) ✗ ! » curl -i -H 'Content-Type application/json' localhost:8000/todo/05d30e0f-23e1-4822-ae3f-716f2c149c35
HTTP/1.1 404 Not Found
content-type: application/json
content-length: 87
connection: keep-alive

{
  "statusCode": 404,
  "documentKind": "com:vmware:dcp:common:ServiceErrorResponse"
}
```
