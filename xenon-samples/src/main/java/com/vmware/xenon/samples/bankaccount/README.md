# Bank Account service

## The command to run:

```bash
mvn -pl xenon-samples compile
mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.bankaccount.Host"
```

## Output of server shell session

```console
git:(master) ✗ ! +? » mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.bankaccount.Host"
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384m; support was removed in 8.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Bank Account Service (Routing) 0.1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ xenon-samples ---
[0][I][1443980053433][Host:8000][startImpl][com.vmware.xenon.common.ServiceHost/2f1928ae listening on 127.0.0.1:8000]
^C[1][W][1443980055305][Host:8000][lambda$main$0][Host stopping ...]
```

## Output of client shell session

```console
git:(master) ✗ ! +? » curl -i -H 'Content-Type: application/json' localhost:8000/bank-accounts
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
  "documentOwner": "919c74b9-b824-418a-be15-e49809374a22"
}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -XPOST -H 'Content-Type: application/json' localhost:8000/bank-accounts -d '{"balance":150.0}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 380
connection: keep-alive

{"balance":150.0,"documentVersion":0,"documentEpoch":0,"documentKind":"com:vmware:xenon:samples:bankaccount:services:BankAccount:BankAccountState","documentSelfLink":"/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9","documentUpdateTimeMicros":1443981946550002,"documentUpdateAction":"POST","documentExpirationTimeMicros":0,"documentOwner":"919c74b9-b824-418a-be15-e49809374a22"}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -H 'Content-Type: application/json' localhost:8000/bank-accounts
HTTP/1.1 200 OK
content-type: application/json
content-length: 288
connection: keep-alive

{
  "documentLinks": [
    "/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9"
  ],
  "documentCount": 1,
  "queryTimeMicros": 7999,
  "documentVersion": 0,
  "documentUpdateTimeMicros": 0,
  "documentExpirationTimeMicros": 0,
  "documentOwner": "919c74b9-b824-418a-be15-e49809374a22"
}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -X PATCH -H 'Content-Type: application/json' localhost:8000/bank-accounts/4c18bf3e-9857-4d0f-8e99-e02f66547541 -d '{"amount":80,"kind":"WITHDRAW"}'
HTTP/1.1 404 Not Found
content-type: application/json
content-length: 78
connection: keep-alive

{"statusCode":404,"documentKind":"com:vmware:xenon:common:ServiceErrorResponse"}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -X PATCH -H 'Content-Type: application/json' localhost:8000/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9 -d '{"amount":80,"kind":"WITHDRAW"}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 379
connection: keep-alive

{"balance":70.0,"documentVersion":1,"documentEpoch":0,"documentKind":"com:vmware:xenon:samples:bankaccount:services:BankAccount:BankAccountState","documentSelfLink":"/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9","documentUpdateTimeMicros":1443981984156000,"documentUpdateAction":"POST","documentExpirationTimeMicros":0,"documentOwner":"919c74b9-b824-418a-be15-e49809374a22"}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -X PATCH -H 'Content-Type: application/json' localhost:8000/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9 -d '{"amount":80,"kind":"WITHDRAW"}'
HTTP/1.1 400 Bad Request
content-type: application/json
content-length: 119
connection: keep-alive

{"message":"not enough funds to withdraw","statusCode":400,"documentKind":"com:vmware:xenon:common:ServiceErrorResponse"}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -X PATCH -H 'Content-Type: application/json' localhost:8000/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9 -d '{"amount":100,"kind":"DEPOSIT"}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 381
connection: keep-alive

{"balance":170.0,"documentVersion":2,"documentEpoch":0,"documentKind":"com:vmware:xenon:samples:bankaccount:services:BankAccount:BankAccountState","documentSelfLink":"/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9","documentUpdateTimeMicros":1443982009435000,"documentUpdateAction":"PATCH","documentExpirationTimeMicros":0,"documentOwner":"919c74b9-b824-418a-be15-e49809374a22"}

± icarrero@icarrero-mbpro:~/vmware/dcp-samples
git:(master) ✗ ! +? » curl -i -X PATCH -H 'Content-Type: application/json' localhost:8000/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9 -d '{"amount":80,"kind":"WITHDRAW"}'
HTTP/1.1 200 OK
content-type: application/json
content-length: 380
connection: keep-alive

{"balance":90.0,"documentVersion":3,"documentEpoch":0,"documentKind":"com:vmware:xenon:samples:bankaccount:services:BankAccount:BankAccountState","documentSelfLink":"/bank-accounts/701ab698-b412-40cf-bb79-62b03eee88e9","documentUpdateTimeMicros":1443982017377003,"documentUpdateAction":"PATCH","documentExpirationTimeMicros":0,"documentOwner":"919c74b9-b824-418a-be15-e49809374a22"}
```
