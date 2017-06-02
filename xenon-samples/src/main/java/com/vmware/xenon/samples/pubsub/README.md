# PubSub Service

This sample shows the different ways in which you can achieve pubsub with DCP.

Including:

* Subscribe to state mutations on a particular service instance

## The command to run:

```bash
mvn -pl xenon-samples compile
mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.pubsub.Host"
```

## Output of server shell session

```console
git:(master) ✗ ! » mvn -pl xenon-samples exec:java -Dexec.mainClass="com.vmware.xenon.samples.pubsub.Host"
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=384m; support was removed in 8.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building PubSub service (Subscriptions) 0.1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.4.0:java (default-cli) @ xenon-samples ---
[0][I][1445377335319][Host:8000][startImpl][com.vmware.xenon.common.ServiceHost/1d2d87fe listening on 127.0.0.1:8000]
[1][I][1445377335608][8000/subscriber][subscribeToTopicUpdates][created topic A sample topic@/topics/a7ddbcd6-5ac7-42dd-a6af-5cdc7b431da0]
```

## Output of client shell session

```
curl -XPUT -H 'Content-Type: application/json' localhost:8000/topics/a7ddbcd6-5ac7-42dd-a6af-5cdc7b431da0 -d '
{ "name": "My Topic" }
'
{
    "documentExpirationTimeMicros": 0,
    "documentKind": "com:vmware:xenon:samples:pubsub:services:TopicService:TopicState",
    "documentSelfLink": "/topics/a7ddbcd6-5ac7-42dd-a6af-5cdc7b431da0",
    "documentUpdateTimeMicros": 1445377368839000,
    "documentVersion": 1,
    "name": "My Topic"
}
```

### server shell session should show logs of the notification

```
[2][I][1445377368841][8000/subscriber][lambda$subscribeToTopicUpdates$1][PUT http://127.0.0.1:8000/baee8059-adb0-49c8-bcb4-33f4c9bc6dc8]
[3][I][1445377368841][8000/subscriber][lambda$subscribeToTopicUpdates$1][com.vmware.xenon.samples.pubsub.services.TopicService$TopicState@153f8967]
```
