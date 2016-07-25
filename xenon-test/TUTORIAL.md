**THIS IS WORKING IN PROGRESS DOCUMENTATION. THE FILE LOCATION WILL BE CHANGED.**

This document describes how to test xenon services/host with xenon-test module.

Full source code is in [ExampleServiceTests.java]().

# Writing a test

This section demonstrates how to write tests with `xenon-test` module.

Sample scenario shows CREATE/UPDATE/DELETE `ExampleService`.
- Preparing `InProcessNodeGroup`
- Create a test client
- Perform _CREATE/UPDATE/DELETE_ synchronously and verify each step


## Create a node group

`InProcessNodeGroup` represents a node group composed of single/multiple `ServiceHost`.


```java
  // create nodegroup and wait conversion and service to be ready
  InProcessNodeGroup<ExampleServiceHost> nodeGroup = new InProcessNodeGroup<>();
  nodeGroup.addHost(host);  // add your test target service host
  nodeGroup.joinHosts();  // TODO
  nodeGroup.waitForConversion();
  nodeGroup.waitForServiceAvailable("/core/examples");
```

- `nodeGroup.joinHosts()`: make all hosts to join the node-group
- `nodeGroup.waitForConversion()`: wait until node-group is ready
- `nodeGroup.waitForServiceAvailable(servicePath);`: wait until specified service to be ready


## Create a test client

TestClient is a request(operation) sender, and defines various methods to send operations(mostly
synchronous, but async as well).


```java
  TestClient client = new TestClient(nodeGroup);
```


## Create an example service

Send a POST request to one of the host in node-group. Wait until the service is ready, then issue a
GET to verify the creation.

```java
  // create example service
  ExampleServiceState postBody = new ExampleServiceState();
  postBody.name = "foo";
  postBody.documentSelfLink = "/foo";

  // synchronously perform operation and expect success
  client.sendPost("/core/examples", op -> op.setBody(postBody));
  nodeGroup.waitForServiceAvailable("/core/examples/foo");

  // synchronously perform operation, then return body
  Operation get = Operation.createGet(nodeGroup.getHost(), "/core/examples/foo");
  ExampleServiceState result = client.sendThenGetBody(get, ExampleServiceState.class);
  assertEquals("foo", result.name);

```

## Update the example service


```java
  // update using patch
  ExampleServiceState patchBody = new ExampleServiceState();
  patchBody.name = "bar";

  // synchronously send a patch with setting body to operation
  client.sendPatch("/core/examples/foo", op -> op.setBody(patchBody));

  // TODO: consider send...() to return OperationResponse
  get = Operation.createGet(nodeGroup.getHost(), "/core/examples/foo");
  result = client.sendThenGetBody(get, ExampleServiceState.class);
  assertEquals("bar", result.name);
```

Verification can also be performed in callback, but it is not recommended since it is less debuggable.

```java
  // verification in callback (this is synchronous call)
  client.sendExpectSuccess(get, op -> {
      // this code will be executed after successful get request.
      ExampleServiceState body = op.getBody(ExampleServiceState.class);
      assertEquals("bar", body.name);
  });
```

_`client.sendExpectSuccess` is a synchronous operation. Given callback handler is performed after
successful operation._


## Delete the example service

```java
  // delete the service
  client.sendDelete("/core/examples/foo");
  get = Operation.createGet(nodeGroup.getHost(), "/core/examples/foo");
  client.sendExpectFailure(get);  // TODO: check with 404
```

