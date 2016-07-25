
**This is temporal documentation.**


# Concept & Design

User construct a node group, and perform operations against it.
Most of the case, user should not care which host it is hitting.

## Sending request

`TestClient` provides methods to send request(operation) mainly for synchronously but async is also available.

```
  InProcessNodeGroup nodeGroup = new InProcessNodeGroup();
  nodeGroup.add(host);
  
  TestClient client = new TestClient(nodeGroup);
  client.sendPost("/core/examples", op -> op.setBody(body));  // send post using one of the host in node-group.
```

## Operation Timeout

  - Instead of setting timeout to host, lookup timeout strategy.
  - use `Duration` instead of milli/micro sec
  - `XenonTestConfig` serves as global variable holder for strategies. 
    (If we want, we can convert to `ServiceLoader` framework)
    - Default implementation looks up system variable, then, thread local.
    - Alternatively, it can to perform operation in different timeout using `DefaultTimeoutStrategy#withTimeout`
    
  see `TimeoutStrategy`, `DefaultTimeoutStrategy`, `XenonTestConfig`


## Error handling 

  - NO method declares `throwing Throwable`
    - throw checked exception as unchecked exception
  - `XenonTestException` is used for framework related exception.

  see `ExceptionTestUtils`, `ExecutableBlock`, `XenonTestException`
    

## TestContext

  - Use `Duration` instead of `long`
  - Use constructor to instantiate
  - Changed not to throw checked exception
  - Lookup timeout strategy from `XenonTestConfig`


# TODO

  - unittest
  - test framework integration (junit4,5)
  - `OperationResult` that contains result and error. It should be result of `send` methods
  - logging for test
  - HTTPS