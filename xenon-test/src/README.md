
**This is temporal documentation.**


# Concept & Design

User construct a node group, and perform operations against it.
Most of the case, user should not care which host it is hitting.

Currently, it is mainly for testing user's ServiceHost(integration, end-to-end test).
For testing service itself(unittest), they might need different way of doing such as similar to `VerificationHost`. 


## Sending request

  It is abstracted in `RequestSender`, `TargetedRequestSender` interfaces.
  `RequestSender` defines generic way of sending Operation sync/async.
  `TargetedRequestSender` has set specified target to send operations. `send[Get|Post|...]` methods don't need to specify target host.
  We can also have `RequestSender` implementation using netty directly which doesn't bind to any `ServiceHost` implementation.
   
  ```
  InProcessNodeGroup nodeGroup = new InProcessNodeGroup();
  nodeGroup.add(host);
  
  TargetedRequestSender client = new NodeGroupRequestSender(nodeGroup);
  client.sendPost("/core/examples", op -> op.setBody(body));  // send post using one of the host in node-group. 

  ```
  
  see `RequestSender`, `TargetedRequestSender`, `NettyRequestSender`, `ServiceHostRequestSender`, `NodeGroupRequestSender`


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