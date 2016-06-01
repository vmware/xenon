# CHANGELOG

## 0.9.0-SNAPSHOT

* Add support for graph queries. The new GraphQueryTaskService, listening on
  /core/graph-queries enables multi stage queries that can traverse linked
  documents. Each stage uses the QueryTask specification, to select the documents
  that serve as the graph nodes. The graph edges, that link nodes together are
  specified through link fields, and the of QueryOption.SELECT_LINKS, which is
  automatically enabled to return the set of document links that form the nodes
  for the next stage of the graph. Like regular query tasks, both direct, and
  asynchronous REST pattern models are supported on the task service instance.
  The following is an example of a two stage graph query
  Stage 0: Filter documents by kind eq ParentState. SelectLinks: childLink,nephewLink
  Stage 1: Filter documents by kind eq ChildState AND field name = Jimmy
  Since each stage is a full query, the results can be sorted, paginated, use
  complex boolean, nested expression trees, etc. They can even use broadcast
  for 3x replication scenarios.
  The graph queries are load balanced across nodes.

* Modify new QueryOption.SELECT_LINKS and QueryOption.EXPAND_LINKS behavior
  to properly de-duplicate expanded state for selected links and make the
  selection results similar to the existing documentLinks and documents,
  in usage and behavior.
  The selectedLinks map is now called selectedLinksPerDocument.

## 0.8.2

* Add global stats for total service pauses, resumes, cache clears
  and ON_DEMAND_LOAD service stops on /core/management service.

* Move a few infrastructure query related helper methods from Utils class
  to QueryTaskUtils. The mergeQueryResults family of methods are used by
  core services, so this should have minimal impact

* Add PropertyUsageOption.REQUIRED to describe fields that are required.

* Add validateState(description, state) to Utils to validate fields that
  are required. If the field is null, REQUIRED and ID, a UUID is automatically
  generated. If the field is null and REQUIRED, an Exception is thrown.

* Add QueryOption.EXPAND_LINKS for expanding selected link values with the
  target document state and including it in the query results. The serialized
  state, is placed in the results.selectedLinks map.

* Add PropertyUsageOption.SENSITIVE to describe fields that contain sensitive
  information. When marked, the field will be hidden when serializing to JSON
  using toJson(boolean hideSensitiveFields, boolean useHtmlFormatting).

* Add support for configurable auth expiration via JVM property or login request to
  BasicAuthenticationService.

* Use SEND_WITH_CALLBACK in NettyHttpServiceClient if the request was
  configured for connectionSharing and SSL

* Add QueryOption.SELECT_LINKS for selecting fields marked with
  PropertyUsageOption.LINK and including the link values in the query results.
  The link selection will be used for future graph query support and
  automatic link content expansion.

* Add transaction flow support, across related operations, similar to
  authorization context and context id flow support

* Rename LuceneQueryTaskFactoryService and LuceneQueryTaskService to
  QueryTaskFactoryService and QueryTaskService. Similar change for
  local query task service. Service code should use
      ServiceUriPaths.CORE_QUERY_TASKS
      ServiceUriPaths.CORE_LOCAL_QUERY_TASKS
  instead of the service SELF_LINK fields.

* Upgrade Netty from 4.1.0.CR7 to 4.1.0.Final

* Add new JVM properties in ServiceClient and ServiceRequestListener interfaces
  for maximum request and response payload size limits.

* Remove ServiceClient.sendWithCallback and ServiceHost.sendRequestWithCallback.
  Functionality is available through OperationOption.SEND_WITH_CALLBACK,
  symmetric to HTTP/2 functionality that is toggled through CONNECTION_SHARING

* Add new static fields that map to JVM properties that enable selection of
  HTTP scheme, for replication and forwarding, in NodeSelectorService interface.

* Invalidate authz cache in ServiceHost when any authz service(UserGroupService,
  RoleService or ResourceGroupService) is created, modified or deleted

* Add Operation.toggleOption and Operation.hasOption to allow direct manipulation
  of operation options and reduce code in Operation class methods

* Add support for OData to filter by all searchable fields of a document.
  Using "ALL_FIELDS" as a property name in a typical OData filter, e.g.
  /documents?$filter=ALL_FIELDS eq foo, will unfold the search to all indexed
  fields of document and their sub-fields nested up to 2 levels, excluding the
  build-in ServiceDocument fields.

* Introduce PropertyIndexingOption.FIXED_ITEM_NAME. This option directs the
  document indexing service to ensure the indexing property name will be a fixed
  value, matching that of the field itself. Applicable for fields of type MAP,
  it will allow to make queries based on the name of the field to search for
  keys and values of a map.

## 0.8.1

* Add support for expiration on stateful in-memory services. We now
  support expiration and auto-DELETE on services with
  ServiceOption.PERSISTED and without

* Add Operation.disableFailureLogging to allow control on default
  failure logging behavior. Disable logging of failures on on demand
  load services, if the demand load action is a DELETE

* Add OData query parameter $orderbytype used for specifying the type
  of property specified through $orderby parameter. This allows the
  OData query service in Xenon to support sorting for both numeric
  and string types.

* Add Utils.registerKind(Class<?>, String) allowing for custom mapping
  of service document types to kind strings. The default is derived
  from the canonical name of the class, as before. This is not a breaking
  change.

* Enable binary payload serialization during replication, using the same KRYO
  serialization we currently use for storing state in the index. The
  binary payload is only used during replication to peers so this is
  an internal optimization, not visible to clients or service authors.
  HTTP + JSON is still the default mechanism for all I/O between
  services and clients to xenon hosts. This is not a breaking change.

* Add StatefulService.getStateDescription() convenience method
  which also reduces allocations when a service author needs
  the reflect state document description

* Upgrade Lucene from 5.3.1 to 6.0.0

* Use Murmur3 hash, instead of SHA1 for ServiceDocument.equals
  and document signature calculation

* Add support for binary serialization of operation body, using
  KRYO binary serializer (same as what currently used for storing
  state in the index). The client opts in by setting the operation
  content type to the new Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM

* Remove experimental PRAGMA_VALUE_HTTP2, it is now expressed
  through OperationOption.CONNECTION_SHARING.

* Add support for starting ServiceHost only with HTTPS.
  To disable HTTP listener, provide "--port=-1" to the startup parameters.

* Add support for connection tags, optimize HTTP/2 I/O path. Connection
  tags allow finer control of connection pools, connection limit. HTTP/2
  default connection tag uses (by default) 4 parallel HTTP/2 connections
  per host, with potentially millions of pending operations in each.
  Perf gain of 2x on HTTP/2, with these changes.

* Enable HTTP2 for forwarding (as part of built in owner selection and loadbalancing)

## 0.8.0

* Enhance QueryOption.BROADCAST to use documents from owner nodes
  For details please see: https://www.pivotaltracker.com/projects/1471320/stories/116412415

* Upgrade netty from 4.1.0.CR3 to 4.1.0.CR7

* Update JWT(Json Web Token) to use private key when SSL is enabled. Otherwise,
  default value(same across all nodes) is used. (NOT SECURE)
  JWT is used to sign/verify authorization context token.

  In multi node environment, it is required to use same certificate and private
  key within same node group.
  If different cert is used, communication between nodes will fail since different
  private key is used to sign/verify json.
  We are planning certificate rotation in future.

  NOTE:
    Using the default token is insecure and should not be done in production
    settings when authorization is enabled.

* Add additional "Operation#setOperationCompletion" that allows the caller
  to supply two discrete callbacks: one for successful completion, and
  one for failure.
  For completion that does not share code between the two paths, this
  should be the preferred alternative.

* Use HTTP/2 for replication and forwarding requests

* Add logging methods that take lambda expression to construct
  log message. The lambda is evaluated lazily.

* Remove PRAGMA_DIRECTIVE_NO_QUEUING and make no-queuing
  the default behavior. That means finding a service succeeds
  if the service exists or fails-fast in the case it doesn't.
  Use PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY to
  override the default.

## 0.7.6

* Improve index searcher management in lucene document index
  service. Track index searchers for paginated query tasks
  using a sorted tree map, and close them when their query
  task expiration is reached. This avoid query failures
  under load, for paginated tasks.

* Add replication request header that allows the replication
  level to be set per operation. Useful for scenarios that
  require all peer nodes to see an update, independent of the
  membership quorum associated with the node group. For example
  all authorization related updates should replicate to all
  peers, before the operation completes

* Switch consistent hashing algorithm used by the default
  node selector, to Murmur3 32bit, instead of SHA1. Much less
  memory use, faster, leaner. This is an implementation detail,
  not visible to consumers to xenon.

* Add ServiceHost.checkFactoryAvailable and
  NodeGroupUtils.checkServiceAvailability convenience methods
  that use broadcast GET requests to all peers, to the service
  /available suffix, and return success if at least one peer
  reports status OK. This is useful for tests and production
  code that wants to determine if factory synchronization is
  complete, either on initial node start or after node changes

* Add MigrationTaskService, a task that can synchronize state
  between two node groups, for a given factory. Used as part
  of the "blue/green" live upgrade procedure. See wiki for
  multi node live upgrade details

* Add userGroupLinks as a field in UserService

* Remove TypeName.ARRAY and use TypeName.COLLECTION instead

## 0.7.5

* New TaskService.java base class to reduce duplicate code when
  implementing a task service. Subclasses no longer need to
  override: handleStart(), sendSelfPatch(), updateSelf(). Also
  provides common validation and default task expiration.

* New FactoryService.create helpers and ServiceHost.startFactory
  and startService helpers that reduce duplicate code

* New Operation.createXXXX(ServiceHost h, link) helpers
  to eliminate need for UriUtils.buildUri

* Option to serve a Swagger 2.0 API description of a ServiceHost

* Add a UiContentService that behaves like a regular web-server serving
  static files from a folder. It simplifies custom UI hosting.

## 0.7.2

* Simplify service synchronization logic during node group
  changes. Eliminate support for complex divergence scenarios
  and focus on reliable behavior on a few scenarios.
  We now throttle synchronization further by electing a single
  node to synchronize a given factory. Since we already only let
  a single factory synchronize per node, this further reduces
  contention.
  See https://www.pivotaltracker.com/story/show/115367955

* Throttle peer synchronization of services further: we now only
  synchronize one replicated factory at a time. A factory already
  throttles and synchronizes a limited number of children before
  doing the next batch. This will greatly help with scenarios when
  N nodes, with lots of existing state, all start and join at once.

* Add ServiceDocument.PropertyOptions that supercedes
  ServiceDocument.UsageOptions and can also specify indexing
  options.

* Add ServiceRequestSender interface that captures the capability
  to send Operation. Allows for ServiceHost, Service and ServiceClient
  to be dealt with uniformly. Clients may need to recompile as a lot
  of overloaded methods are removed.

* Add ServiceHost helper method startFactoryServicesSynchronously
  for starting factory services.

* Add a new module xenon-client that provides 'xenonc' executable.
  The client application works with an intuitive DSL to interact
  with a xenon-host. YAML and JSON are currently supported.

## 0.7.1

* Added VerificationHost.waitFor() utility method
  useful for convergence loops in tests.

* Support ODATA $orderby and $top

* Introduce ServiceOption.URI_NAMESPACE_OWNER. Allow
  for a service to handle all requests that start with
  a given prefix, allowing for emulation of other REST
  APIs, gateway style translation, etc.

* Add handleCreate handler to Service interface.
  This is not a breaking change. The new handler is
  opt-in, just like handleStop.
  Service lifecycle is now symmetric:
  1) Client POST -> handleCreate
  2) Client DELETE -> handleDelete
  handleStart always follows handleCreate, on owner only.
  handleStop always follows handleDelete, on owner only.
  handleStart occurs on service start, due to host restart,
  synch, or create (on owner)
  handleStop occurs on service stop, on service host stop,
  or due to delete (on owner)
  Added ServiceHost.isServiceCreate(operation) to match
  ServiceHost.isServiceStop(operation)

* Add StatelessService & StatefulService handlePeriodicMaintenance
  method to get periodic maintenance notifications when enabling
  ServiceOption.PERIODIC_MAINTENANCE. Invoked by handleMaintenance
  on MaintenanceReason.PERIODIC_SCHEDULE reason.

* Add StatelessService & StatefulService handleNodeGroupMaintenance
  method to get node group change notifications. Invoked by
  handleMaintenance on MaintenanceReason.NODE_GROUP_CHANGE reason.

* Include ServiceDocument.documentExpirationTimeMicros in the
  document signature computation done by Utils.computeSignature.
  Effectively this means that comparisons of two service
  documents using ServiceDocument.equals will return false if
  their "documentExpirationTimeMicros" differ.

## 0.7.0

* Add PRAGMA_FORCE_INDEX_UPDATE allowing for conflict
  resolution or overriding version checks on restarting a
  deleted service

* Fix NodeGroupBroadcastResponse race, where the jsonResponse
  list was getting corrupted, thus removing results from some
  of the nodes. This affected broadcast request behavior.

* Simplify constructs relating to consensus and availability.
  ServiceOption.ENFORCE_QUORUM is removed. Its semantics are
  rolled in with ServiceOption.OWNER_SELECTION. This implies
  that a service enabling owner selection, will get strong
  consensus and leader election, across the node group. The
  membershipQuorum must be properly set for strong consensus
  on state update.
  Eventual consistency is still supported and achieved by
  relaxing the membership quorum (using the REST api on
  node group /config suffix)
  Services with just ServiceOption.REPLICATION are always
  eventually consistent.
  Xenon will make an attempt to converge state across peers
  regardless of the options set.

* Remove NodeState.synchQuorum - Its functionality is collapsed
  into the existing membershipQuorum field. The quorum number
  determines when requests are allowed to flow, given node group
  health, and when resynchronization should occur. If the
  host argument --peerNodes is used, membership quorum is
  automatically set to the majority: (total / 2) + 1

* Services with ServiceOption.ON_DEMAND_LOAD and
  ServiceOption.REPLICATION will not automatically synchronize
  on every node group change. Services marked as such are
  expected to be explicitly synchronized or, on first use.

* Add FactoryService.create(), update ExampleService.
  Service authors no longer need to implement a factory service
  and derive from FactoryService class. Instead, they can
  just use FactoryService.create() to get a default, functional
  factory service instance which can then be started with
  ServiceHost.startService()

* Enable update of synch quorum through PATCH to
  node-groups/<group>/config. Membership quorum was already
  supported, now synch quorum is included.

* Fix consensus protocol commit behavior. Commit messages
  are no longer sent if the request to owner service fails
  or the service does not have ENFORCE_QUORUM.

* Split Service.handleDelete into Service.handleStop and
  Service.handleDelete. If the service is being stopped, the
  host has always added a special pragma, indicating this is
  a DELETE with the intent to stop a service, and to avoid
  any persistence or replication side effects.
  This is now formalized through the new handleStop method,
  invoked only when the service is being issued a DELETE, with the
  pragma included. This is potentially a breaking change for
  services that did special cleanup ONLY on stop, not delete+stop

## 0.6.0

* Add new per service utility suffix, /available. Provides a
  consistent mechanism for a service to declare it is available,
  re-using the underlying stats support. By default, all services
  that are started and in ProcessingState.AVAILABLE will return
  200 (OK) on GET /<service/available

* Add service host set/get service cache clear delay so cache clears are
  configurable

* Fixed bug: when DELETEs occurred during network partition, they
  would not be correctly synchronized after nodes rejoined.

## 0.5.1

* Enable HTTPS support for node groups. Several fixes in URI
  manipulation now allow Xenon hosts to start with just a HTTPS
  listener and use exclusively HTTPS for node group operations,
  including join, maintenance, replication and forwarding

* Add ServiceHost.Arguments.resourceSandbox to speed up UI
  development experience. When specified, xenon will publish
  the UI resource files directly from the file folder, allowing
  for real time edits / refresh of UI content

## 0.5.0

* Implement authorization on stateless services. This is a breaking
  change for xenon deployments that use authorization and have
  non factory service implementations of StatelessService class.
  Stateless services must now be included in roles, using a resource
  specification over the document self link.

## 0.4.1

* Updated the API for backing up or restoring the index. The Bearer
  parameter is now named bearer (the capitalization changed). While
  this is a breaking change, we believe this is unused and should be
  harmless.

* Added new ServiceOption: CONCURRENT_GET_HANDLING. This is (and has
  been) the default, but can now be disabled when appropriate.

* Added SLF4J-Xenon bridge

* QueryTasks that are broadcast queries can now be direct tasks.

## 0.4.0

* Added default support for HTTP Options. StafulService and
StatelessService now return the same content as GET on
*/template suffix, providing a default self documentation
capability.

* Added honoring of verbs (POST, PATCH, etc) in roles

* Renamed authentication cookie from dcp-auth-cookie to
  xenon-auth-cookie. This should be transparent for clients that use
  cookies, but clients that extract the cookie value in order to make
  an x-xenon-auth-token header need to use this new cookie instead.

* The basic authentication service (/core/authn/basic) now includes
  the x-xenon-auth-token header in its response, in addition to the
  cookie.

* ExampleServiceHost now takes arguments to create users (one admin,
  one non-admin that can access example services). This enables you to
  use the ExampleServiceHost when authorization is enabled with the
  isAuthorizationEnabled flag. The new arguments are:

  - adminUser
  - adminUserPassword
  - exampleUser
  - exampleUserPassword

## 0.3.2

* Add on demand service load, enabled with
ServiceOption.ON_DEMAND_LOAD

* Upgrade to Lucene 5.3.1 from 5.2.1. The Lucene file format is
backwards compatible, so this is a transparent update.

## 0.3.1

* Indexing I/O path optimizations producing 20% throughput
increase. Removed ServiceOption.INSTRUMENTATION from
LuceneDocumentIndexService. It can be toggled through /config
at runtime.

* Referer is no longer indexed with every document update

* All Operation request and response headers are converted
to lower case before being added to request/response maps

* Tunable query limit for service load during factory service
restart and during synchronization. A large result limit will
increase the service load throughput and decrease overal time
for factory child services becoming available. Large limits
can impact memory and network utilization however.

* Add keyPassphrase command line argument for providing SSL private key
passphrase.

## 0.3.0

* Renamed ServiceOption.EAGER_CONSISTENCY to ServiceOption.ENFORCE_QUORUM

* Renamed packages, build artifacts and various properties to
Xenon (from DCP). This is a runtime and binary breaking change
since HTTP headers, runtime properties, command line arguments
have changed.

## 0.2.0

* Request rate limiting support using authorization subject.
New serviceHost.setRequestRateLimit method and rate tracking
logic added to ServiceHost.

* Remove Operation.joinWith and associated support code in service
client. Joins are created and sent with OperationJoin.create() and
sendWith.

* Add Claims class to common package that extends the Claims in the jwt
package to include a properties map. This can be used to carry arbitrary
session state.

* Add QueryOption.CONTINUOUS so query tasks can be used for real time
update notifications with queries spanning the entire index

* Add ServiceDocument.documentUpdateAction so each state version is
associated with the action that caused the change. Also allows
subscribers to continuous query tasks to determine if a notification
was for a deleted state. This is a breaking change, please delete
existing lucene index

## 0.1.1

* Service authorization.

* Basic authentication.

* Allow booleanClauses to have single clause.

* Implement sort for QueryTask.

* Add QueryFilter for in-place evaluation of query specifications against documents.

* Add support for JavaScript DCP services connected over WebSocket.

## 0.1.0

* Start of CHANGELOG. See commit history.
